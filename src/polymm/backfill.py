"""`polymm backfill [trades|prices|resolutions|all]`: one-shot, idempotent history pulls
for every market that appears in the `markets` reference table.

  trades       Data API /v2/trades?condition=...   -> data/backfill/trades_hist/condition_id=.../page-NNNN.parquet
  prices       Data API /v2/prices-history         -> data/backfill/prices_hist/token_id=.../bucket=60/part-....parquet
  resolutions  Data API /v2/resolutions            -> data/reference/resolutions/resolutions.parquet (overwritten)

Why idempotent: every output file name is a pure function of (market, page/range),
so re-running after a failure overwrites identical files instead of duplicating rows.
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from .config import Config

log = logging.getLogger(__name__)

TRADES_HIST = pa.schema(
    [
        ("fetched_at_ns", pa.int64()),
        ("timestamp_s", pa.int64()),
        ("condition_id", pa.string()),
        ("token_id", pa.string()),
        ("outcome", pa.string()),
        ("price", pa.float64()),
        ("size", pa.float64()),
        ("usdc_size", pa.float64()),
        ("side", pa.string()),
        ("proxy_wallet", pa.string()),
        ("tx_hash", pa.string()),
    ]
)

PRICES_HIST = pa.schema(
    [
        ("fetched_at_ns", pa.int64()),
        ("token_id", pa.string()),
        ("timestamp_s", pa.int64()),
        ("price", pa.float64()),
        ("resolution_s", pa.int32()),
    ]
)


def _write(path: Path, rows: list[tuple], schema: pa.Schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = list(zip(*rows)) if rows else [[] for _ in schema]
    tbl = pa.Table.from_arrays([pa.array(c, type=f.type) for c, f in zip(cols, schema)], schema=schema)
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(tbl, tmp, compression="zstd")
    os.replace(tmp, path)


def _recorded_markets(cfg: Config) -> list[tuple[str, str, str]]:
    """(condition_id, token_id, outcome) for everything the recorder ever subscribed."""
    con = duckdb.connect()
    try:
        return con.sql(
            f"select distinct condition_id, asset_id, outcome from read_parquet('{cfg.data_dir}/reference/markets/**/*.parquet')"
        ).fetchall()
    except duckdb.IOException:
        return []


async def run_backfill(cfg: Config, what: str) -> int:
    mk = _recorded_markets(cfg)
    if not mk:
        log.error("no markets recorded yet (data/reference/markets is empty); run the recorder first")
        return 1
    conds = sorted({c for c, _, _ in mk})
    tokens = sorted({t for _, t, _ in mk})
    log.info("backfill %s for %d markets / %d tokens", what, len(conds), len(tokens))
    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": "polymm-backfill/0.1 (research)"}) as http:
        if what in ("all", "trades"):
            for c in conds:
                await _trades(cfg, http, c)
        if what in ("all", "prices"):
            for t in tokens:
                await _prices(cfg, http, t)
        if what in ("all", "resolutions"):
            await _resolutions(cfg, http, conds)
    return 0


async def _trades(cfg: Config, http: httpx.AsyncClient, cond: str) -> None:
    base = cfg.data_dir / "backfill" / "trades_hist" / f"condition_id={cond}"
    cursor = None
    page = 0
    total = 0
    while True:
        params = {"condition": cond, "taker_only": "true", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        r = await http.get(f"{cfg.endpoints.data_api}/v2/trades", params=params)
        if r.status_code == 429:
            await _sleep_retry(r)
            continue
        r.raise_for_status()
        body = r.json()
        rows_in = body.get("data") or body.get("trades") or (body if isinstance(body, list) else [])
        now = time.time_ns()
        rows = [
            (
                now, int(t.get("timestamp") or 0), t.get("condition_id") or t.get("conditionId") or cond,
                t.get("token_id") or t.get("asset") or "", t.get("outcome", ""), float(t.get("price") or 0),
                float(t.get("size") or 0), float(t.get("usdc_size") or t.get("usdcSize") or 0), t.get("side", ""),
                t.get("proxy_wallet") or t.get("proxyWallet") or "", t.get("transaction_hash") or t.get("transactionHash") or "",
            )
            for t in rows_in
        ]
        _write(base / f"page-{page:04d}.parquet", rows, TRADES_HIST)
        total += len(rows)
        cursor = (body.get("pagination") or {}).get("next_cursor") if isinstance(body, dict) else None
        page += 1
        if not cursor or not rows:
            break
    log.info("trades %s…: %d rows, %d pages", cond[:10], total, page)


async def _prices(cfg: Config, http: httpx.AsyncClient, token: str) -> None:
    # Why 1m for the last 7 days only: that is the retention floor; coarser buckets
    # are fetched with interval=max for the whole life of the market.
    base = cfg.data_dir / "backfill" / "prices_hist" / f"token_id={token}"
    for name, params in (
        ("bucket=60", {"token_id": token, "interval": "1w", "bucket_seconds": 60}),
        ("bucket=max", {"token_id": token, "interval": "max"}),
    ):
        r = await http.get(f"{cfg.endpoints.data_api}/v2/prices-history", params=params)
        if r.status_code != 200:
            log.warning("prices %s… %s -> %d", token[:10], name, r.status_code)
            continue
        pts = r.json().get("data") or r.json().get("history") or []
        now = time.time_ns()
        rows = [
            (now, token, int(p.get("timestamp") or p.get("t") or 0), float(p.get("price") or p.get("p") or 0),
             int(p.get("resolution_seconds") or 0))
            for p in pts
        ]
        _write(base / name / "part-0000.parquet", rows, PRICES_HIST)
        log.info("prices %s… %s: %d points", token[:10], name, len(rows))


async def _resolutions(cfg: Config, http: httpx.AsyncClient, conds: list[str]) -> None:
    out: list[dict] = []
    for i in range(0, len(conds), 20):
        chunk = conds[i : i + 20]
        r = await http.get(f"{cfg.endpoints.data_api}/v2/resolutions", params={"condition": ",".join(chunk)})
        if r.status_code != 200:
            log.warning("resolutions chunk %d -> %d", i, r.status_code)
            continue
        body = r.json()
        out.extend(body.get("data") if isinstance(body, dict) else body)
    if out:
        path = cfg.data_dir / "reference" / "resolutions" / "resolutions.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        tbl = pa.Table.from_pylist([{k: (str(v) if not isinstance(v, (int, float, bool)) and v is not None else v) for k, v in o.items()} for o in out])
        tmp = path.with_suffix(".parquet.tmp")
        pq.write_table(tbl, tmp, compression="zstd")
        os.replace(tmp, path)
    log.info("resolutions: %d rows", len(out))


async def _sleep_retry(r: httpx.Response) -> None:
    import asyncio

    wait = float(r.headers.get("retry-after", 5))
    log.warning("rate limited, sleeping %.0fs", wait)
    await asyncio.sleep(wait)

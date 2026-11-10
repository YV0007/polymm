"""R5 data: book snapshots of every market that pays liquidity rewards.

We record full books only for our three families; to rank the whole universe we sample each
paying market's YES book over REST (POST /books, 500 tokens per call) every few minutes and keep
the levels that can score (within the max spread + 1¢ of the adjusted mid) plus the side totals.
The YES book already holds the NO orders at 1 − p (verified mirror, DATA_PLAN §5), so the two
sides are Q_one and Q_two of the rules directly.

Output: <data>/reference/rewards_books/dt=<day>/snap=<HHMMSS>.parquet, one row per market.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl

from .config import CLOB, http_json, snapshot
from .rules import adjusted_mid, competing_bracket, side_scores

log = logging.getLogger(__name__)


def _levels(side: list[dict]) -> tuple[np.ndarray, np.ndarray]:
    if not side:
        return np.zeros(0), np.zeros(0)
    px = np.array([float(x["price"]) for x in side])
    sz = np.array([float(x["size"]) for x in side])
    return px, sz


def summarise(book: dict, v_c: float, min_size: float) -> dict:
    bp, bs = _levels(book.get("bids") or [])
    ap, as_ = _levels(book.get("asks") or [])
    mid = adjusted_mid(bp, bs, ap, as_, min_size)
    row = {"yes_token": book.get("asset_id"), "t_ms": int(book.get("timestamp") or 0), "tick": float(book.get("tick_size") or 0.01),
           "best_bid": float(bp.max()) if len(bp) else None, "best_ask": float(ap.min()) if len(ap) else None, "mid_adj": mid,
           "q_one": None, "q_two": None, "comp_low": None, "comp_central": None, "comp_high": None,
           "bid_px": [], "bid_sz": [], "ask_px": [], "ask_sz": []}
    if mid is None:
        return row
    q1, q2 = side_scores(bp, bs, ap, as_, mid, v_c, min_size)
    lo, ce, hi = competing_bracket(q1, q2, mid)
    keep_b = (mid - bp) * 100 <= v_c + 1
    keep_a = (ap - mid) * 100 <= v_c + 1
    row.update(q_one=q1, q_two=q2, comp_low=lo, comp_central=ce, comp_high=hi,
               bid_px=bp[keep_b].tolist(), bid_sz=bs[keep_b].tolist(), ask_px=ap[keep_a].tolist(), ask_sz=as_[keep_a].tolist())
    return row


def sample_once(markets: pl.DataFrame, batch: int = 500) -> pl.DataFrame:
    toks = markets.select("yes_token", "max_spread_c", "min_size").drop_nulls("yes_token")
    par = {r["yes_token"]: (r["max_spread_c"], r["min_size"]) for r in toks.iter_rows(named=True)}
    ids = list(par)
    rows = []
    t0 = datetime.now(timezone.utc)
    for i in range(0, len(ids), batch):
        try:
            books = http_json(f"{CLOB}/books", data=[{"token_id": t} for t in ids[i:i + batch]], timeout=120)
        except Exception as e:  # one failed batch must not cost the snapshot
            log.warning("books batch %d failed: %s", i // batch, e)
            continue
        for b in books or []:
            v, ms = par.get(b.get("asset_id"), (0.0, 0.0))
            rows.append(summarise(b, v, ms))
        time.sleep(0.5)
    df = pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()
    return df.with_columns(pl.lit(t0.isoformat()).alias("snap_at")) if df.height else df


def run_sampler(data_root: Path, every_s: int = 600, count: int = 18) -> None:
    """Snapshot the reward configs, then sample every paying, unresolved market `count` times."""
    m = snapshot(data_root)
    now = datetime.now(timezone.utc)
    # Why parse: end dates come as ISO strings ("…T00:00:00Z"); comparing strings let markets that
    # ended earlier today through (first run, 2026-11-10)
    end = pl.col("end_date").str.to_datetime(strict=False, time_zone="UTC")
    m = m.filter((pl.col("rate_per_day") > 0) & pl.col("yes_token").is_not_null() & (end.is_null() | (end > pl.lit(now))))
    log.info("sampling %d paying markets, %d × every %d s", m.height, count, every_s)
    for k in range(count):
        t = time.time()
        s = sample_once(m)
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        out = data_root / "reference" / "rewards_books" / f"dt={day}"
        out.mkdir(parents=True, exist_ok=True)
        if s.height:
            s.write_parquet(out / f"snap={datetime.now(timezone.utc).strftime('%H%M%S')}.parquet")
        log.info("snapshot %d/%d: %d books in %.0f s", k + 1, count, s.height, time.time() - t)
        if k < count - 1:
            time.sleep(max(0.0, every_s - (time.time() - t)))


if __name__ == "__main__":
    import argparse
    import sys
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data")
    ap.add_argument("--every", type=int, default=600)
    ap.add_argument("--count", type=int, default=18)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    run_sampler(Path(a.data), a.every, a.count)

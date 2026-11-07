"""Binance spot feed: the BTC price that drives the BTC markets.

Three pieces, all small:
  BinanceSource   listener for the public market-data WebSocket (stamp, enqueue)
  SpotParser      bookTicker / trade messages -> `spot_quotes` / `spot_trades` rows
  backfill_spot   daily history zips from data.binance.vision -> Parquet

Why Binance and not the resolution oracle: the BTC 15m markets resolve on a
Chainlink BTC/USD 60 s TWAP (paid stream, $150+/month, no free tier); the daily
ladder resolves on Binance BTC/USDT 1-minute closes. Binance is free, public,
and is where the price is *discovered*; the Chainlink TWAP is derived from
exchanges like it with a lag. We approximate the TWAP from our own quotes and
check that approximation against recorded resolutions later.

Terms (checked 2026-11-07): the public WebSocket needs no account or key;
limits are 1024 streams and 5 client messages/s per connection, 300 connects
per 5 min per IP, forced disconnect at 24 h, server ping every 20 s (the
websockets library answers pongs automatically). Historical zips are
CC BY-NC-SA 4.0: research and personal backtesting are permitted, live or
commercial trading use is not, so backfilled data stays research-only.
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import os
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path

import httpx
import orjson
import polars as pl
from websockets.asyncio.client import connect

from .config import Config

log = logging.getLogger(__name__)

RECONNECT = "__RECONNECT__"  # same marker convention as sources.py


# ---- live ---------------------------------------------------------------------


class BinanceSource:
    def __init__(self, url: str, queue: asyncio.Queue, symbols: list[str], streams: list[str]):
        self.queue = queue
        names = [f"{s.lower()}@{st}" for s in symbols for st in streams]
        # Why the combined-stream URL: one connection carries all streams and each
        # frame is wrapped as {"stream": name, "data": payload}, so the parser knows
        # which stream a message came from without guessing from its fields.
        self.url = f"{url.rstrip('/')}/stream?streams={'/'.join(names)}"
        self.conn_id = 0
        self.reconnects = 0
        self.connected_at: float | None = None
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                # Why default ping settings: Binance pings us every 20 s and the
                # library pongs automatically; our own 20 s pings (0.05 msg/s) are far
                # below the 5 msg/s limit and detect a dead connection in 40 s.
                async with connect(self.url, open_timeout=20, max_queue=4096) as ws:
                    self.conn_id += 1
                    self.connected_at = time.time()
                    backoff = 1.0
                    log.info("binance connected conn_id=%d url=%s", self.conn_id, self.url)
                    async for raw in ws:
                        ts = time.time_ns()  # first thing, before anything else
                        if isinstance(raw, bytes):
                            raw = raw.decode()
                        await self.queue.put((ts, self.conn_id, raw))
                # Why not an error: Binance closes every connection at the 24 h mark.
                log.info("binance connection closed by server (conn_id=%d); reconnecting", self.conn_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self._stop.is_set():
                    break
                log.warning("binance disconnected (conn_id=%d): %r; reconnecting in %.0fs", self.conn_id, e, backoff)
            self.reconnects += 1
            await self.queue.put((time.time_ns(), self.conn_id, RECONNECT))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


@dataclass
class SpotStats:
    msgs: int = 0
    quotes: int = 0
    trades: int = 0
    other: int = 0
    parse_errors: int = 0
    reconnects: int = 0


class SpotParser:
    def __init__(self, buffers: dict[str, list[tuple]]):
        self.buffers = buffers
        self.stats = SpotStats()

    def feed(self, ts_recv_ns: int, conn_id: int, raw: str) -> None:
        if raw == RECONNECT:
            self.stats.reconnects += 1
            self.buffers["spot_quotes"].append((ts_recv_ns, "", 0, None, None, None, None, conn_id))
            return
        try:
            m = orjson.loads(raw)
            d = m.get("data", m)
            stream = m.get("stream", "")
            self.stats.msgs += 1
            if stream.endswith("@bookTicker") or ("b" in d and "A" in d and "e" not in d):
                self.stats.quotes += 1
                self.buffers["spot_quotes"].append(
                    (ts_recv_ns, d["s"], int(d["u"]), float(d["b"]), float(d["B"]), float(d["a"]), float(d["A"]), conn_id)
                )
            elif d.get("e") == "trade":
                self.stats.trades += 1
                self.buffers["spot_trades"].append(
                    (ts_recv_ns, int(d["T"]), int(d["E"]), d["s"], int(d["t"]), float(d["p"]), float(d["q"]), bool(d["m"]), conn_id)
                )
            else:
                self.stats.other += 1
        except Exception as e:  # never let one bad message kill the consumer
            self.stats.parse_errors += 1
            log.warning("spot parse error: %r on %.120s", e, raw)


# ---- history ------------------------------------------------------------------

_TRADE_COLS = ["trade_id", "price", "qty", "quote_qty", "time", "buyer_is_maker", "best_match"]
_AGG_COLS = ["agg_id", "price", "qty", "first_id", "last_id", "time", "buyer_is_maker", "best_match"]


def _download(client: httpx.Client, url: str, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    with client.stream("GET", url) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_bytes(1 << 20):
                f.write(chunk)
    os.replace(tmp, dst)


def backfill_spot(cfg: Config, symbol: str, date: str, kind: str = "trades") -> Path | None:
    """One day of Binance spot history -> data/backfill/spot_<kind>_hist/symbol=X/<date>.parquet.

    Idempotent: output name is a pure function of (symbol, date, kind). Returns None
    when the day is not published yet (daily files appear the next UTC day).
    """
    out = cfg.data_dir / "backfill" / f"spot_{kind}_hist" / f"symbol={symbol}" / f"{date}.parquet"
    if out.exists():
        log.info("spot backfill %s %s %s: already present", kind, symbol, date)
        return out
    name = f"{symbol}-{kind}-{date}.zip"
    url = f"{cfg.spot.history_base}/{kind}/{symbol}/{name}"
    # Why a separate raw dir: downloads are untrusted input; they are read as data
    # only (zip -> csv -> parquet) and kept so a parse bug can be fixed offline.
    raw_dir = cfg.data_dir / "backfill" / "binance_raw"
    zp = raw_dir / name
    with httpx.Client(timeout=120, headers={"User-Agent": "polymm-recorder/0.1 (research)"}) as client:
        if not zp.exists():
            try:
                _download(client, url, zp)
                _download(client, url + ".CHECKSUM", zp.with_suffix(".zip.CHECKSUM"))
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    log.warning("spot backfill %s %s %s: not published yet (404)", kind, symbol, date)
                    return None
                raise
    expected = zp.with_suffix(".zip.CHECKSUM").read_text().split()[0]
    actual = hashlib.sha256(zp.read_bytes()).hexdigest()
    if actual != expected:
        zp.unlink()
        raise RuntimeError(f"checksum mismatch for {name}: {actual} != {expected}")

    with zipfile.ZipFile(zp) as z:
        csv = z.read(z.namelist()[0])
    cols = _TRADE_COLS if kind == "trades" else _AGG_COLS
    # Why sniff the header: Binance added header rows to some datasets in 2024;
    # a data row always starts with a digit (the trade id).
    has_header = not csv[:1].isdigit()
    df = pl.read_csv(io.BytesIO(csv), has_header=has_header, new_columns=cols if not has_header else None)
    if has_header:
        df.columns = cols
    # Why detect the unit: timestamps are microseconds from 2025-01-01, milliseconds before.
    t = df["time"].cast(pl.Int64)
    ts_us = t if t.max() > 10**14 else t * 1000
    df = (
        df.with_columns(ts_us.alias("ts_exch_us"), pl.lit(symbol).alias("symbol"))
        .drop("time")
        .with_columns(pl.col("price").cast(pl.Float64), pl.col("qty").cast(pl.Float64))
        .sort("ts_exch_us")
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.tmp")
    df.write_parquet(tmp, compression="zstd")
    os.replace(tmp, out)
    log.info("spot backfill %s %s %s: %d rows -> %s", kind, symbol, date, df.height, out)
    return out

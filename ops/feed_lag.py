"""Latency budget: how late each feed reaches the recorder, and the round trip to the CLOB API.

    python ops/feed_lag.py --data data 2026-11-09 [2026-11-10 ...]     feed lag per feed and day
    python ops/feed_lag.py --rtt 60                                    API round trip from THIS machine

Feed lag = our receive time − the venue's own timestamp (ts_recv_ns − ts_exch_ms), in ms.
Why it is meaningful: the VPS clock is NTP-disciplined to ~0.3 ms (chronyc tracking), so the
difference is network + the venue's publishing delay, not clock error. The venue stamp may be set
when the message is published rather than when the match happened, so this is a lower bound on
"event → us".
Why quantiles, not the mean: the tail (p99) is when the market bursts, which is exactly when
prices jump and stale quotes get picked off; a mean hides it.
Why the RTT runs on one kept-alive connection: a fresh TLS connection adds ~20 ms of handshake that
a live engine never pays per order (it keeps its connection warm). Run it ON THE VPS for the
numbers that matter (`ssh polymm`, then this script); from the laptop it measures the laptop.
"""
from __future__ import annotations

import argparse
import glob
import http.client
import time
from pathlib import Path

import polars as pl

FEEDS = {  # name → (glob under <data>/dt=<day>, which venue)
    "polymarket book changes": ("events/book_events", "Polymarket"),
    "polymarket top of book": ("events/tob", "Polymarket"),
    "polymarket trades": ("events/trades", "Polymarket"),
    "binance BTC trades": ("spot/spot_trades", "Binance"),
}
Q = (0.05, 0.5, 0.95, 0.99)


def feed_lag(data: Path, day: str) -> pl.DataFrame:
    rows = []
    for name, (sub, venue) in FEEDS.items():
        fs = sorted(glob.glob(str(data / sub / f"dt={day}" / "*" / "*.parquet")))
        if not fs:
            continue
        lf = pl.scan_parquet(fs)
        if "ts_exch_ms" not in lf.collect_schema().names():
            continue
        lag = lf.select((pl.col("ts_recv_ns") / 1e6 - pl.col("ts_exch_ms")).alias("lag")).collect()["lag"]
        rows.append({"day": day, "feed": name, "venue": venue, "n": len(lag),
                     **{f"p{int(q * 100)}_ms": round(float(lag.quantile(q)), 1) for q in Q}})
    return pl.DataFrame(rows)


def api_rtt(n: int = 60, host: str = "clob.polymarket.com", path: str = "/time") -> dict:
    """Round trip of a trivial GET on one warm connection (the first request, which pays the TLS
    handshake, is reported separately and left out of the quantiles)."""
    c = http.client.HTTPSConnection(host, timeout=10)
    out = []
    for i in range(n + 1):
        t0 = time.perf_counter()
        c.request("GET", path, headers={"User-Agent": "Mozilla/5.0 (polymm research)"})
        c.getresponse().read()
        out.append((time.perf_counter() - t0) * 1000)
    first, warm = out[0], sorted(out[1:])
    pick = lambda q: round(warm[min(len(warm) - 1, int(q * len(warm)))], 1)
    return {"host": host, "n": n, "first_ms": round(first, 1), **{f"p{int(q * 100)}_ms": pick(q) for q in Q}}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("days", nargs="*")
    ap.add_argument("--data", default="data")
    ap.add_argument("--rtt", type=int, default=0, help="number of warm requests to time (0 = skip)")
    a = ap.parse_args()
    pl.Config.set_tbl_cols(20)
    pl.Config.set_tbl_width_chars(200)
    if a.days:
        print(pl.concat([feed_lag(Path(a.data), d) for d in a.days], how="diagonal"))
    if a.rtt:
        print(api_rtt(a.rtt))

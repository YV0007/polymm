"""Turn a feed sample into GB/month, calibrated on what we really store for Binance spot.
Calibration: on 2026-11-12 our recorder stored 16.64 M Binance spot messages (13.10 M quotes +
3.54 M trades) as 162 MB raw zstd-12 + 242 MB parquet = 404 MB/day. So
  activity factor  a = 16.64 M / (sample msgs/s of the same stream × 86 400)
  storage factor   f = 404 MB / (16.64 M × sample zstd bytes per msg)
and every feed: GB/month = sample zstd B/s × 86 400 × 30 × a × f / 1e9."""
import json, sys
d = json.load(open(sys.argv[1])); F = d["feeds"]
ref = F["binance_spot.bookTicker+trade (recorded now)"]
T = ref["seconds"]
DAY_MSGS, DAY_MB = 16.64e6, 404.0
a = DAY_MSGS / (ref["msgs"] / T * 86400)
f = DAY_MB * 1e6 / (DAY_MSGS * ref["zstd12_bytes"] / ref["msgs"])
print(f"sample start {d['t0_utc']}, {T:.0f} s; activity factor a = {a:.2f}, storage factor f = {f:.2f}")
print(f"{'feed':48s} {'msg/s':>8s} {'raw B/msg':>9s} {'zstd x':>7s} {'GB/mo stored':>12s}  error")
rows = []
for k, v in F.items():
    if not v["msgs"]:
        print(f"{k:48s} {'0':>8s}  {v['error']}"); continue
    rate = v["msgs"] / T
    gbm = v["zstd12_bytes"] / T * 86400 * 30 * a * f / 1e9
    rows.append((k, gbm))
    print(f"{k:48s} {rate:8.1f} {v['raw_bytes']/v['msgs']:9.0f} {v['raw_bytes']/max(v['zstd12_bytes'],1):7.1f} {gbm:12.2f}  {v['error'] or ''}")

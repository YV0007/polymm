"""F0c: the input grid. One row per (YES token, second) inside strict clean windows.

Every column is something we KNEW at second t on our own receive clock:

    book    best bid/ask, sizes, mid, microprice    last replay `tob` row with ts <= t
    spot    Binance BTC/USDT mid S, its age, the trailing 60 s mean S60
            live days: bookTicker mid with OUR receive stamp <= t
            history days: last trade with exchange time + measured latency <= t
    sigma   step-5 forecast σ per √s (EWMA λ=0.99, hour-deseasonalised; a 1-min bar
            is used from 60 s after it closed)
    ref     strike K, window start/end, outcome (Gamma, F0b)

plus, for SCORING only (never fed to a model): the mid τ seconds later inside the same
clean window, τ ∈ {1, 5, 30, 60}, and two oracle spot prices (zero-latency S, S one
second ahead) used to bound what a faster feed could be worth.

Why a 1 s grid, not every event: all horizons are ≥ 1 s, and a regular grid weights
time, not activity (a burst of 50 deltas in one second is one observation of the state).
Why strict windows: [t_start, last_verified] is the part of each window a REST or WS
snapshot confirmed. Knowing a window was clean is hindsight, but it only decides which
seconds are scored, not what any model sees.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

log = logging.getLogger(__name__)

NS = 1_000_000_000
TAUS = (1, 5, 30, 60)
P_CLIP = 1e-3  # log-odds of 0 or 1 is infinite; the venue's own price range is [0.001, 0.999]


def logit(p: pl.Expr) -> pl.Expr:
    q = p.clip(P_CLIP, 1 - P_CLIP)
    return (q / (1 - q)).log()


def day_bounds(date: str) -> tuple[int, int]:
    d0 = int(datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    return d0, d0 + 86400


# ---- spot -------------------------------------------------------------------------------


def _dates_between(s0: int, s1: int) -> list[str]:
    """Every UTC date touched by [s0, s1). Why not just the two ends: a range from the evening
    before to the morning after spans three dates, and the middle one is the day itself
    (that bug once fed a whole day a stale Binance price)."""
    return [datetime.fromtimestamp(d, tz=timezone.utc).strftime("%Y-%m-%d") for d in range(s0 - s0 % 86400, s1, 86400)]


def _live_quotes(spot_root: Path, s0: int, s1: int) -> pl.DataFrame:
    dates = _dates_between(s0, s1)
    files = [f for d in dates for f in (spot_root / "spot" / "spot_quotes" / f"dt={d}").rglob("*.parquet")]
    if not files:
        return pl.DataFrame(schema={"ts": pl.Int64, "px": pl.Float64})
    return (pl.scan_parquet(files).filter((pl.col("symbol") != "") & pl.col("bid").is_not_null())
            .select(pl.col("ts_recv_ns").alias("ts"), ((pl.col("bid") + pl.col("ask")) / 2).alias("px"))
            .filter((pl.col("ts") >= s0 * NS) & (pl.col("ts") < s1 * NS)).collect().sort("ts"))


def _hist_trades(spot_root: Path, s0: int, s1: int, latency_ms: float) -> pl.DataFrame:
    """History trades on the exchange clock. Why shift by the measured latency: on the
    server, a trade arrives a median 129 ms after its exchange time (Tokyo → Falkenstein);
    without the shift a history day would see Binance earlier than any live day can."""
    dates = _dates_between(s0, s1)
    files = [spot_root / "backfill" / "spot_trades_hist" / "symbol=BTCUSDT" / f"{d}.parquet" for d in dates]
    files = [f for f in files if f.exists()]
    if not files:
        return pl.DataFrame(schema={"ts": pl.Int64, "px": pl.Float64, "ts_exch": pl.Int64})
    return (pl.scan_parquet(files).select((pl.col("ts_exch_us") * 1000).alias("ts_exch"), pl.col("price").alias("px"))
            .with_columns((pl.col("ts_exch") + int(latency_ms * 1e6)).alias("ts"))
            .filter((pl.col("ts") >= s0 * NS) & (pl.col("ts") < s1 * NS)).collect().sort("ts"))


def measure_latency_ms(spot_root: Path) -> float:
    """Median (our receive time − Binance event time) over live trades: one-way latency plus
    clock offset, the quantity that matters for "when could we have known"."""
    files = list((spot_root / "spot" / "spot_trades").rglob("*.parquet"))
    if not files:
        return 129.0
    return float(pl.scan_parquet(files).filter(pl.col("symbol") != "")
                 .select((pl.col("ts_recv_ns") / 1e6 - pl.col("event_ms")).median()).collect().item())


def spot_grid(date: str, spot_root: Path, latency_ms: float, pre_s: int = 1200) -> pl.DataFrame:
    """1 s grid of what we knew about BTC at each second, from `pre_s` before the day.

    Columns: sec, S (price known at sec), S_age_s, S_src (live | hist), S60 (mean of S over
    the last 60 grid seconds, sec−59..sec), S_zero (oracle: the price with zero latency),
    S_lead1 (oracle: S one second later)."""
    d0, d1 = day_bounds(date)
    s0, s1 = d0 - pre_s, d1 + 2
    grid = pl.DataFrame({"sec": pl.int_range(s0, s1, eager=True)}).with_columns((pl.col("sec") * NS).alias("t"))
    live = _live_quotes(spot_root, s0 - 120, s1)
    hist = _hist_trades(spot_root, s0 - 120, s1, latency_ms)
    out = grid
    if live.height:
        out = out.join_asof(live.rename({"ts": "ts_live", "px": "px_live"}), left_on="t", right_on="ts_live", strategy="backward")
        # zero-latency oracle: what Binance showed at t, which reached us L ms later
        z = live.with_columns((pl.col("ts") - int(latency_ms * 1e6)).alias("ts_zero")).select("ts_zero", pl.col("px").alias("px_zero_live"))
        out = out.join_asof(z, left_on="t", right_on="ts_zero", strategy="backward")
    else:
        out = out.with_columns(pl.lit(None, pl.Int64).alias("ts_live"), pl.lit(None, pl.Float64).alias("px_live"),
                               pl.lit(None, pl.Int64).alias("ts_zero"), pl.lit(None, pl.Float64).alias("px_zero_live"))
    if hist.height:
        out = out.join_asof(hist.select(pl.col("ts").alias("ts_hist"), pl.col("px").alias("px_hist")), left_on="t", right_on="ts_hist", strategy="backward")
        out = out.join_asof(hist.select(pl.col("ts_exch").alias("ts_hz"), pl.col("px").alias("px_zero_hist")), left_on="t", right_on="ts_hz", strategy="backward")
    else:
        out = out.with_columns(pl.lit(None, pl.Int64).alias("ts_hist"), pl.lit(None, pl.Float64).alias("px_hist"),
                               pl.lit(None, pl.Int64).alias("ts_hz"), pl.lit(None, pl.Float64).alias("px_zero_hist"))
    live_ok = pl.col("ts_live").is_not_null() & ((pl.col("t") - pl.col("ts_live")) <= 5 * NS)
    out = out.with_columns(
        pl.when(live_ok).then(pl.col("px_live")).otherwise(pl.col("px_hist")).alias("S"),
        pl.when(live_ok).then(pl.col("ts_live")).otherwise(pl.col("ts_hist")).alias("S_ts"),
        pl.when(live_ok).then(pl.lit("live")).when(pl.col("px_hist").is_not_null()).then(pl.lit("hist")).otherwise(None).alias("S_src"),
        pl.when(live_ok).then(pl.col("px_zero_live")).otherwise(pl.col("px_zero_hist")).alias("S_zero"),
    ).with_columns(((pl.col("t") - pl.col("S_ts")) / NS).alias("S_age_s"))
    out = out.with_columns(
        pl.col("S").rolling_mean(window_size=60, min_samples=45).alias("S60"),
        pl.col("S").shift(-1).alias("S_lead1"),
    )
    return out.select("sec", "S", "S_ts", "S_age_s", "S_src", "S60", "S_zero", "S_lead1")


# ---- book grid ------------------------------------------------------------------------------


def _tob_last_per_second(day_dir: Path, assets: list[str]) -> pl.DataFrame:
    """The replayed top of book, reduced to the last valid update per (token, second in which it
    becomes visible on the 1 s grid, ⌈ts⌉).

    Why (D-035): a day has 40–65 M updates, 4–8 GB in memory with the token id as text (11-06
    alone peaked at 5.2 GB here), but the as-of join in `book_grid` only ever picks the LAST
    update at or before each whole second, so every other update is dead weight. Why one hour
    folder at a time: then only one hour of raw updates (~2–3 M rows) is ever in memory.
    Exact: the latest update ≤ t is the latest of its ⌈ts⌉ bucket, and keeping each folder's
    own latest per bucket keeps it (a bucket split across two folders just keeps two rows)."""
    cols = ["tob_ts", "bid", "ask", "bid_sz", "ask_sz", "mid", "micro", "spread"]
    folders = sorted({p.parent for p in (day_dir / "replay" / "derived" / "tob").rglob("*.parquet")})
    parts = []
    for f in folders:
        parts.append(
            pl.scan_parquet(str(f / "*.parquet"))
            .filter(pl.col("asset_id").is_in(assets) & pl.col("valid") & (pl.col("event_type") != "reconnect"))
            .select(pl.col("ts_recv_ns").alias("tob_ts"), "asset_id", pl.col("best_bid").alias("bid"), pl.col("best_ask").alias("ask"),
                    pl.col("bid_size").alias("bid_sz"), pl.col("ask_size").alias("ask_sz"), "mid", pl.col("microprice").alias("micro"), "spread")
            .group_by("asset_id", ((pl.col("tob_ts") + NS - 1) // NS).alias("_s"))
            .agg([pl.col(c).get(pl.col("tob_ts").arg_max()) for c in cols])  # arg_max: the latest update of the bucket
            .drop("_s").collect())
    if not parts:
        return pl.DataFrame(schema={"tob_ts": pl.Int64, "asset_id": pl.String, **{c: pl.Float64 for c in cols[1:]}})
    return pl.concat(parts).sort("tob_ts")


def book_grid(day_dir: Path, date: str, assets: list[str]) -> pl.DataFrame:
    """Per YES token: one row per second inside its strict clean windows, with the last
    valid top of book at or before that second, and the future mids for scoring."""
    d0, d1 = day_bounds(date)
    sw = (pl.read_parquet(day_dir / "clean_windows_strict.parquet")
          .filter(pl.col("asset_id").is_in(assets)))
    end_col = "last_verified" if "last_verified" in sw.columns else "t_end"
    sw = (sw.with_columns(pl.coalesce(pl.col(end_col), pl.col("t_start")).alias("t_last"))
          .with_columns(((pl.col("t_start") + NS - 1) // NS).alias("s_a"), (pl.col("t_last") // NS).alias("s_b"))
          .with_columns(pl.max_horizontal("s_a", pl.lit(d0)).alias("s_a"), pl.min_horizontal("s_b", pl.lit(d1 - 1)).alias("s_b"))
          .filter(pl.col("s_b") > pl.col("s_a"))
          .with_row_index("wid"))
    if not sw.height:
        return pl.DataFrame()
    g = (sw.select("wid", "asset_id", pl.int_ranges("s_a", pl.col("s_b") + 1).alias("sec")).explode("sec")
         .with_columns((pl.col("sec") * NS).alias("t")).sort("t"))
    tob = _tob_last_per_second(day_dir, assets)
    g = g.join_asof(tob, left_on="t", right_on="tob_ts", by="asset_id", strategy="backward")
    g = g.filter(pl.col("mid").is_not_null()).sort("wid", "sec")
    # future mids inside the same window. Why a join, not a shift: seconds with a one-sided
    # book (no mid) are dropped above, so row k ahead is not always k seconds ahead.
    m = g.select("wid", "sec", "mid")
    for k in TAUS:
        g = g.join(m.select("wid", (pl.col("sec") - k).alias("sec"), pl.col("mid").alias(f"mid_f{k}")), on=["wid", "sec"], how="left")
    return g.sort("wid", "sec")


# ---- per-window BTC features (15m) ---------------------------------------------------------------


def window_spot_features(r15: pl.DataFrame, sp: pl.DataFrame) -> pl.DataFrame:
    """Per 15-minute market and second, the BTC quantities that depend on the window.

    A_known, A_known_zero: inside the last 60 s, the mean of S over [T−60, t] (the part of
        the settlement average already fixed); null before. Why: settlement is the 60 s
        average ending at T, so with τ < 60 s left, (60−τ)/60 of it is history, not risk.
    Oracles (use the future; scoring only):
        rv30_s    realized σ per √s over the window from 30 s returns (the signature plot
                  showed 1 s returns understate variance by ~32 %, 30 s does not)
        basis_T   ln(S60 at T / finalPrice): the Binance–Chainlink gap at the END
    """
    if not r15.height:
        return pl.DataFrame()
    w = r15.select("condition_id", "t_start_s", "t_end_s", "final_price")
    secs = w.select("condition_id", "t_start_s", "t_end_s", "final_price",
                    pl.int_ranges(pl.col("t_start_s") - 60, pl.col("t_end_s") + 1).alias("sec")).explode("sec")
    s = secs.join(sp.select("sec", "S", "S_zero", "S60"), on="sec", how="left").sort("condition_id", "sec")
    last = pl.col("sec") >= pl.col("t_end_s") - 60
    s = s.with_columns(
        pl.when(last).then(pl.col("S")).otherwise(None).cum_sum().over("condition_id").alias("_cs"),
        pl.when(last).then(pl.col("S_zero")).otherwise(None).cum_sum().over("condition_id").alias("_csz"),
        pl.when(last & pl.col("S").is_not_null()).then(1).otherwise(0).cum_sum().over("condition_id").alias("_n"),
        pl.when(last & pl.col("S_zero").is_not_null()).then(1).otherwise(0).cum_sum().over("condition_id").alias("_nz"),
    ).with_columns(
        pl.when(last & (pl.col("_n") > 0)).then(pl.col("_cs") / pl.col("_n")).otherwise(None).alias("A_known"),
        pl.when(last & (pl.col("_nz") > 0)).then(pl.col("_csz") / pl.col("_nz")).otherwise(None).alias("A_known_zero"),
    )
    # oracle σ: 30 s log returns over [t_start, T]
    r30 = (s.filter((pl.col("sec") >= pl.col("t_start_s")) & (((pl.col("sec") - pl.col("t_start_s")) % 30) == 0))
           .with_columns(pl.col("S").log().diff().over("condition_id").alias("r"))
           .group_by("condition_id").agg(((pl.col("r") ** 2).sum() / (pl.col("r").is_not_null().sum() * 30)).sqrt().alias("rv30_s")))
    bT = (s.filter(pl.col("sec") == pl.col("t_end_s"))
          .select("condition_id", (pl.col("S60") / pl.col("final_price")).log().alias("basis_T")))
    feats = s.filter(pl.col("sec") < pl.col("t_end_s")).select("condition_id", "sec", "A_known", "A_known_zero")
    return feats.join(r30, on="condition_id", how="left").join(bT, on="condition_id", how="left")


# ---- assemble ---------------------------------------------------------------------------------


def build_inputs(date: str, day_dir: Path, spot_root: Path, ref: pl.DataFrame, latency_ms: float) -> pl.DataFrame:
    d0, d1 = day_bounds(date)
    r = ref.filter(pl.col("yes_asset").is_not_null())
    # Why: a ladder or Fed market is live for days; a 15-minute market only matters from its
    # window start (before that its strike is not even known) to its end.
    r = r.filter(((pl.col("family") == "btc_15m") & (pl.col("t_end_s") > d0) & (pl.col("t_start_s") < d1))
                 | ((pl.col("family") != "btc_15m") & (pl.col("t_end_s") > d0)))
    g = book_grid(day_dir, date, r["yes_asset"].to_list())
    if not g.height:
        return pl.DataFrame()
    g = g.join(r.select(pl.col("yes_asset").alias("asset_id"), "family", "event_slug", "condition_id", "t_start_s", "t_end_s",
                        pl.col("strike").alias("K"), "final_price", "outcome"), on="asset_id", how="inner")
    g = g.filter((pl.col("sec") < pl.col("t_end_s"))
                 & ((pl.col("family") != "btc_15m") | (pl.col("sec") >= pl.col("t_start_s"))))
    sp = spot_grid(date, spot_root, latency_ms)
    g = g.join(sp, on="sec", how="left")
    # basis to Chainlink at the window start: ln(S60 at start / priceToBeat); 15m only
    b = (r.filter(pl.col("family") == "btc_15m").select("condition_id", "t_start_s", "strike")
         .join(sp.select(pl.col("sec").alias("t_start_s"), pl.col("S60").alias("S60_start")), on="t_start_s", how="left")
         .with_columns((pl.col("S60_start") / pl.col("strike")).log().alias("basis0")).select("condition_id", "S60_start", "basis0"))
    g = g.join(b, on="condition_id", how="left")
    wf = window_spot_features(r.filter(pl.col("family") == "btc_15m"), sp)
    if wf.height:
        g = g.join(wf, on=["condition_id", "sec"], how="left")
    else:
        g = g.with_columns([pl.lit(None, pl.Float64).alias(c) for c in ("A_known", "A_known_zero", "rv30_s", "basis_T")])
    sig_path = day_dir / "vol" / "sigma_btc.parquet"
    if sig_path.exists():
        sig = pl.read_parquet(sig_path).select("sec", (pl.col("sigma_fc_1m") / math.sqrt(60)).alias("sigma_s"),
                                               pl.col("ewma_age_s").alias("sigma_age_s"), pl.col("sigma_rv15_1s"))
        g = g.join(sig, on="sec", how="left")
    else:
        log.warning("%s: no sigma_btc.parquet, model inputs incomplete", date)
        g = g.with_columns(pl.lit(None, pl.Float64).alias("sigma_s"), pl.lit(None, pl.Float64).alias("sigma_age_s"),
                           pl.lit(None, pl.Float64).alias("sigma_rv15_1s"))
    g = g.with_columns(
        pl.lit(date).alias("date"),
        (pl.col("t_end_s") - pl.col("sec")).cast(pl.Float64).alias("tau"),
        pl.when(pl.col("family") == "btc_15m").then((pl.col("sec") - pl.col("t_start_s")) // 60).otherwise(None).alias("minute"),
        # Why cluster per 15m window but per market-hour elsewhere: one 15m window is one BTC
        # path; a ladder market lives two days, so an hour is the unit of near-independence.
        pl.when(pl.col("family") == "btc_15m").then(pl.col("condition_id"))
          .otherwise(pl.col("condition_id") + "|" + (pl.col("sec") // 3600).cast(pl.String)).alias("cluster"),
        logit(pl.col("mid")).alias("x_mid"),
    ).with_columns([logit(pl.col(f"mid_f{k}")).alias(f"x_mid_f{k}") for k in TAUS])
    return g.sort("asset_id", "sec")


def check_inputs(g: pl.DataFrame) -> dict:
    """No-lookahead and coverage checks. Raises on lookahead (that would make every score a lie)."""
    t = pl.col("sec") * NS
    bad_book = g.filter(pl.col("tob_ts") > t).height
    bad_spot = g.filter(pl.col("S_ts").is_not_null() & (pl.col("S_ts") > t)).height
    bad_sig = g.filter(pl.col("sigma_age_s").is_not_null() & (pl.col("sigma_age_s") < 0)).height
    if bad_book or bad_spot or bad_sig:
        raise AssertionError(f"lookahead: book {bad_book}, spot {bad_spot}, sigma {bad_sig} rows")
    live = g.filter(pl.col("S_src") == "live")
    return {
        "rows": g.height,
        "by_family": dict(g.group_by("family").len().iter_rows()),
        "spot_missing": g.filter(pl.col("S").is_null()).height,
        "spot_age_p99_s": float(live["S_age_s"].quantile(0.99)) if live.height else None,
        "spot_src": dict(g.group_by("S_src").len().iter_rows()),
        "sigma_missing": g.filter(pl.col("sigma_s").is_null()).height,
        "basis_missing_15m": g.filter((pl.col("family") == "btc_15m") & pl.col("basis0").is_null()).height,
    }

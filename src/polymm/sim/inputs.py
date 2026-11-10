"""S0: everything the simulator replays for one day, on OUR receive clock.

    levels, trades, top of book   stage 5's loaders (queue.events.load_day): the per-level
                                  update stream with consumption / adds / cancels and pseudo-L3,
                                  trades timed at their taker order's start
    fair value at every tick      stage 3's walk-forward fair value at each whole second, moved
                                  between seconds by every Binance quote (below)
    feed-lag extra per second     how far the Polymarket feed lagged in each second, above its
                                  normal ~12 ms: the burst proxy for our own order latency (D-031)
    segments                      where we may quote: strict clean window ∩ live window

Fair value between seconds. Stage 3's fair value (fair_oos, D-024) is
    x_fair = x_book + w · (x_anch − x_book),   x_anch = x_model + EWMA(x_book − x_model)
on a 1 s grid. Inside a second the book part and the EWMA offset are held; what moves is the
BTC model, through the spot price S. So at a Binance quote at time t inside second k:

    x_tick(t) = x_fair(k) + w(k) · [x_model(S_t; k) − x_model(S_k; k)]

with every other model input (strike, Chainlink basis, σ, τ, the known part of the settlement
average) as at second k. At t = k·1s exactly this is stage 3's number (checked). Why: stage 5
named the 1 s fair-value grid as the stale signal's weak point; Binance moves ~3 times a second.
"""
from __future__ import annotations

import glob
import json
import logging
import math
from pathlib import Path

import numpy as np
import polars as pl

from ..fair.inputs import build_inputs, measure_latency_ms
from ..fair.models import SETTLE_AVG_S, norm_cdf
from ..fair.run import apply_params, candidates, fit_params
from ..markouts.fills import NS, data_root, fee_table, market_index
from ..queue.events import PK, TICK, load_day

log = logging.getLogger(__name__)

FAMS = ("btc_15m", "fed_dec_2026")
P_CLIP = 1e-3
NORMAL_FEED_LAG_MS = 12.0  # D-031: Polymarket feed p50 on the VPS


# ------------------------------------------------------------------------------------------------
# fair value: whole seconds (walk-forward, with the model's inputs kept) and ticks
# ------------------------------------------------------------------------------------------------
KEEP = ["date", "family", "condition_id", "asset_id", "sec", "t_end_s", "mid", "x_fair", "w_x_fair", "x_model",
        "S", "K", "basis0", "sigma_s", "tau", "A_known"]


def fair_seconds(dates: list[str], fair_root: Path, spot_root: Path, out: Path, force: bool = False) -> dict[str, Path]:
    """Per day: stage 3's walk-forward fair value per (token, second), plus what the tick update
    needs. Same fold rule as markouts.fair_oos (day d uses parameters fitted on the days before
    it; the first day uses its own fit), so the grid values equal fair_oos exactly."""
    paths = {d: out / d / "fair_sec.parquet" for d in dates}
    if not force and all(p.exists() for p in paths.values()):
        return paths
    fit_tau = json.loads((fair_root / "fair_params.json").read_text()).get("fit_tau", 30)
    ref = pl.read_parquet(fair_root / "reference" / "markets_ref.parquet")
    lat = measure_latency_ms(spot_root)
    G = pl.concat([build_inputs(d, fair_root / d, spot_root, ref, lat) for d in dates], how="diagonal_relaxed")
    G, _ = candidates(G)
    for i, d in enumerate(dates):
        train = dates[:i] or [d]
        params = fit_params(G.filter(pl.col("date").is_in(train)), fit_tau)
        T, _ = apply_params(G.filter((pl.col("date") == d) & pl.col("family").is_in(list(FAMS))), params)
        T = T.with_columns(pl.col("w_x_fair").fill_null(0.0))
        T = T.select([c for c in KEEP if c in T.columns]).sort("asset_id", "sec")
        # variance per second of the fair PRICE over the last 60 s (causal): the σ_p² of the
        # reservation price r = m − q γ σ_p² (T − t)
        p = 1 / (1 + (-pl.col("x_fair")).exp())
        T = T.with_columns(p.alias("p_fair")).with_columns(
            pl.col("p_fair").diff().over("asset_id").pow(2).rolling_mean(60, min_samples=20).over("asset_id").alias("v2"))
        paths[d].parent.mkdir(parents=True, exist_ok=True)
        T.write_parquet(paths[d])
        log.info("fair_sec %s (params from %s): %d rows", d, train, T.height)
    return paths


def _phi_x(d: np.ndarray) -> np.ndarray:
    p = np.clip(norm_cdf(d), P_CLIP, 1 - P_CLIP)
    return np.log(p / (1 - p))


def _spot_ticks(spot_root: Path, date: str) -> pl.DataFrame:
    fs = glob.glob(str(spot_root / "spot" / "spot_quotes" / f"dt={date}" / "*" / "*.parquet"))
    if not fs:
        return pl.DataFrame(schema={"t": pl.Int64, "S_t": pl.Float64})
    q = (pl.scan_parquet(fs).filter((pl.col("symbol") == "BTCUSDT") & pl.col("bid").is_not_null())
         .select(pl.col("ts_recv_ns").alias("t"), ((pl.col("bid") + pl.col("ask")) / 2).alias("S_t")).collect().sort("t"))
    # only the instants the mid changed: a bookTicker update that moves sizes only says nothing new
    return q.filter(pl.col("S_t") != pl.col("S_t").shift(1).fill_null(-1.0))


def fair_ticks(fs: pl.DataFrame, spot: pl.DataFrame) -> pl.DataFrame:
    """(yes, t, p, v2): the fair price at every whole second and at every Binance mid change in
    between (BTC 15m only; other families change once a second)."""
    sec_rows = fs.select(pl.col("asset_id").alias("yes"), (pl.col("sec") * NS).alias("t"), "p_fair", "v2")
    b = fs.filter((pl.col("family") == "btc_15m") & (pl.col("w_x_fair") > 0) & pl.col("x_model").is_finite()
                  & pl.col("sigma_s").is_not_null() & pl.col("basis0").is_not_null())
    if not b.height or not spot.height:
        return sec_rows.rename({"p_fair": "p"}).sort("yes", "t")
    sp = spot.with_columns((pl.col("t") // NS).alias("sec")).filter(pl.col("t") % NS != 0)
    j = sp.join(b, on="sec", how="inner")
    tau = j["tau"].to_numpy().astype(float)
    S_t = j["S_t"].to_numpy()
    A = j["A_known"].to_numpy().astype(float)
    last = tau <= SETTLE_AVG_S
    ea = np.where(last & np.isfinite(A), ((SETTLE_AVG_S - tau) * A + tau * S_t) / SETTLE_AVG_S, S_t)
    tau_eff = np.where(tau > SETTLE_AVG_S, tau - 40.0, tau ** 3 / (3 * SETTLE_AVG_S ** 2))
    d = (np.log(ea) - j["basis0"].to_numpy() - np.log(j["K"].to_numpy())) / (j["sigma_s"].to_numpy() * np.sqrt(np.maximum(tau_eff, 1e-9)))
    x_new = _phi_x(d)
    x = j["x_fair"].to_numpy() + j["w_x_fair"].to_numpy() * (x_new - j["x_model"].to_numpy())
    ticks = pl.DataFrame({"yes": j["asset_id"], "t": j["t"], "p_fair": 1 / (1 + np.exp(-x)), "v2": j["v2"]})
    return pl.concat([sec_rows, ticks]).rename({"p_fair": "p"}).drop_nulls("p").sort("yes", "t")


# ------------------------------------------------------------------------------------------------
# feed lag per second (the burst proxy) and the day's segments
# ------------------------------------------------------------------------------------------------
def feed_lag_extra(root: Path, date: str, assets: list[str]) -> pl.DataFrame:
    """(sec, extra_ms): median Polymarket feed lag in that second minus the normal 12 ms, ≥ 0.
    Why the median per second, used for the NEXT second: it is what we could have observed
    (no lookahead), and bursts last seconds, not milliseconds."""
    fs = glob.glob(str(root / "events" / "book_events" / f"dt={date}" / "*" / "*.parquet"))
    if not fs:
        return pl.DataFrame(schema={"sec": pl.Int64, "extra_ms": pl.Float64})
    g = (pl.scan_parquet(fs).filter(pl.col("asset_id").is_in(assets))
         .select((pl.col("ts_recv_ns") // NS).alias("sec"), (pl.col("ts_recv_ns") / 1e6 - pl.col("ts_exch_ms")).alias("lag"))
         .group_by("sec").agg(pl.col("lag").median()).collect().sort("sec"))
    return g.select((pl.col("sec") + 1).alias("sec"), (pl.col("lag") - NORMAL_FEED_LAG_MS).clip(0, 5000).alias("extra_ms"))


def segments(idx: pl.DataFrame, windows: pl.DataFrame, date: str) -> pl.DataFrame:
    """Where we may quote: each strict clean window of a YES token, cut to the market's live
    window (BTC 15m: [start, end)) and to the day. Data gaps end a segment: our orders are
    cancelled there (we would be blind), inventory carries to the next segment."""
    from ..fair.inputs import day_bounds
    d0, d1 = day_bounds(date)
    mk = (idx.filter((pl.col("oi") == 0) & pl.col("family").is_in(list(FAMS)))
          .select("yes", "family", "condition_id", "t_start_s", "t_end_s", "outcome").unique("yes"))
    w = windows.join(mk, on="yes", how="inner")
    lo = pl.when(pl.col("family") == "btc_15m").then(pl.max_horizontal(pl.col("t_start"), pl.col("t_start_s") * NS)).otherwise(pl.col("t_start"))
    hi = pl.min_horizontal(pl.col("t_last"), pl.col("t_end_s") * NS, pl.lit(d1 * NS))
    w = w.with_columns(pl.max_horizontal(lo, pl.lit(d0 * NS)).alias("t0"), hi.alias("t1")).filter(pl.col("t1") - pl.col("t0") > 5 * NS)
    return w.select("yes", "family", "condition_id", "t_start_s", "t_end_s", "outcome", "t0", "t1").sort("yes", "t0")


# ------------------------------------------------------------------------------------------------
# one day, everything
# ------------------------------------------------------------------------------------------------
def load_sim_day(date: str, fair_root: Path, data_dirs: list[Path], spot_root: Path, fair_sec_path: Path, idx: pl.DataFrame) -> dict:
    day_dir = fair_root / date
    root = data_root(day_dir, data_dirs, date)
    ix = idx.filter(pl.col("family").is_in(list(FAMS)))
    D = load_day(date, day_dir, root, ix)
    fs = pl.read_parquet(fair_sec_path)
    ticks = fair_ticks(fs, _spot_ticks(spot_root, date))
    seg = segments(ix, D["windows"], date)
    lag = feed_lag_extra(root, date, ix["asset_id"].to_list())
    return {"levels": D["levels"], "trades": D["trades"], "tob": D["tob"], "fair": ticks, "fair_sec": fs, "segments": seg,
            "lag": lag, "checks": D["checks"], "root": root}


def check_inputs(S: dict, fair_oos_path: Path | None = None) -> dict:
    """S0 checks. (1) our whole-second fair value is stage 3/4's walk-forward series (fair_oos)
    exactly; (2) the tick formula evaluated at the second's own spot price returns that second's
    fair value (so ticks move the fair value only by what BTC did since); (3) coverage: share of
    BTC 15m segment seconds with a fair value. (An earlier version compared the second rows with
    the table they were copied from, which is always 0: audit 6.)"""
    fs, ft = S["fair_sec"], S["fair"]
    err = None
    if fair_oos_path is not None and fair_oos_path.exists():
        oos = pl.read_parquet(fair_oos_path).select("asset_id", "sec", pl.col("p_fair").alias("p_oos"))
        j = fs.join(oos, on=["asset_id", "sec"], how="inner")
        err = float((j["p_fair"] - j["p_oos"]).abs().max()) if j.height else None
    self_err = None
    b = fs.filter((pl.col("family") == "btc_15m") & (pl.col("w_x_fair") > 0) & pl.col("x_model").is_finite() & pl.col("S").is_not_null()
                  & pl.col("sigma_s").is_not_null() & pl.col("basis0").is_not_null())
    if b.height:
        sp = b.select((pl.col("sec") * NS + 1).alias("t"), pl.col("S").alias("S_t")).unique("t").sort("t")
        tk = fair_ticks(b.head(0).vstack(b), sp).filter(pl.col("t") % NS == 1).join(
            b.select(pl.col("asset_id").alias("yes"), (pl.col("sec") * NS + 1).alias("t"), "p_fair"), on=["yes", "t"], how="inner")
        self_err = float((tk["p"] - tk["p_fair"]).abs().max()) if tk.height else None
    sub = ft.filter(pl.col("t") % NS != 0)
    seg = S["segments"].filter(pl.col("family") == "btc_15m")
    covered = None
    if seg.height:
        secs = (seg.select("yes", pl.int_ranges(pl.col("t0") // NS + 1, pl.col("t1") // NS).alias("sec")).explode("sec").drop_nulls())
        have = fs.select(pl.col("asset_id").alias("yes"), "sec").unique()
        covered = round(secs.join(have, on=["yes", "sec"], how="semi").height / max(secs.height, 1), 4)
    jumps = (ft.sort("yes", "t").with_columns(pl.col("p").diff().over("yes").abs().alias("dp"))
             .filter(pl.col("t") % NS != 0)["dp"])
    return {"fair_grid_max_abs_err": err, "fair_tick_at_own_spot_max_abs_err": self_err, "fair_ticks_between_seconds": sub.height,
            "fair_tick_move_p99_c": round(float(jumps.quantile(0.99)) * 100, 3) if jumps.len() else None,
            "btc15m_segment_seconds_with_fair": covered, "segments": S["segments"].height,
            "segments_by_family": dict(S["segments"].group_by("family").len().iter_rows())}


def rebate_k(fees: pl.DataFrame) -> dict[str, float]:
    """family → r·f: the maker rebate per share is r·f·p(1−p) USD (D-029)."""
    return {r["family"]: float(r["fee_rate"]) * float(r["fee_rebate_rate"]) for r in fees.iter_rows(named=True)}


__all__ = ["fair_seconds", "fair_ticks", "feed_lag_extra", "segments", "load_sim_day", "check_inputs", "rebate_k",
           "market_index", "fee_table", "PK", "TICK", "NS"]

"""S0: everything the simulator replays for one day, on OUR receive clock.

    levels, trades, top of book   stage 5's loaders (queue.events.load_day): the per-level
                                  update stream with consumption / adds / cancels and pseudo-L3,
                                  trades timed at their taker order's start
    fair value at every event     stage 3's walk-forward fair value, recomputed at every change
                                  of the YES book's touch and at every Binance mid change (below)
    feed-lag extra per second     how far the Polymarket feed lagged in each second, above its
                                  normal ~12 ms: the burst proxy for our own order latency (D-031)
    segments                      where we may quote: strict clean window ∩ live window

Fair value between the grid seconds (D-038). Stage 3's fair value (fair_oos, D-024) is
    x_fair = x_book + w · (x_anch − x_book),   x_anch = x_model + off,   off = EWMA(x_book − x_model)
on a 1 s grid, with x_book the Stoikov table applied to the touch (mid + adj[spread, imbalance]).
Inside second k the fitted pieces are held — w_k, the offset off_k, σ, τ, the known part of the
settlement average — and the two observed inputs move the value as they arrive:

    book touch changes at t     x_book(t) = logit(mid_t) + adj[spr_b(t), imb_b(t)]
    Binance mid changes at t    x_model(S_t; k)
    x(t) = x_book(t) + w_k · [x_model(S_t; k) + off_k − x_book(t)]

At t = k·1 s this is stage 3's number exactly (checked on every day, `fair_grid_reproduced`).
Why (D-036, lever 1a): the fair value is ~92 % book, and the stage-7 diagnosis found the
strategy quoting off a book state up to 1 s old while the touch had moved; the sweep that
fills us is often the one the frozen book part has not seen yet. `fair_mode="sec"` keeps
the old stream (book held per second, Binance ticks only) for A/B runs on the same engine.
"""
from __future__ import annotations

import glob
import json
import logging
import math
from pathlib import Path

import numpy as np
import polars as pl

from ..fair.inputs import logit
from ..fair.models import SETTLE_AVG_S, norm_cdf
from ..markouts.fills import NS, data_root, fee_table, market_index
from ..queue.events import PK, TICK, load_day

log = logging.getLogger(__name__)

FAMS = ("btc_15m", "fed_dec_2026")
P_CLIP = 1e-3
NORMAL_FEED_LAG_MS = 12.0  # D-031: Polymarket feed p50 on the VPS


# ------------------------------------------------------------------------------------------------
# fair value: whole seconds (walk-forward, with the model's inputs kept) and ticks
# ------------------------------------------------------------------------------------------------
KEEP = ["date", "family", "condition_id", "asset_id", "sec", "t_end_s", "mid", "x_fair", "w_x_fair", "x_model", "x_anch", "x_stoikov",
        "S", "K", "basis0", "sigma_s", "tau", "A_known"]
TOUCH = ("bid", "ask", "bid_sz", "ask_sz")


def fair_seconds(dates: list[str], fair_root: Path, spot_root: Path, out: Path, force: bool = False) -> dict[str, Path]:
    """Per day: stage 3's walk-forward fair value per (token, second), plus what the event stream
    needs (the anchor, the Stoikov book value, the model inputs) and the day's Stoikov table.
    Same fold rule as markouts.fair_oos (day d uses parameters fitted on the days before it; the
    first day uses its own fit), so the grid values equal fair_oos exactly.
    Why on `walk_forward` (D-035, D-038): the first version stacked every day's input grid in
    memory and refitted on the stack per fold (5.2 GB at 5 days, growing by a day each day);
    the helper keeps one day plus ~85 MB of fit rows per past day."""
    from ..fair.walk import walk_forward

    paths = {d: out / d / "fair_sec.parquet" for d in dates}
    st_paths = {d: out / d / "stoikov.parquet" for d in dates}
    if not force and all(p.exists() for p in paths.values()) and all(p.exists() for p in st_paths.values()):
        return paths
    fit_tau = json.loads((fair_root / "fair_params.json").read_text()).get("fit_tau", 30)

    def on_day(day) -> None:
        T = day.fair.filter(pl.col("family").is_in(list(FAMS)))
        T = T.with_columns(pl.col("w_x_fair").fill_null(0.0))
        T = T.select([c for c in KEEP if c in T.columns]).sort("asset_id", "sec")
        # variance per second of the fair PRICE over the last 60 s (causal): the σ_p² of the
        # reservation price r = m − q γ σ_p² (T − t)
        p = 1 / (1 + (-pl.col("x_fair")).exp())
        T = T.with_columns(p.alias("p_fair")).with_columns(
            pl.col("p_fair").diff().over("asset_id").pow(2).rolling_mean(60, min_samples=20).over("asset_id").alias("v2"))
        paths[day.date].parent.mkdir(parents=True, exist_ok=True)
        T.write_parquet(paths[day.date])
        day.params["stoikov"].write_parquet(st_paths[day.date])
        log.info("fair_sec %s (params from %s): %d rows", day.date, day.train, T.height)

    walk_forward(dates, fair_root, spot_root, fit_tau, on_day, first_day="self")
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


def _model_x(j: pl.DataFrame, S: str) -> np.ndarray:
    """log-odds of the BTC digital at the rows' spot price column `S`, every other input as at the
    row's grid second (the tick update of the module docstring)."""
    tau = j["tau"].to_numpy().astype(float)
    S_t = j[S].to_numpy().astype(float)
    A = j["A_known"].to_numpy().astype(float)
    last = tau <= SETTLE_AVG_S
    ea = np.where(last & np.isfinite(A), ((SETTLE_AVG_S - tau) * A + tau * S_t) / SETTLE_AVG_S, S_t)
    tau_eff = np.where(tau > SETTLE_AVG_S, tau - 40.0, tau ** 3 / (3 * SETTLE_AVG_S ** 2))
    d = (np.log(ea) - j["basis0"].to_numpy() - np.log(j["K"].to_numpy())) / (j["sigma_s"].to_numpy() * np.sqrt(np.maximum(tau_eff, 1e-9)))
    return _phi_x(d)


def _btc_rows(fs: pl.DataFrame) -> pl.DataFrame:
    return fs.filter((pl.col("family") == "btc_15m") & (pl.col("w_x_fair") > 0) & pl.col("x_model").is_finite()
                     & pl.col("sigma_s").is_not_null() & pl.col("basis0").is_not_null())


def book_events(tob: pl.DataFrame, assets: list[str]) -> pl.DataFrame:
    """The instants a token's touch changed (price or size on either side); the first row of a
    token counts. Why not every update: a day has ~28 M valid BTC 15m updates and ~5 M touch
    changes; an update that changes nothing at the touch cannot move the book value."""
    tb = tob.filter(pl.col("yes").is_in(assets)).select("yes", "t", *TOUCH).sort("yes", "t")
    ch = pl.any_horizontal([(pl.col(c) != pl.col(c).shift(1).over("yes")).fill_null(True) for c in TOUCH])
    return tb.filter(ch)


def fair_ticks(fs: pl.DataFrame, spot: pl.DataFrame, tob: pl.DataFrame | None = None, stoikov: pl.DataFrame | None = None,
               mode: str = "event", btc_lead_ms: float = 0.0, info: dict | None = None) -> pl.DataFrame:
    """(yes, t, p, v2): the fair price at every whole second and, in between, at every event that
    moves it (BTC 15m only; other families change once a second).
        mode "event"   book touch changes and Binance mid changes (D-038; needs tob and stoikov)
        mode "sec"     Binance mid changes only, the book part held per second (the old stream)
    btc_lead_ms > 0: oracle — every Binance tick is known that much EARLIER (its time shifted back;
    the book stays at its true time), the clean ceiling for a faster or better BTC signal.
    `info`: filled with the whole-second reproduction error (event mode) and tick counts."""
    sec_rows = fs.select(pl.col("asset_id").alias("yes"), (pl.col("sec") * NS).alias("t"), "p_fair", "v2")
    if btc_lead_ms and spot.height:
        spot = spot.with_columns(pl.col("t") - np.int64(btc_lead_ms * 1e6))
    if mode == "event" and tob is not None and stoikov is not None:
        ticks = _event_ticks(fs, spot, tob, stoikov, info)
    else:
        ticks = _spot_ticks_only(fs, spot)
    out = pl.concat([sec_rows, ticks]).rename({"p_fair": "p"}).drop_nulls("p").sort("yes", "t")
    # Why drop a tick that leaves the price unchanged: it would only cost the engine a decision
    unchanged = (pl.col("t") % NS != 0) & (pl.col("p") == pl.col("p").shift(1).over("yes"))
    out = out.filter(~unchanged.fill_null(False))
    if info is not None:
        info["ticks_between_seconds"] = out.filter(pl.col("t") % NS != 0).height
    return out


def _spot_ticks_only(fs: pl.DataFrame, spot: pl.DataFrame) -> pl.DataFrame:
    b = _btc_rows(fs)
    if not b.height or not spot.height:
        return fs.head(0).select(pl.col("asset_id").alias("yes"), pl.col("sec").alias("t"), "p_fair", "v2")
    sp = spot.with_columns((pl.col("t") // NS).alias("sec")).filter(pl.col("t") % NS != 0)
    j = sp.join(b, on="sec", how="inner")
    x = j["x_fair"].to_numpy() + j["w_x_fair"].to_numpy() * (_model_x(j, "S_t") - j["x_model"].to_numpy())
    return pl.DataFrame({"yes": j["asset_id"], "t": j["t"], "p_fair": 1 / (1 + np.exp(-x)), "v2": j["v2"]})


def _event_ticks(fs: pl.DataFrame, spot: pl.DataFrame, tob: pl.DataFrame, stoikov: pl.DataFrame, info: dict | None) -> pl.DataFrame:
    cols = ["sec", "x_fair", "p_fair", "w_x_fair", "x_model", "x_anch", "x_stoikov", "K", "basis0", "sigma_s", "tau", "A_known", "v2", "S"]
    B = (fs.filter(pl.col("family") == "btc_15m").select(pl.col("asset_id").alias("yes"), *cols)
         .with_columns(pl.col("w_x_fair").fill_null(0.0).alias("w"), (pl.col("x_anch") - pl.col("x_model")).fill_null(0.0).alias("off")))
    if not B.height:
        return B.head(0).select("yes", pl.col("sec").alias("t"), "p_fair", "v2")
    assets = B["yes"].unique().to_list()
    tb = book_events(tob, assets).with_columns(pl.lit(0, pl.Int8).alias("src"))
    # Why every second of the token's span, not only its grid seconds: a second without a grid row
    # (one-sided book) still has Binance ticks, and the next grid second's S is the latest of them
    span = (B.group_by("yes").agg(pl.col("sec").min().alias("s0"), pl.col("sec").max().alias("s1"))
            .select("yes", pl.int_ranges("s0", pl.col("s1") + 1).alias("sec")).explode("sec"))
    sp = (spot.with_columns((pl.col("t") // NS).alias("sec")).join(span, on="sec", how="inner")
          .select("yes", "t", "S_t", pl.col("t").alias("t_sp")).with_columns(pl.lit(1, pl.Int8).alias("src")))
    # check rows: the stream evaluated at the whole seconds must give the grid's own value
    wk = B.select("yes", (pl.col("sec") * NS).alias("t")).with_columns(pl.lit(2, pl.Int8).alias("src"))
    # Why forward fill over the token's whole day (book rows before the first grid second too):
    # the state at a tick is the last touch and the last Binance mid at or before it, whenever they
    # arrived; a Binance row before the token's first spot row falls back to the grid's S(k) below
    ev = (pl.concat([tb, sp, wk], how="diagonal_relaxed").sort("yes", "t", "src")
          .with_columns([pl.col(c).forward_fill().over("yes") for c in (*TOUCH, "S_t", "t_sp")])
          .with_columns((pl.col("t") // NS).alias("sec"))
          .join(B, on=["yes", "sec"], how="inner")
          .drop_nulls(["bid", "ask"]))
    I = pl.col("bid_sz") / (pl.col("bid_sz") + pl.col("ask_sz"))
    spread = pl.col("ask") - pl.col("bid")
    ev = ev.with_columns(
        logit((pl.col("bid") + pl.col("ask")) / 2).alias("x_mid"),
        (I * 10).floor().clip(0, 9).cast(pl.Int8).alias("imb_b"),
        pl.when(spread < 0.0095).then(0).otherwise((spread * 100).round().clip(1, 3)).cast(pl.Int8).alias("spr_b"),
        # Why the 5 s rule: the grid (fair.inputs.spot_grid) falls back to the history trade tape when
        # the last live quote is older than 5 s (a Binance feed gap); the stream does the same, by
        # taking the grid second's S, so the two agree at every whole second
        pl.when((pl.col("t") - pl.col("t_sp")) <= 5 * NS).then(pl.col("S_t")).otherwise(pl.col("S")).alias("S_t"))
    st = stoikov.filter(pl.col("family") == "btc_15m").select(pl.col("spr_b").cast(pl.Int8), pl.col("imb_b").cast(pl.Int8), "adj")
    ev = ev.join(st, on=["spr_b", "imb_b"], how="left").with_columns((pl.col("x_mid") + pl.col("adj").fill_null(0.0)).alias("x_book"))
    ok = (ev["w"] > 0).to_numpy() & np.isfinite(ev["x_model"].fill_null(float("nan")).to_numpy()) \
        & ev["sigma_s"].is_not_null().to_numpy() & ev["basis0"].is_not_null().to_numpy() & ev["S_t"].is_not_null().to_numpy()
    xm = np.full(ev.height, np.nan)
    if ok.any():
        xm[ok] = _model_x(ev.filter(pl.Series(ok)), "S_t")
    xb = ev["x_book"].to_numpy()
    w = ev["w"].to_numpy()
    x = np.where(ok, xb + w * (xm + ev["off"].to_numpy() - xb), xb)
    ev = ev.with_columns(pl.Series("p", 1 / (1 + np.exp(-x))))
    if info is not None:
        chk = ev.filter(pl.col("src") == 2)
        info["fair_grid_reproduced_max_abs_err"] = float((chk["p"] - chk["p_fair"]).abs().max()) if chk.height else None
        info["fair_grid_reproduced_rows"] = chk.height
        info["book_ticks"] = int((ev["src"] == 0).sum())
        info["spot_ticks"] = int((ev["src"] == 1).sum())
    # Why keep the last row per instant: a book row and a Binance row at the same t are one
    # instant; the later one in (t, src) order carries both states
    return (ev.filter(pl.col("src") < 2).unique(["yes", "t"], keep="last").filter(pl.col("t") % NS != 0)
            .select("yes", "t", pl.col("p").alias("p_fair"), "v2"))


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
def load_sim_day(date: str, fair_root: Path, data_dirs: list[Path], spot_root: Path, fair_sec_path: Path, idx: pl.DataFrame,
                 fair_mode: str = "event", btc_lead_ms: float = 0.0) -> dict:
    day_dir = fair_root / date
    root = data_root(day_dir, data_dirs, date)
    ix = idx.filter(pl.col("family").is_in(list(FAMS)))
    D = load_day(date, day_dir, root, ix)
    fs = pl.read_parquet(fair_sec_path)
    st_path = fair_sec_path.parent / "stoikov.parquet"
    st = pl.read_parquet(st_path) if st_path.exists() else None
    info: dict = {"fair_mode": fair_mode, "btc_lead_ms": btc_lead_ms}
    ticks = fair_ticks(fs, _spot_ticks(spot_root, date), D["tob"], st, mode=fair_mode, btc_lead_ms=btc_lead_ms, info=info)
    # Why drop mid / micro of the book stream here: the engine and the diagnosis read only t, the
    # touch (prices and sizes); the two columns are 16 B on ~80 M rows a day (~1.3 GB of the day's
    # resident memory) and nothing downstream reads them. bid / ask stay for `refair` (oracles).
    tob = D["tob"].drop([c for c in ("mid", "micro") if c in D["tob"].columns])
    seg = segments(ix, D["windows"], date)
    lag = feed_lag_extra(root, date, ix["asset_id"].to_list())
    return {"levels": D["levels"], "trades": D["trades"], "tob": tob, "fair": ticks, "fair_sec": fs, "segments": seg,
            "lag": lag, "checks": D["checks"], "root": root, "fair_info": info, "stoikov": st, "spot_root": spot_root, "date": date}


def refair(S: dict, fair_mode: str = "event", btc_lead_ms: float = 0.0) -> pl.DataFrame:
    """The day's fair stream rebuilt with another mode or BTC lead (oracles), from what
    `load_sim_day` kept; attribution keeps using the true stream (S["fair"])."""
    return fair_ticks(S["fair_sec"], _spot_ticks(S["spot_root"], S["date"]), S["tob"], S["stoikov"], mode=fair_mode, btc_lead_ms=btc_lead_ms)


def check_inputs(S: dict, fair_oos_path: Path | None = None) -> dict:
    """S0 checks. (1) our whole-second fair value is stage 3/4's walk-forward series (fair_oos)
    exactly; (2) the event stream evaluated at the whole seconds reproduces the grid (event mode:
    the book and Binance state carried through the stream equal the grid's), or, in "sec" mode,
    the tick formula at the second's own spot price returns the second's value; (3) coverage:
    share of BTC 15m segment seconds with a fair value."""
    fs, ft = S["fair_sec"], S["fair"]
    err = None
    if fair_oos_path is not None and fair_oos_path.exists():
        oos = pl.read_parquet(fair_oos_path).select("asset_id", "sec", pl.col("p_fair").alias("p_oos"))
        j = fs.join(oos, on=["asset_id", "sec"], how="inner")
        err = float((j["p_fair"] - j["p_oos"]).abs().max()) if j.height else None
    info = S.get("fair_info", {})
    self_err = info.get("fair_grid_reproduced_max_abs_err")
    if self_err is None:
        b = _btc_rows(fs).filter(pl.col("S").is_not_null())
        if b.height:
            sp = b.select((pl.col("sec") * NS + 1).alias("t"), pl.col("S").alias("S_t")).unique("t").sort("t")
            tk = fair_ticks(b, sp, mode="sec").filter(pl.col("t") % NS == 1).join(
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
    return {"fair_mode": info.get("fair_mode"), "fair_grid_max_abs_err": err, "fair_grid_reproduced_max_abs_err": self_err,
            "fair_ticks_between_seconds": sub.height, "book_ticks": info.get("book_ticks"), "spot_ticks": info.get("spot_ticks"),
            "fair_tick_move_p99_c": round(float(jumps.quantile(0.99)) * 100, 3) if jumps.len() else None,
            "btc15m_segment_seconds_with_fair": covered, "segments": S["segments"].height,
            "segments_by_family": dict(S["segments"].group_by("family").len().iter_rows())}


def rebate_k(fees: pl.DataFrame) -> dict[str, float]:
    """family → r·f: the maker rebate per share is r·f·p(1−p) USD (D-029)."""
    return {r["family"]: float(r["fee_rate"]) * float(r["fee_rebate_rate"]) for r in fees.iter_rows(named=True)}


__all__ = ["fair_seconds", "fair_ticks", "book_events", "refair", "feed_lag_extra", "segments", "load_sim_day", "check_inputs", "rebate_k",
           "market_index", "fee_table", "PK", "TICK", "NS"]

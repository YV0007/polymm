"""A0: the fill table. One row per public trade, rewritten as the MAKER's fill on the YES token.

    trades (WS last_trade_price) ─┐
    market index (Gamma ref)  ────┼─► YES-equivalent side s and price P
    events_labeled (D-020)    ────┤   pre-trade time t_pre = before the delta the trade consumed
    replay tob (event level)  ────┼─► book just before the fill: bid, ask, sizes, mid, microprice
    strict clean windows      ────┼─► clean flag + the window end (horizons may not cross it)
    fair.parquet (1 s grid)   ────┘   fair value, Stoikov book, BTC S and σ at the last second before

Every public trade is somebody's maker fill: flipping the taker side gives the fill an average
resting order at that price got. That is what stage 4 measures, with no model of our own quoting.

Sign convention (the one place it is set; everything downstream trusts it):
    s = +1  maker BOUGHT YES   (taker sold YES, or taker bought NO)
    s = -1  maker SOLD YES     (taker bought YES, or taker sold NO)
    P = trade price on the YES token = price if the trade printed on YES, else 1 - price
Why YES-equivalent: 94 % of WS trades are BUY, because selling YES is done by buying NO. A raw
BUY therefore means opposite things on the two tokens; mixing them flips half the markouts.
Verified on 11-07: each match prints on exactly one token (no double counting), `side` is the
taker's side, and `fee_rate_bps` is always 0 (the fee comes from market metadata instead).
"""
from __future__ import annotations

import glob
import logging
import re
from pathlib import Path

import numpy as np
import polars as pl

log = logging.getLogger(__name__)

NS = 1_000_000_000
# Why 50 ms: when a trade has no linked delta, the consuming delta still usually arrives BEFORE
# the trade message (median -18 ms on 11-07). At t-0 only 79 % of trades print at the touch of
# the "pre-trade" book, at t-50 ms 93 %; further back the book is simply stale (t-1 s: 76 %).
FALLBACK_PRE_MS = 50
FLOW_WINDOW_S = 5


def market_index(ref: pl.DataFrame) -> pl.DataFrame:
    """asset_id → (yes token, outcome index 0/1, market facts). Both tokens of every market."""
    keep = ["family", "event_slug", "condition_id", "t_start_s", "t_end_s", "outcome"]
    yes = ref.filter(pl.col("yes_asset").is_not_null())
    return pl.concat([
        yes.select(pl.col("yes_asset").alias("asset_id"), pl.col("yes_asset").alias("yes"), pl.lit(0).alias("oi"), *keep),
        yes.select(pl.col("no_asset").alias("asset_id"), pl.col("yes_asset").alias("yes"), pl.lit(1).alias("oi"), *keep),
    ]).unique(subset=["asset_id"])


def fee_table(data_dirs: list[Path]) -> pl.DataFrame:
    """Per family: taker fee rate and maker rebate share, from our own Gamma snapshots (D1).
    Why not the trade's fee_rate_bps: it is 0 on every recorded trade, also in fee-enabled markets."""
    files = [f for d in data_dirs for f in glob.glob(str(d / "reference" / "markets" / "**" / "*.parquet"), recursive=True)]
    m = pl.concat([pl.read_parquet(f, columns=["fetched_at_ns", "family", "fees_enabled", "fee_rate", "fee_rebate_rate"]) for f in files],
                  how="diagonal_relaxed")
    return (m.sort("fetched_at_ns").group_by("family").agg(pl.col("fees_enabled").last(), pl.col("fee_rate").last(),
                                                           pl.col("fee_rebate_rate").last())
            .with_columns(pl.when(pl.col("fees_enabled")).then(pl.col("fee_rate")).otherwise(0.0).fill_null(0.0).alias("fee_rate"),
                          pl.col("fee_rebate_rate").fill_null(0.0))
            .select("family", "fee_rate", "fee_rebate_rate"))


def data_root(day_dir: Path, candidates: list[Path], date: str) -> Path:
    """The event store the day's replay was built from (quality.md says), so trades and books
    come from the same recorder. Fallback: the first candidate holding the day's trades."""
    q = day_dir / "quality.md"
    if q.exists():
        m = re.search(r"Data root `([^`]+)`", q.read_text()[:2000])
        if m and (Path(m.group(1)) / "events" / "trades" / f"dt={date}").exists():
            return Path(m.group(1))
    for c in candidates:
        if (c / "events" / "trades" / f"dt={date}").exists():
            return c
    raise FileNotFoundError(f"no trades for {date} in {candidates}")


def strict_windows(day_dir: Path) -> pl.DataFrame:
    sw = pl.read_parquet(day_dir / "clean_windows_strict.parquet")
    end = "last_verified" if "last_verified" in sw.columns else "t_end"
    return (sw.select(pl.col("asset_id").alias("yes"), "t_start", pl.coalesce(pl.col(end), pl.col("t_start")).alias("t_last"))
            .filter(pl.col("t_last") > pl.col("t_start")).sort("t_start"))


def load_tob(day_dir: Path, yes_assets: list[str]) -> pl.DataFrame:
    """Event-level top of book of the YES tokens (valid rows only)."""
    return (pl.scan_parquet(str(day_dir / "replay" / "derived" / "tob" / "**" / "*.parquet"))
            .filter(pl.col("asset_id").is_in(yes_assets) & pl.col("valid") & (pl.col("event_type") != "reconnect"))
            .select(pl.col("ts_recv_ns").alias("t"), pl.col("asset_id").alias("yes"), pl.col("best_bid").alias("bid"),
                    pl.col("best_ask").alias("ask"), pl.col("bid_size").alias("bid_sz"), pl.col("ask_size").alias("ask_sz"),
                    "mid", pl.col("microprice").alias("micro"))
            .collect().sort("t"))


def linked_deltas(day_dir: Path, idx: pl.DataFrame) -> pl.DataFrame:
    """(trade receive time, YES token) → receive time of the earliest book decrease it consumed (D-020)."""
    el = (pl.scan_parquet(str(day_dir / "replay" / "derived" / "events_labeled" / "**" / "*.parquet"))
          .filter(pl.col("label") == "fill").select("ts_recv_ns", "asset_id", "trade_ts_recv_ns").collect())
    return (el.join(idx.select("asset_id", "yes"), on="asset_id", how="inner")
            .group_by("trade_ts_recv_ns", "yes").agg(pl.col("ts_recv_ns").min().alias("t_delta")))


def to_yes_equivalent(tr: pl.DataFrame) -> pl.DataFrame:
    """Adds taker_dir (+1 taker bought YES), s (maker side, +1 maker bought YES) and P (YES price).
    Needs `oi` (0 = traded token is YES, 1 = NO), `side` (taker side on the traded token), `price`."""
    taker_buys_yes = ((pl.col("oi") == 0) & (pl.col("side") == "BUY")) | ((pl.col("oi") == 1) & (pl.col("side") == "SELL"))
    return tr.with_columns(
        pl.when(taker_buys_yes).then(1).otherwise(-1).alias("taker_dir"),
        pl.when(taker_buys_yes).then(-1).otherwise(1).alias("s"),
        pl.when(pl.col("oi") == 0).then(pl.col("price")).otherwise(1 - pl.col("price")).alias("P"),
    )


def _signed_prior_flow(f: pl.DataFrame, window_s: int) -> np.ndarray:
    """Per fill: taker volume in YES terms over (t - window, t_pre), signed so that + = flow in the
    direction that hurts THIS maker (takers buying YES when the maker sold). Strictly before the
    fill, so it is known at quote time."""
    out = np.zeros(f.height)
    tdir = f["taker_dir"].to_numpy()  # +1 taker bought YES
    size = f["size"].to_numpy()
    t = f["t_recv"].to_numpy()
    tp = f["t_pre"].to_numpy()
    s = f["s"].to_numpy()
    for idxs in f.with_row_index("i").group_by("yes").agg("i")["i"].to_list():
        ii = np.asarray(idxs)
        ii = ii[np.argsort(t[ii], kind="stable")]
        tt = t[ii]
        cum = np.concatenate([[0.0], np.cumsum(tdir[ii] * size[ii])])
        lo = np.searchsorted(tt, tt - window_s * NS, side="left")
        hi = np.searchsorted(tt, tp[ii], side="left")  # trades received strictly before t_pre
        hi = np.maximum(hi, lo)
        out[ii] = -s[ii] * (cum[hi] - cum[lo])
    return out


def build_fills(date: str, day_dir: Path, root: Path, idx: pl.DataFrame, fees: pl.DataFrame, tob: pl.DataFrame,
                fair: pl.DataFrame | None) -> tuple[pl.DataFrame, dict]:
    """The A0 table for one UTC day, plus the checks that say whether it can be trusted."""
    tr = pl.read_parquet(str(root / "events" / "trades" / f"dt={date}" / "**" / "*.parquet"))
    n_raw = tr.height
    tr = tr.join(idx, on="asset_id", how="inner")
    chk: dict = {"trades_raw": n_raw, "trades_unmapped": n_raw - tr.height, "data_root": str(root)}
    f = to_yes_equivalent(tr).select(
        pl.lit(date).alias("date"), "family", "event_slug", "condition_id", "yes", "oi", "t_start_s", "t_end_s", "outcome",
        pl.col("ts_recv_ns").alias("t_recv"), pl.col("ts_exch_ms").alias("t_exch_ms"), pl.col("side").alias("side_raw"),
        pl.col("price").alias("price_raw"), "size", "taker_dir", "s", "P")
    # ---- pre-trade instant -----------------------------------------------------------------
    lk = linked_deltas(day_dir, idx)
    f = f.join(lk, left_on=["t_recv", "yes"], right_on=["trade_ts_recv_ns", "yes"], how="left")
    f = f.with_columns(
        pl.col("t_delta").is_not_null().alias("linked"),
        # Why min(trade, delta) - 1 ns: the book must not yet contain this trade's own decrease,
        # whichever message arrived first. Unlinked trades fall back to 50 ms before the trade.
        pl.when(pl.col("t_delta").is_not_null()).then(pl.min_horizontal("t_recv", "t_delta") - 1)
          .otherwise(pl.col("t_recv") - FALLBACK_PRE_MS * 1_000_000).alias("t_pre"),
    ).drop("t_delta")
    # Why (token, exchange ms, direction) as the order key: WS tx hashes are unique per message,
    # so they cannot group a sweep; messages of one taker order share the exchange millisecond.
    # Why t_pre = the order's earliest: for the 2nd level of a sweep the book at its own t_pre is
    # already half swept (audit: 66 % of 2nd-level fills looked "at the touch"). The state a
    # resting quote was in is the one before the whole order arrived.
    f = (f.with_columns(pl.struct("yes", "t_exch_ms", "taker_dir").hash().alias("order_key"))
         .with_columns(pl.col("t_pre").min().over("order_key")))
    # ---- clean window: the fill and its pre-trade book inside one strict window -----------
    sw = strict_windows(day_dir)
    f = (f.sort("t_pre").join_asof(sw, left_on="t_pre", right_on="t_start", by="yes", strategy="backward")
         .with_columns(((pl.col("t_start").is_not_null()) & (pl.col("t_recv") <= pl.col("t_last"))).alias("clean")))
    # ---- book just before the fill ----------------------------------------------------------
    f = f.join_asof(tob.rename({"t": "t_book"}), left_on="t_pre", right_on="t_book", by="yes", strategy="backward")
    f = f.with_columns(pl.col("clean") & pl.col("bid").is_not_null() & pl.col("ask").is_not_null() & (pl.col("ask") > pl.col("bid")))
    eps = 1e-9
    f = f.with_columns(
        # consistent: a maker sale prints at or above the prior ask, a maker purchase at or below the prior bid
        pl.when(pl.col("s") == -1).then(pl.col("P") >= pl.col("ask") - eps).otherwise(pl.col("P") <= pl.col("bid") + eps).alias("sign_ok"),
        ((pl.col("P") > pl.col("bid") + eps) & (pl.col("P") < pl.col("ask") - eps)).alias("inside"),
        # how far through the touch the fill printed, in cents (0 = at the touch)
        (100 * pl.when(pl.col("s") == -1).then(pl.col("P") - pl.col("ask")).otherwise(pl.col("bid") - pl.col("P"))).round(1).alias("level_c"),
        (100 * (pl.col("ask") - pl.col("bid"))).round(1).alias("spread_c"),
        pl.when(pl.col("s") == -1).then(pl.col("ask_sz")).otherwise(pl.col("bid_sz")).alias("q_touch"),
        # imbalance pointing AGAINST the maker: for a maker sale, a heavy bid (up-pressure) is bad
        pl.when(pl.col("s") == -1).then(pl.col("bid_sz") / (pl.col("bid_sz") + pl.col("ask_sz")))
          .otherwise(pl.col("ask_sz") / (pl.col("bid_sz") + pl.col("ask_sz"))).alias("imb_adv"),
    ).with_columns((pl.col("size") / pl.col("q_touch")).alias("size_vs_touch"))
    # ---- taker order and prior flow ----------------------------------------------------------
    # flow counts trades received strictly before t_pre, and every message of this order arrives
    # after the order's t_pre, so the fill's own order never counts as "prior" flow.
    f = f.with_columns(pl.len().over("order_key").alias("order_msgs"), pl.col("size").sum().over("order_key").alias("order_size"),
                       pl.col("P").n_unique().over("order_key").alias("order_levels"))
    f = f.sort("t_recv").with_columns(pl.Series("flow5_adv", _signed_prior_flow(f.sort("t_recv"), FLOW_WINDOW_S)))
    # ---- fees -------------------------------------------------------------------------------
    f = f.join(fees, on="family", how="left").with_columns(pl.col("fee_rate").fill_null(0.0), pl.col("fee_rebate_rate").fill_null(0.0))
    f = f.with_columns((pl.col("fee_rate") * pl.col("P") * (1 - pl.col("P"))).alias("taker_fee_ps"))
    f = f.with_columns((pl.col("fee_rebate_rate") * pl.col("taker_fee_ps")).alias("rebate_ps"))
    # ---- time features ----------------------------------------------------------------------
    f = f.with_columns(
        (pl.col("t_end_s") - pl.col("t_recv") / NS).alias("tte_s"),
        pl.when(pl.col("family") == "btc_15m").then(((pl.col("t_recv") // NS) - pl.col("t_start_s")) // 60).otherwise(None).alias("minute"),
        ((pl.col("t_recv") // NS) % 86400 // 3600).alias("hour"),
        pl.when(pl.col("family") == "btc_15m").then(pl.col("condition_id"))
          .otherwise(pl.col("condition_id") + "|" + (pl.col("t_recv") // NS // 3600).cast(pl.String)).alias("cluster"),
    )
    # ---- fair value and BTC state at the last whole second before the fill ------------------
    if fair is not None and fair.height:
        fr = fair.select(pl.col("asset_id").alias("yes"), "sec", pl.col("p_fair").alias("fair0"), pl.col("p_book").alias("book0"),
                         pl.col("S").alias("S0"), "sigma_s")
        f = f.with_columns((pl.col("t_pre") // NS).alias("sec_pre"))
        f = f.join(fr.rename({"sec": "sec_pre"}), on=["yes", "sec_pre"], how="left")
        f = f.join(fr.select("yes", (pl.col("sec") + FLOW_WINDOW_S).alias("sec_pre"), pl.col("S0").alias("S_m5")), on=["yes", "sec_pre"], how="left")
        # BTC 5 s log return in σ units, signed against the maker (+ = BTC moved the way that hurts it).
        # Why every family: a YES token in both BTC families rises with BTC; Fed rows have no S move.
        f = f.with_columns(
            pl.when(pl.col("family") != "fed_dec_2026")
              .then(-pl.col("s") * (pl.col("S0") / pl.col("S_m5")).log() / (pl.col("sigma_s") * np.sqrt(FLOW_WINDOW_S)))
              .otherwise(0.0).alias("btc5_adv"),
        ).drop("S_m5")
    f = f.sort("t_recv")
    # ---- checks -----------------------------------------------------------------------------
    c = f.filter(pl.col("clean"))
    chk.update({
        "fills": f.height, "clean": c.height, "clean_share": round(c.height / max(f.height, 1), 4),
        "linked_share": round(c["linked"].mean() or 0, 4),
        "sign_ok": round(c["sign_ok"].mean() or 0, 4),
        "sign_ok_linked": round(c.filter(pl.col("linked"))["sign_ok"].mean() or 0, 4),
        "inside_spread": round(c["inside"].mean() or 0, 4),
        "maker_buy_share": round((c["s"] == 1).mean() or 0, 4),
        "fair_missing": round(c["fair0"].is_null().mean(), 4) if "fair0" in c.columns else None,
        "by_family": {r["family"]: r["n"] for r in c.group_by("family").agg(pl.len().alias("n")).to_dicts()},
        "usd_notional": round(float((c["P"] * c["size"]).sum()), 0),
    })
    return f, chk

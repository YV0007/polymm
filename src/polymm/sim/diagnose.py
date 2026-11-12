"""Strategy diagnosis (D-034): where the simulated BTC 15m strategy loses and earns, why, and how
much each lever could be worth.

Four tools, each answering one question:

1. Fill anatomy. Every fill of the walk-forward-selected rung-1 and rung-3 settings, labelled by
   mechanism (mutually exclusive, in this order):
       A  cancel in flight     we had already decided to pull the order; the taker beat our cancel
       B  stale, not pulled    fair value had crossed our price before the taker came, and we had
                               not reacted yet (signal or decision too slow)
       C  swept through        a taker traded THROUGH our price (a sweep), fair value not crossed
       D  at our level         an ordinary fill at our price from the front of the queue
   plus timing (order age, minute of the window), state (spread, recent fair-value move) and the
   markout curve s·(m(t+τ) − P), which splits ② into the part that happens within 1 s (a race)
   and the part that comes later (a prediction problem).

2. Oracles. Rerun the same setting with one handicap removed. The gain is an UPPER bound on what
   that lever is worth (a perfect version of it), so the ranking of gains ranks the levers:
       fair_lead      the fair value is known L ms earlier (L = 127: Binance with no feed lag;
                      1 s: predicting BTC 1 s ahead; 5 s; 60 s: a near-perfect fair value)
       lat0 / cx0     no order latency / instant cancels
       front          our order is first in line at its price
       colo           lat0 + fair 127 ms earlier (colocated with Binance and the venue)
       all            lat0 + fair 1 s earlier + front of the queue

3. The race. After every fair-value jump (≥ 1 ¢ within 0.5 s, as we receive it), how soon does
   the first taker trade in the jump's direction start? That interval is the time a resting
   quote has to get out; compare it with our order path.

4. Selection. Can the state at the moment we SEND an order tell good orders from bad? A ridge
   model of an order's net P&L on its send-time state, fitted on earlier days and applied to the
   next (walk-forward): the P&L of the orders it keeps vs all, and its AUC for "loses money".
   Dropping orders after the fact ignores how the path would change: an estimate, stated as such.
"""
from __future__ import annotations

import json
import logging
import math
from pathlib import Path

import numpy as np
import polars as pl

from ..markouts.fills import fee_table, market_index
from . import attrib as A
from . import day as Dy
from . import experiments as X
from .inputs import NS, PK, load_sim_day, rebate_k, refair

log = logging.getLogger(__name__)

RUNGS = (1, 3)
ORACLES = {
    "base": {},
    "fair_lead_127ms": {"fair_lead_ms": 127.0},
    "fair_lead_1s": {"fair_lead_ms": 1000.0},
    "fair_lead_5s": {"fair_lead_ms": 5000.0},
    "fair_lead_60s": {"fair_lead_ms": 60000.0},
    "lat0": {"lat_ms": 0.0},
    "cancel0": {"cx_lat_ms": 0.0},
    "front": {"queue_front": True},
    "colo": {"lat_ms": 0.0, "fair_lead_ms": 127.0},
    "all": {"lat_ms": 0.0, "fair_lead_ms": 1000.0, "queue_front": True},
    # D-038: only the BTC input is known earlier; the book stays at its true time. Why a separate
    # oracle: fair_lead shifts the whole stream, book-driven ticks included, so it also "knows"
    # the next second of order flow; btc_lead is the ceiling for a faster or better BTC signal alone
    "btc_lead_127ms": {"btc_lead_ms": 127.0},
    "btc_lead_250ms": {"btc_lead_ms": 250.0},
    "btc_lead_500ms": {"btc_lead_ms": 500.0},
    "btc_lead_1s": {"btc_lead_ms": 1000.0},
}
TAUS_MS = (0, 100, 250, 500, 1000, 2000, 5000, 10000, 30000, 60000)
# Exploratory (chosen after seeing minute 13–14 carry half of rung 1's loss, so NOT walk-forward):
# reduce-only in the last END_S seconds. Rung 1 gets it through rung 2 with γ = 0.
END_S = (120.0, 180.0)
MECH = {"A": "A cancel in flight", "B": "B stale, not pulled", "C": "C swept through", "D": "D at our level"}


def _asof(df: pl.DataFrame, fr: pl.DataFrame, tcol: str, out: str, by: str = "yes") -> pl.DataFrame:
    return (df.sort(tcol).join_asof(fr.select(by, pl.col("t").alias("_t"), pl.col("p").alias(out)).sort("_t"),
                                     left_on=tcol, right_on="_t", by=by, strategy="backward").drop("_t"))


def anatomy(a: pl.DataFrame, fair: pl.DataFrame, tob: pl.DataFrame) -> pl.DataFrame:
    """Per fill: mechanism A–D, timing and state features, the markout curve (¢ per share)."""
    if not a.height:
        return a
    f = a.with_columns((pl.col("t") - pl.col("t_arr_o")).alias("age_ns"))
    f = _asof(f, fair, "t_send_o", "m_send")
    f = f.with_columns((pl.col("t_pre") - NS).alias("_t1"))
    f = _asof(f, fair, "_t1", "m_pre_1s").drop("_t1")
    for ms in TAUS_MS:
        if ms == 0:
            continue
        f = f.with_columns((pl.col("t") + ms * 1_000_000).alias("_tt"))
        f = _asof(f, fair, "_tt", f"m_{ms}").drop("_tt")
        past = (pl.col("t") + ms * 1_000_000) >= pl.col("t_end_s") * NS
        f = f.with_columns(pl.when(past & ~pl.col("marked")).then(pl.col("V")).otherwise(pl.col(f"m_{ms}")).alias(f"m_{ms}"))
    bk = tob.select("t", "yes", (pl.col("ask_pk") - pl.col("bid_pk")).alias("spread_pk"), "bid_sz", "ask_sz").sort("t")
    f = f.sort("t_pre").join_asof(bk, left_on="t_pre", right_on="t", by="yes", strategy="backward", suffix="_bk")
    s = pl.col("s")
    h_pre = s * (pl.col("m") - pl.col("P"))
    f = f.with_columns(
        (100 * h_pre).alias("h_c"),
        (100 * s * (pl.col("m_send") - pl.col("P"))).alias("h_send_c"),
        (100 * s * (pl.col("m") - pl.col("m_send"))).alias("fair_move_since_send_c"),
        (100 * (pl.col("m") - pl.col("m_pre_1s")).abs()).alias("fair_move_1s_c"),
        ((pl.col("t") // NS - pl.col("t_start_s")) // 60).alias("minute"),
        (pl.col("spread_pk") / 10).alias("spread_c"),
        pl.when(pl.col("cx_pending")).then(pl.lit("A")).when(h_pre < 0).then(pl.lit("B"))
          .when(pl.col("through")).then(pl.lit("C")).otherwise(pl.lit("D")).alias("mech"),
    )
    q = pl.col("qty")
    cols = {f"M_{ms}ms_c": 100 * s * ((pl.col(f"m_{ms}") if ms else pl.col("m")) - pl.col("P")) for ms in TAUS_MS}
    f = f.with_columns(**cols).with_columns(
        (100 * s * (pl.col("m_1000") - pl.col("m"))).alias("adv_0_1s_c"),
        (100 * s * (pl.col("m_60000") - pl.col("m_1000"))).alias("adv_1_60s_c"),
        ((pl.col("pnl_hold") + pl.col("c4_rebate")) / q * 100).alias("net_c"))
    return f


def group_table(f: pl.DataFrame, by: str, total_loss: float | None = None) -> pl.DataFrame:
    """Per group: fills, share of shares, ¢ per share (① ② ③ ④ and net, ② split at 1 s), USD."""
    q = pl.col("qty")
    w = lambda c: (pl.col(c) * q).sum() / q.sum()
    g = f.group_by(by).agg(pl.len().alias("fills"), q.sum().alias("shares"),
                           (100 * pl.col("c1_spread").sum() / q.sum()).alias("c1_c"), (100 * pl.col("c2_adverse").sum() / q.sum()).alias("c2_c"),
                           (100 * pl.col("c3_inventory").sum() / q.sum()).alias("c3_c"), (100 * pl.col("c4_rebate").sum() / q.sum()).alias("c4_c"),
                           w("adv_0_1s_c").alias("adv_0_1s_c"), w("adv_1_60s_c").alias("adv_1_60s_c"),
                           (pl.col("pnl_hold") + pl.col("c4_rebate")).sum().alias("usd"))
    tot_sh = float(f["qty"].sum())
    tl = total_loss if total_loss is not None else float((f["pnl_hold"] + f["c4_rebate"]).sum())
    return g.with_columns((pl.col("shares") / tot_sh).alias("share_of_shares"), (100 * pl.col("usd") / pl.col("shares")).alias("net_c"),
                          (pl.col("usd") / tl).alias("share_of_pnl")).sort(by)


def bucket(f: pl.DataFrame) -> pl.DataFrame:
    age = pl.col("age_ns") / 1e6
    return f.with_columns(
        pl.when(age < 100).then(pl.lit("a <100 ms")).when(age < 1000).then(pl.lit("b 0.1–1 s")).when(age < 10000).then(pl.lit("c 1–10 s"))
          .otherwise(pl.lit("d >10 s")).alias("age_b"),
        pl.when(pl.col("minute") <= 4).then(pl.lit("00-04")).when(pl.col("minute") <= 9).then(pl.lit("05-09"))
          .when(pl.col("minute") <= 12).then(pl.lit("10-12")).otherwise(pl.lit("13-14")).alias("minute_b"),
        pl.when(pl.col("spread_c") <= 1).then(pl.lit("1c")).when(pl.col("spread_c") <= 2).then(pl.lit("2c"))
          .when(pl.col("spread_c") <= 5).then(pl.lit("3-5c")).otherwise(pl.lit(">5c")).alias("spread_b"),
        pl.when(pl.col("fair_move_1s_c") < 0.25).then(pl.lit("a <0.25c")).when(pl.col("fair_move_1s_c") < 1).then(pl.lit("b 0.25-1c"))
          .when(pl.col("fair_move_1s_c") < 3).then(pl.lit("c 1-3c")).otherwise(pl.lit("d >=3c")).alias("jump_b"),
        pl.when(pl.col("typ") == 0).then(pl.lit("improve")).when(pl.col("typ") == 1).then(pl.lit("touch"))
          .when(pl.col("typ") == 2).then(pl.lit("behind")).otherwise(pl.lit("deeper")).alias("type"),
        pl.when(pl.col("h_send_c") < 1).then(pl.lit("a <1c")).when(pl.col("h_send_c") < 2).then(pl.lit("b 1-2c"))
          .when(pl.col("h_send_c") < 3.5).then(pl.lit("c 2-3.5c")).otherwise(pl.lit("d >=3.5c")).alias("edge_at_send_b"),
    )


def signature(f: pl.DataFrame) -> pl.DataFrame:
    """Share-weighted markout s·(m(t+τ) − P) of our fills, ¢ per share, by τ; τ = 0 uses the fair
    value at the taker order's start (the spread we captured, ①)."""
    q = pl.col("qty")
    rows = []
    for ms in TAUS_MS:
        c = f"M_{ms}ms_c"
        x = f.filter(pl.col(c).is_not_null())
        rows.append({"tau_ms": ms, "markout_c": float((x[c] * x["qty"]).sum() / x["qty"].sum()) if x.height else None, "fills": x.height})
    return pl.DataFrame(rows)


def race(fair: pl.DataFrame, tr: pl.DataFrame, seg: pl.DataFrame, jump_c: float = 1.0, win_ms: int = 500, cap_ms: int = 5000) -> pl.DataFrame:
    """Per fair-value jump (≥ jump_c within win_ms, as received, BTC 15m live window): delay to the
    first taker trade in the jump's direction (taker buying YES after an up-jump), capped."""
    s15 = seg.filter(pl.col("family") == "btc_15m").select("yes", "t0", "t1")
    fr = fair.join(s15.select("yes").unique(), on="yes", how="semi").sort("yes", "t")
    fr = fr.with_columns((pl.col("t") - win_ms * 1_000_000).alias("_tb"))
    back = fr.select("yes", pl.col("t").alias("_t"), pl.col("p").alias("p_back")).sort("_t")
    j = fr.sort("_tb").join_asof(back, left_on="_tb", right_on="_t", by="yes", strategy="backward").drop("_t", "_tb")
    j = j.with_columns((100 * (pl.col("p") - pl.col("p_back"))).alias("dp_c")).filter(pl.col("dp_c").abs() >= jump_c)
    # one jump per episode: the first tick of each run of qualifying ticks (≥ 1 s apart)
    j = j.sort("yes", "t").with_columns((pl.col("t") - pl.col("t").shift(1).over("yes")).alias("_gap"))
    j = j.filter(pl.col("_gap").is_null() | (pl.col("_gap") > NS)).drop("_gap")
    j = j.join(s15, on="yes", how="inner").filter((pl.col("t") >= pl.col("t0")) & (pl.col("t") < pl.col("t1"))).drop("t0", "t1").unique(["yes", "t"])
    if not j.height:
        return pl.DataFrame()
    # taker direction: maker side s = −1 (an ask was lifted) after an up-jump
    j = j.with_columns(pl.when(pl.col("dp_c") > 0).then(-1).otherwise(1).cast(pl.Int8).alias("s_hit"))
    t = tr.select("yes", pl.col("t").alias("t_tr"), pl.col("s").alias("s_hit")).sort("t_tr")
    k = j.sort("t").join_asof(t, left_on="t", right_on="t_tr", by=["yes", "s_hit"], strategy="forward")
    # Why also look BACK: a taker who sees BTC before our feed does trades before the jump reaches
    # us. A same-direction taker trade in the 500 ms BEFORE we see the jump = we were pre-empted.
    k = k.sort("t").join_asof(t.rename({"t_tr": "t_prev"}), left_on="t", right_on="t_prev", by=["yes", "s_hit"], strategy="backward")
    # control: the same look-back for trades in the OPPOSITE direction (the base rate of any trade)
    t_opp = t.rename({"t_tr": "t_prev_opp"}).with_columns((-pl.col("s_hit")).cast(pl.Int8).alias("s_hit"))
    k = k.sort("t").join_asof(t_opp, left_on="t", right_on="t_prev_opp", by=["yes", "s_hit"], strategy="backward")
    k = k.with_columns((((pl.col("t") - pl.col("t_prev_opp")) / 1e6) <= 500).fill_null(False).alias("opposite_500ms"))
    return k.with_columns(((pl.col("t_tr") - pl.col("t")) / 1e6).alias("delay_ms"), ((pl.col("t") - pl.col("t_prev")) / 1e6).alias("lead_ms")).with_columns(
        pl.when(pl.col("delay_ms") <= cap_ms).then(pl.col("delay_ms")).otherwise(None).alias("delay_ms"),
        (pl.col("lead_ms") <= 500).fill_null(False).alias("preempted_500ms"))


def market_factors(S: dict, rc: pl.DataFrame) -> dict:
    """Properties of the market itself on one day (BTC 15m live windows), the ones to compare
    across markets: how wide and deep the book is, how much and how jumpily fair value moves, and
    how often takers trade ahead of the jump we see."""
    seg = S["segments"].filter(pl.col("family") == "btc_15m")
    secs = (seg.select("yes", pl.int_ranges(pl.col("t0") // NS + 1, pl.col("t1") // NS).alias("sec")).explode("sec").drop_nulls()
            .with_columns((pl.col("sec") * NS).alias("t")).sort("t"))
    tob = S["tob"].select("t", "yes", "bid_pk", "ask_pk", "bid_sz", "ask_sz").sort("t")
    g = secs.join_asof(tob, on="t", by="yes", strategy="backward").drop_nulls("bid_pk")
    g = g.filter(pl.col("bid_pk").is_between(50, 950) & pl.col("ask_pk").is_between(50, 950))
    spr = (g["ask_pk"] - g["bid_pk"]) / 10
    fs = S["fair_sec"].filter(pl.col("family") == "btc_15m").sort("asset_id", "sec")
    dp = fs.with_columns((100 * pl.col("p_fair").diff().over("asset_id")).alias("d"))["d"].drop_nulls()
    live_h = float(((seg["t1"] - seg["t0"]).sum()) / NS / 3600)
    out = {"live_hours": round(live_h, 2), "windows": int(seg["yes"].n_unique()),
           "spread_1tick_share": float((spr <= 1.0001).mean()), "spread_median_c": float(spr.median()),
           "touch_size_median": float(pl.concat([g["bid_sz"], g["ask_sz"]]).median()),
           "fair_move_sd_c_per_s": float(dp.std()), "fair_move_abs_p99_c_per_s": float(dp.abs().quantile(0.99))}
    if rc.height:
        out |= {"jumps_per_hour": float(rc.height / live_h) if live_h else None,
                "preempted_share": float(rc["preempted_500ms"].mean()), "preempted_control": float(rc["opposite_500ms"].mean())}
    return out


# ------------------------------------------------------------------------------------------------
# selection: order-level, send-time state, walk-forward ridge
# ------------------------------------------------------------------------------------------------
SEL_FEATS = ["spread_c", "minute", "fair_move_1s_c", "fair_toward_c", "h_send_c", "touch_sz_log", "tau_left_s"]


def order_table(orders: pl.DataFrame, a: pl.DataFrame, fair: pl.DataFrame, tob: pl.DataFrame, seg: pl.DataFrame) -> pl.DataFrame:
    """One row per order that reached the book (not rejected): its send-time state and its net P&L."""
    if not orders.height:
        return pl.DataFrame()
    o = orders.filter(pl.col("end") != 3).join(seg.select("yes", "t_start_s", "t_end_s").unique("yes"), on="yes", how="left")
    o = _asof(o, fair, "t_send", "m_send")
    o = o.with_columns((pl.col("t_send") - NS).alias("_t1"))
    o = _asof(o, fair, "_t1", "m_1s").drop("_t1")
    bk = tob.select("t", "yes", (pl.col("ask_pk") - pl.col("bid_pk")).alias("spread_pk"), "bid_sz", "ask_sz").sort("t")
    o = o.sort("t_send").join_asof(bk, left_on="t_send", right_on="t", by="yes", strategy="backward", suffix="_bk")
    s = pl.col("s")
    o = o.with_columns((pl.col("spread_pk") / 10).alias("spread_c"), ((pl.col("t_send") // NS - pl.col("t_start_s")) // 60).alias("minute"),
                       (100 * (pl.col("m_send") - pl.col("m_1s")).abs()).alias("fair_move_1s_c"),
                       # + = fair value has been moving toward our side's danger (up for an ask)
                       (-100 * s * (pl.col("m_send") - pl.col("m_1s"))).alias("fair_toward_c"),
                       (100 * s * (pl.col("m_send") - pl.col("pk") / PK)).alias("h_send_c"),
                       (pl.when(s == 1).then(pl.col("bid_sz")).otherwise(pl.col("ask_sz")) + 1).log().alias("touch_sz_log"),
                       (pl.col("t_end_s") - pl.col("t_send") / NS).alias("tau_left_s"))
    if a.height:
        pnl = a.group_by("yes", "seg", "oid").agg((pl.col("pnl_hold") + pl.col("c4_rebate")).sum().alias("net_usd"), pl.col("qty").sum().alias("filled_q"))
        o = o.join(pnl, on=["yes", "seg", "oid"], how="left")
    else:
        o = o.with_columns(pl.lit(None, pl.Float64).alias("net_usd"), pl.lit(None, pl.Float64).alias("filled_q"))
    return o.with_columns(pl.col("net_usd").fill_null(0.0), pl.col("filled_q").fill_null(0.0)).drop_nulls(SEL_FEATS)


def _ridge(X, y, lam=1.0):
    mu, sd = X.mean(0), X.std(0) + 1e-9
    Z = np.c_[np.ones(len(X)), (X - mu) / sd]
    b = np.linalg.solve(Z.T @ Z + lam * np.diag([0] + [1] * X.shape[1]), Z.T @ y)
    return lambda Xn: np.c_[np.ones(len(Xn)), (Xn - mu) / sd] @ b


def _auc(y, p):
    o = np.argsort(p)
    r = np.empty(len(p))
    r[o] = np.arange(1, len(p) + 1)
    n1, n0 = y.sum(), len(y) - y.sum()
    return float((r[y == 1].sum() - n1 * (n1 + 1) / 2) / max(n1 * n0, 1))


def selection(O: pl.DataFrame, days: list[str]) -> pl.DataFrame:
    """Walk-forward per rung: fit on days < k, keep the orders predicted to make money on day k."""
    rows = []
    for r in sorted(O["rung"].unique().to_list()):
        o = O.filter(pl.col("rung") == r)
        for k in range(1, len(days)):
            tr, te = o.filter(pl.col("date").is_in(days[:k])), o.filter(pl.col("date") == days[k])
            if tr.height < 200 or not te.height:
                continue
            f = _ridge(tr.select(SEL_FEATS).to_numpy().astype(float), tr["net_usd"].to_numpy())
            pred = f(te.select(SEL_FEATS).to_numpy().astype(float))
            y = te["net_usd"].to_numpy()
            filled = te["filled_q"].to_numpy() > 0
            loses = (y < 0).astype(int)
            insample = _ridge(te.select(SEL_FEATS).to_numpy().astype(float), y)(te.select(SEL_FEATS).to_numpy().astype(float))
            nw = te["yes"].n_unique()
            rows.append({"rung": r, "test_day": days[k], "orders": te.height, "windows": nw,
                         "all_usd_per_window": float(y.sum() / nw),
                         "kept_share": float((pred > 0).mean()), "kept_usd_per_window": float(y[pred > 0].sum() / nw),
                         "auc_loses_filled": _auc(loses[filled], -pred[filled]) if filled.sum() > 50 else None,
                         "insample_kept_usd_per_window": float(y[insample > 0].sum() / nw)})
    return pl.DataFrame(rows)


# ------------------------------------------------------------------------------------------------
# general stats
# ------------------------------------------------------------------------------------------------
def general_stats(W: pl.DataFrame) -> pl.DataFrame:
    """Per rung over all out-of-sample windows in time order (USD, 10-share quotes)."""
    rows = []
    for r in sorted(W["rung"].unique().to_list()):
        w = W.filter(pl.col("rung") == r).sort("t_start_s")
        x = w["pnl"].to_numpy()
        n = len(x)
        cum = np.cumsum(x)
        dd = float((cum - np.maximum.accumulate(np.r_[0.0, cum])[1:]).min()) if n else 0.0
        sd = float(x.std(ddof=1)) if n > 1 else float("nan")
        days = w["date"].n_unique()
        cap = float(w["capital"].max())
        rows.append({"rung": r, "days": days, "windows": n, "pnl_usd": float(x.sum()), "mean_per_window": float(x.mean()),
                     "se_per_window": sd / math.sqrt(n) if n > 1 else None, "sharpe_per_window": float(x.mean() / sd) if sd > 0 else None,
                     "sharpe_annualised": float(x.mean() / sd * math.sqrt(96 * 365)) if sd > 0 else None,
                     "hit_rate_windows": float((x > 0).mean()), "worst_window": float(x.min()), "best_window": float(x.max()),
                     "max_drawdown_usd": dd, "pnl_per_day": float(x.sum() / days), "fills_per_day": float(w["fills"].sum() / days),
                     "shares_per_day": float(w["shares"].sum() / days), "capital_peak_usd": cap,
                     "return_on_capital_per_day": float(x.sum() / days / cap) if cap else None})
    return pl.DataFrame(rows)


# ------------------------------------------------------------------------------------------------
# driver
# ------------------------------------------------------------------------------------------------
def run(sim_root: Path = Path("reports/sim"), out: Path = Path("reports/sim_diag"), fair_root: Path = Path("reports/fair"),
        data_dirs: list[Path] | None = None, spot_root: Path = Path("data_vps"), plot_dir: Path | None = Path("research/strategy_diagnosis"),
        mk_root: Path = Path("reports/markouts")) -> dict:
    data_dirs = data_dirs or [Path("data_vps"), Path("data")]
    out.mkdir(parents=True, exist_ok=True)
    params = json.loads((sim_root / "sim_params.json").read_text())
    sim_res = json.loads((sim_root / "sim.json").read_text())
    sim_dates = sim_res["dates"]
    fair_mode = sim_res.get("fair_mode", "sec")
    sel = pl.DataFrame(params["selected"]).filter(pl.col("rung").is_in(list(RUNGS)))
    days = sorted(sel["date"].unique().to_list())
    cf = {c["cfg"]: c for c in X.all_configs()}
    idx = market_index(pl.read_parquet(fair_root / "reference" / "markets_ref.parquet"))
    rk = rebate_k(fee_table(data_dirs))["btc_15m"]
    Fs, Ws, Os, Rs, MF, PF, EX = [], [], [], [], [], [], []
    for d in days:
        S = load_sim_day(d, fair_root, data_dirs, spot_root, sim_root / d / "fair_sec.parquet", idx, fair_mode=fair_mode)
        P = Dy.prepare(S, d)
        # the BTC-lead oracles replay a different fair stream: prepare it once per lead
        P_lead = {}
        for kw in ORACLES.values():
            if "btc_lead_ms" in kw and kw["btc_lead_ms"] not in P_lead:
                P_lead[kw["btc_lead_ms"]] = Dy.prepare(S | {"fair": refair(S, fair_mode, kw["btc_lead_ms"])}, d)
        seed = sim_dates.index(d) if d in sim_dates else 0
        for r in RUNGS:
            row = sel.filter((pl.col("date") == d) & (pl.col("rung") == r))
            if not row.height:
                continue
            c = cf[row["cfg"][0]]
            for name, kw in ORACLES.items():
                kw = {"lat_ms": X.LAT_BASE} | kw
                Pq = P_lead[kw.pop("btc_lead_ms")] if "btc_lead_ms" in kw else P
                R = Dy.run_day(Pq, r, X.prm_of(c), model=X.MODEL_BASE, seed=seed, **kw)
                a = A.attribute(R["fills"], S["fair"], R["segments"], rk)
                w = A.per_window(a, R["segments"], X.SIZE)
                Ws.append(w.with_columns(pl.lit(d).alias("date"), pl.lit(r).alias("rung"), pl.lit(name).alias("oracle"), pl.lit(row["fold"][0]).alias("fold")))
                if name == "base" and a.height:
                    a2 = a.join(R["segments"].select("yes", "t_start_s").unique("yes"), on="yes", how="left")
                    f = bucket(anatomy(a2, S["fair"], S["tob"]))
                    Fs.append(f.with_columns(pl.lit(d).alias("date"), pl.lit(r).alias("rung")))
                    o = order_table(R["orders"], a, S["fair"], S["tob"], R["segments"])
                    if o.height:
                        Os.append(o.with_columns(pl.lit(d).alias("date"), pl.lit(r).alias("rung")))
            for e in END_S:
                cc = {k: v for k, v in c.items() if k not in ("rung", "cfg")} | {"end_s": e}
                rr = r
                if r == 1:
                    rr, cc["gamma"] = 2, 0.0
                R = Dy.run_day(P, rr, X.prm_of({"rung": rr, **cc}), model=X.MODEL_BASE, lat_ms=X.LAT_BASE, seed=seed)
                a = A.attribute(R["fills"], S["fair"], R["segments"], rk)
                w = A.per_window(a, R["segments"], X.SIZE)
                EX.append({"date": d, "rung": r, "end_s": e, "windows": w.height, "pnl": float(w["pnl"].sum()), "fills": int(w["fills"].sum())})
            log.info("diagnose %s rung %d: %d runs", d, r, len(ORACLES) + len(END_S))
        rc = race(S["fair"], S["trades"], S["segments"])
        if rc.height:
            Rs.append(rc.with_columns(pl.lit(d).alias("date")))
        MF.append({"date": d} | market_factors(S, rc))
        PF.append(predict_frame(d, sim_root / d / "fair_sec.parquet", S["trades"], S["tob"]))
        del S, P, P_lead
    F = pl.concat(Fs, how="diagonal_relaxed")
    W = pl.concat(Ws, how="diagonal_relaxed")
    O = pl.concat(Os, how="diagonal_relaxed")
    RC = pl.concat(Rs, how="diagonal_relaxed") if Rs else pl.DataFrame()
    keep_f = [c for c in F.columns if not c.startswith("m_") or c in ("m_send", "m_pre_1s")]
    F.select(keep_f).write_parquet(out / "fills.parquet")
    W.write_parquet(out / "windows.parquet")
    O.write_parquet(out / "orders.parquet")
    if RC.height:
        RC.write_parquet(out / "race.parquet")
    T = tables(F, W, O, RC, days)
    T["market"] = pl.DataFrame(MF)
    T["predictability"] = predictability(pl.concat(PF), days)
    ex = pl.DataFrame(EX)
    base_ex = W.filter(pl.col("oracle") == "base").group_by("rung").agg(pl.len().alias("windows"), pl.col("pnl").sum(), pl.col("fills").sum()).with_columns(pl.lit(0.0).alias("end_s"))
    T["end_rule"] = (pl.concat([base_ex.select("rung", "end_s", "windows", "pnl", "fills"),
                                ex.group_by("rung", "end_s").agg(pl.col("windows").sum(), pl.col("pnl").sum(), pl.col("fills").sum())], how="diagonal_relaxed")
                     .with_columns((pl.col("pnl") / pl.col("windows")).alias("mean")).sort("rung", "end_s"))
    # the average resting maker on the same market (stage 4): the market's own baseline
    sig = mk_root / "table_signature.parquet"
    if sig.exists():
        T["maker_signature"] = (pl.read_parquet(sig).filter((pl.col("family") == "btc_15m") & (pl.col("ref") == "fair"))
                                .select((pl.col("tau") * 1000).alias("tau_ms"), pl.col("mean").alias("markout_c"), "se", "n", "sample"))
    T["examples"] = examples(F)
    for k, v in T.items():
        v.write_parquet(out / f"table_{k}.parquet")
    res = {"days": days, "rungs": list(RUNGS), "oracles": ORACLES, "selected": sel.to_dicts(), "fair_mode": fair_mode}
    (out / "diagnose.json").write_text(json.dumps(res, indent=1, default=str))
    if plot_dir is not None:
        make_plots(T, plot_dir)
    return {"T": T, "res": res}


def tables(F: pl.DataFrame, W: pl.DataFrame, O: pl.DataFrame, RC: pl.DataFrame, days: list[str]) -> dict:
    T = {}
    base = W.filter(pl.col("oracle") == "base")
    T["general"] = general_stats(base)
    T["general_by_fold"] = pl.concat([general_stats(base.filter(pl.col("fold") == fo)).with_columns(pl.lit(fo).alias("fold"))
                                      for fo in base["fold"].unique().to_list()])
    g = (W.group_by("rung", "oracle").agg(pl.len().alias("windows"), pl.col("pnl").mean().alias("mean"),
                                          (pl.col("pnl").std() / pl.len().sqrt()).alias("se"), pl.col("fills").sum().alias("fills"),
                                          pl.col("shares").sum().alias("shares"))
         .join(base.group_by("rung").agg(pl.col("pnl").mean().alias("base_mean")), on="rung")
         .with_columns((pl.col("mean") - pl.col("base_mean")).alias("gain_per_window"),
                       ((pl.col("mean") - pl.col("base_mean")) / pl.col("base_mean").abs()).alias("gain_vs_loss")))
    # paired SE of the gain: same windows, base vs oracle
    pw = W.select("rung", "oracle", "date", "yes", "pnl").join(base.select("rung", "date", "yes", pl.col("pnl").alias("pnl0")), on=["rung", "date", "yes"])
    pse = pw.group_by("rung", "oracle").agg(((pl.col("pnl") - pl.col("pnl0")).std() / pl.len().sqrt()).alias("gain_se"))
    T["oracles"] = g.join(pse, on=["rung", "oracle"], how="left").sort("rung", "gain_per_window")
    tl = {r: float((F.filter(pl.col("rung") == r)["pnl_hold"] + F.filter(pl.col("rung") == r)["c4_rebate"]).sum()) for r in F["rung"].unique().to_list()}
    for by in ("mech", "age_b", "minute_b", "spread_b", "jump_b", "type", "edge_at_send_b", "through", "cx_pending"):
        T[f"by_{by}"] = pl.concat([group_table(F.filter(pl.col("rung") == r), by, tl[r]).with_columns(pl.lit(r).alias("rung"))
                                   for r in sorted(tl)], how="diagonal_relaxed")
    T["signature"] = pl.concat([signature(F.filter(pl.col("rung") == r)).with_columns(pl.lit(r).alias("rung")) for r in sorted(tl)])
    T["signature_mech"] = pl.concat([signature(F.filter((pl.col("rung") == r) & (pl.col("mech") == m))).with_columns(pl.lit(r).alias("rung"), pl.lit(m).alias("mech"))
                                     for r in sorted(tl) for m in "ABCD"])
    T["selection"] = selection(O, days)
    if RC.height:
        d = RC["delay_ms"].drop_nulls()
        T["race"] = pl.DataFrame([{"jumps": RC.height, "with_taker_within_5s": d.len(),
                                   **{f"p{int(q * 100)}_ms": float(d.quantile(q)) for q in (0.1, 0.25, 0.5, 0.75, 0.9)},
                                   "share_within_75ms": float((d <= 75).sum() / RC.height), "share_within_200ms": float((d <= 200).sum() / RC.height),
                                   "share_within_1s": float((d <= 1000).sum() / RC.height),
                                   "share_preempted_500ms": float(RC["preempted_500ms"].mean()),
                                   "share_opposite_500ms_control": float(RC["opposite_500ms"].mean())}])
        T["race_by_size"] = (RC.with_columns(pl.when(pl.col("dp_c").abs() < 2).then(pl.lit("1-2c")).when(pl.col("dp_c").abs() < 4).then(pl.lit("2-4c"))
                                             .otherwise(pl.lit(">=4c")).alias("jump_size"))
                             .group_by("jump_size").agg(pl.len().alias("jumps"), pl.col("delay_ms").median().alias("median_ms"),
                                                        (pl.col("delay_ms") <= 75).mean().alias("share_within_75ms"),
                                                        pl.col("delay_ms").is_not_null().mean().alias("share_any_taker_5s"),
                                                        pl.col("preempted_500ms").mean().alias("share_preempted_500ms"),
                                                        pl.col("opposite_500ms").mean().alias("share_opposite_500ms_control")).sort("jump_size"))
    return T


def examples(F: pl.DataFrame, n: int = 1) -> pl.DataFrame:
    """The worst fill of each mechanism (rung 1), with its timeline."""
    cols = ["date", "mech", "yes", "s", "P", "qty", "t_send_o", "t_arr_o", "t_pre", "t", "m_send", "m", "M_0ms_c", "M_1000ms_c", "M_60000ms_c",
            "net_c", "through", "cx_pending", "age_ns", "minute", "spread_c", "fair_move_1s_c"]
    x = F.filter(pl.col("rung") == 1).with_columns(((pl.col("pnl_hold") + pl.col("c4_rebate"))).alias("usd"))
    return pl.concat([x.filter(pl.col("mech") == m).sort("usd").head(n) for m in "ABCD"]).select([c for c in cols if c in x.columns] + ["usd"])


# ------------------------------------------------------------------------------------------------
# plots
# ------------------------------------------------------------------------------------------------
BLUE, ORANGE, AQUA, PURPLE, INK, GREY = "#2a78d6", "#eb6834", "#1baf7a", "#8e5bd0", "#3a3a37", "#9a9a94"
MECH_COL = {"A": ORANGE, "B": PURPLE, "C": "#c2410c", "D": AQUA}


def _style(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#e6e5df", lw=0.8)
    ax.set_axisbelow(True)


def make_plots(T: dict, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    out.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.size": 9, "axes.titlesize": 10})
    # 1. where the money goes: USD by mechanism, per rung
    m = T["by_mech"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 3.6), sharey=False)
    for ax, r in zip(axes, (1, 3)):
        x = m.filter(pl.col("rung") == r).sort("mech")
        ax.bar([MECH[k] for k in x["mech"]], x["usd"], color=[MECH_COL[k] for k in x["mech"]])
        for i, (u, n) in enumerate(zip(x["usd"], x["net_c"])):
            ax.text(i, u, f"{n:+.1f}¢/sh", ha="center", va="bottom" if u >= 0 else "top", fontsize=8, color=INK)
        ax.axhline(0, color=INK, lw=0.8)
        ax.set_title(f"Rung {r}: P&L by how the fill happened (USD, all days)")
        ax.set_ylabel("USD")
        ax.tick_params(axis="x", labelsize=8)
        _style(ax)
    fig.tight_layout()
    fig.savefig(out / "mechanism.png", dpi=150)
    plt.close(fig)
    # 2. when the loss arrives: markout curve of our fills vs the average maker
    sg = T["signature"]
    fig, ax = plt.subplots(figsize=(7.5, 3.8))
    for r, col in ((1, BLUE), (3, ORANGE)):
        x = sg.filter(pl.col("rung") == r).sort("tau_ms")
        ax.plot([max(t, 50) for t in x["tau_ms"]], x["markout_c"], marker="o", ms=4, lw=2, color=col, label=f"our fills, rung {r}")
    if "maker_signature" in T:
        x = T["maker_signature"].filter(pl.col("sample") == "balanced").sort("tau_ms") if "sample" in T["maker_signature"].columns else T["maker_signature"]
        x = x.filter(pl.col("tau_ms") <= 60000)
        ax.plot([max(t, 50) for t in x["tau_ms"]], x["markout_c"], marker="s", ms=3, lw=1.5, color=GREY, label="average resting maker (stage 4)")
    ax.axhline(0, color=INK, lw=0.8)
    ax.set_xscale("log")
    ax.set_xticks([50, 100, 250, 500, 1000, 5000, 10000, 60000], ["fill", "0.1", "0.25", "0.5", "1", "5", "10", "60"])
    ax.set_xlabel("seconds after the fill")
    ax.set_ylabel("¢ per share")
    ax.set_title("Markout of each filled share against fair value: most of the loss lands within 1 s")
    ax.legend(frameon=False, fontsize=8)
    _style(ax)
    fig.tight_layout()
    fig.savefig(out / "signature.png", dpi=150)
    plt.close(fig)
    # 3. what each lever is worth at most (oracles)
    o = T["oracles"].filter(pl.col("oracle") != "base")
    fig, axes = plt.subplots(1, 2, figsize=(12, 3.8))
    for ax, r in zip(axes, (1, 3)):
        x = o.filter(pl.col("rung") == r).sort("gain_per_window")
        ax.barh(x["oracle"], x["gain_per_window"], xerr=1.96 * x["gain_se"].fill_null(0), color=[AQUA if g > 0 else ORANGE for g in x["gain_per_window"]])
        ax.axvline(0, color=INK, lw=0.8)
        ax.set_title(f"Rung {r}: change in P&L per window with one handicap removed (95 % CI)")
        ax.set_xlabel("USD per window vs the real setting")
        ax.grid(axis="x", color="#e6e5df", lw=0.8)
        ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(out / "oracles.png", dpi=150)
    plt.close(fig)
    # 4. the race: who trades first after a BTC jump
    if "race_by_size" in T:
        x = T["race_by_size"]
        fig, ax = plt.subplots(figsize=(7, 3.4))
        k = range(x.height)
        ax.bar([i - 0.2 for i in k], 100 * x["share_preempted_500ms"], width=0.38, color=ORANGE, label="a taker traded in the jump's direction in the 500 ms BEFORE we saw it")
        ax.bar([i + 0.2 for i in k], 100 * x["share_opposite_500ms_control"], width=0.38, color=GREY, label="control: opposite direction, same window")
        ax.set_xticks(list(k), [f"fair jump {j}" for j in x["jump_size"]])
        ax.set_ylabel("% of jumps")
        ax.set_title("Takers see BTC before we do")
        ax.legend(frameon=False, fontsize=7.5)
        _style(ax)
        fig.tight_layout()
        fig.savefig(out / "race.png", dpi=150)
        plt.close(fig)


# ------------------------------------------------------------------------------------------------
# how much of the next second is predictable from what we already record
# ------------------------------------------------------------------------------------------------
PRED_FEATS = ["dfair_1s", "dfair_5s", "gap_mid_fair", "spot_ret_1s", "spot_ret_5s", "flow_1s", "flow_5s", "book_imb"]


def predict_frame(d: str, fair_sec: Path, trades: pl.DataFrame, tob: pl.DataFrame) -> pl.DataFrame:
    """1 s grid per BTC 15m token: target = fair-value change over the next 1 s (¢); features known
    at t: the fair value's own last moves, the mid − fair gap, BTC returns, signed taker flow
    (+ = takers buying YES) over the last 1 s / 5 s, and the top-of-book size imbalance."""
    fs = (pl.read_parquet(fair_sec).filter(pl.col("family") == "btc_15m").select("asset_id", "sec", "p_fair", "mid", "S").sort("asset_id", "sec")
          .rename({"asset_id": "yes"}))
    lag = lambda c, k: pl.col(c).shift(k).over("yes")
    fs = fs.with_columns((100 * (pl.col("p_fair").shift(-1).over("yes") - pl.col("p_fair"))).alias("y_1s"),
                         (100 * (pl.col("p_fair") - lag("p_fair", 1))).alias("dfair_1s"), (100 * (pl.col("p_fair") - lag("p_fair", 5))).alias("dfair_5s"),
                         (100 * (pl.col("mid") - pl.col("p_fair"))).alias("gap_mid_fair"),
                         (1e4 * (pl.col("S") / lag("S", 1)).log()).alias("spot_ret_1s"), (1e4 * (pl.col("S") / lag("S", 5)).log()).alias("spot_ret_5s"))
    # taker flow: maker side s = −1 means a taker BOUGHT YES
    tr = trades.select("yes", (pl.col("t") // NS).alias("sec"), (-pl.col("s").cast(pl.Float64) * pl.col("v")).alias("flow"))
    fl = tr.group_by("yes", "sec").agg(pl.col("flow").sum())
    fs = fs.join(fl, on=["yes", "sec"], how="left").with_columns(pl.col("flow").fill_null(0.0)).sort("yes", "sec")
    fs = fs.with_columns(pl.col("flow").alias("flow_1s"), pl.col("flow").rolling_sum(5, min_samples=1).over("yes").alias("flow_5s")).drop("flow")
    bk = tob.select("t", "yes", (pl.col("bid_sz") / (pl.col("bid_sz") + pl.col("ask_sz"))).alias("book_imb")).sort("t")
    fs = fs.with_columns((pl.col("sec") * NS).alias("t")).sort("t").join_asof(bk, on="t", by="yes", strategy="backward").drop("t")
    return fs.with_columns(pl.lit(d).alias("date")).drop_nulls(["y_1s", *PRED_FEATS])


def predictability(G: pl.DataFrame, days: list[str]) -> pl.DataFrame:
    """Walk-forward out-of-sample R² and correlation of a ridge model of the next 1 s fair move, plus
    each feature's own correlation; and the share of 1 s moves ≥ 1 ¢ whose sign the model gets right."""
    rows = []
    for k in range(1, len(days)):
        tr, te = G.filter(pl.col("date").is_in(days[:k])), G.filter(pl.col("date") == days[k])
        if tr.height < 1000 or not te.height:
            continue
        f = _ridge(tr.select(PRED_FEATS).to_numpy().astype(float), tr["y_1s"].to_numpy(), lam=10.0)
        p = f(te.select(PRED_FEATS).to_numpy().astype(float))
        y = te["y_1s"].to_numpy()
        big = np.abs(y) >= 1.0
        rows.append({"test_day": days[k], "seconds": te.height, "r2_oos": float(1 - ((y - p) ** 2).sum() / ((y - y.mean()) ** 2).sum()),
                     "corr": float(np.corrcoef(p, y)[0, 1]), "big_moves": int(big.sum()),
                     "sign_hit_big_moves": float((np.sign(p[big]) == np.sign(y[big])).mean()) if big.sum() else None,
                     **{f"corr_{c}": float(np.corrcoef(te[c].to_numpy(), y)[0, 1]) for c in PRED_FEATS}})
    return pl.DataFrame(rows)

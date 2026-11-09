"""Q2–Q4: what the probes say. Every table is per cancel model, with SEs clustered by market
window (`condition_id`): probes in one 15-minute window share one BTC path and one book.

Units: cents per share, maker's view (+ = we made money). One probe = one placement of a
5-share order (the minimum size), so a probe's value is "what placing this order was worth".

    P_fill(T)   share of placements filled within T seconds, before being cancelled for being
                left behind. Being left behind is a policy decision (a competing risk), not
                missing data, so it counts as "not filled"; only probes cut by the end of the
                clean window / market before T are dropped (administrative censoring).
    a(x)        adverse selection of our fills at queue position x: h − M(τ), h measured at
                the fill against the fair value
    V           value of a placement: 1{filled} · (M(τ) + rebate), 0 if never filled
"""
from __future__ import annotations

import numpy as np
import polars as pl

from ..markouts.engine import add_markouts
from ..markouts.fills import NS
from ..markouts.stats import grouped_mean, wide
from .engine import MODELS

TAUS = (1, 5, 10, 30, 60)
TS = (1, 5, 10, 30, 60)
X_EDGES = [0.5, 50, 200, 1000, 5000]
X_LABELS = ["0", "1-50", "50-200", "200-1k", "1k-5k", ">5k"]
CL = "condition_id"


def _bucket(col: str, edges, labels) -> pl.Expr:
    return pl.col(col).cut(edges, labels=labels, left_closed=True).cast(pl.String)


def add_features(R: pl.DataFrame, fair: pl.DataFrame | None) -> pl.DataFrame:
    """Decision-time features and buckets. h_dec: how far inside the fair value the quote sits,
    100·s·(fair − p), at the decision instant (the 1 s fair grid, last whole second)."""
    R = R.with_columns((pl.col("pk") / 1000).alias("P"), (pl.col("t_dec") // NS).alias("sec"))
    if fair is not None and fair.height:
        R = R.join(fair.select(pl.col("asset_id").alias("yes"), "sec", "p_fair"), on=["yes", "sec"], how="left")
    else:
        R = R.with_columns(pl.lit(None, pl.Float64).alias("p_fair"))
    return R.with_columns(
        (100 * pl.col("s") * (pl.col("p_fair") - pl.col("P"))).alias("h_dec"),
        _bucket("x0", X_EDGES, X_LABELS).alias("x0b"),
        # Why q_touch for decisions: x0 is read on ARRIVAL (128 ms after the decision), so using it
        # to decide would be lookahead; the touch size at the decision is what a quoter sees.
        _bucket("q_touch", X_EDGES, X_LABELS).alias("qtb"),
        # queue position actually taken: "0" = a price-improving order alone at a new best price
        # (the true front); touch orders that were still at the touch on arrival get their x0
        # bucket; a touch order whose level was swept in flight is not a queue position (null)
        pl.when((pl.col("type") == "improve") & (pl.col("x0") == 0) & (pl.col("pos_arr") < 0)).then(pl.lit("0"))
          .when((pl.col("type") == "touch") & (pl.col("pos_arr") == 0)).then(_bucket("x0", X_EDGES, X_LABELS))
          .otherwise(None).alias("qpos"),
        _bucket("spread_c", [1.5, 2.5, 5.5], ["1c", "2c", "3-5c", ">5c"]).alias("spread_b"),
        _bucket("imb_adv", [0.2, 0.4, 0.6, 0.8], ["<0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8", ">=0.8"]).alias("imb_b"),
        pl.when(pl.col("family") == "btc_15m").then(((pl.col("t_dec") // NS) - pl.col("t_start_s")) // 60).otherwise(None).alias("minute"),
    )


def probe_markouts(R: pl.DataFrame, tob: pl.DataFrame, fair: pl.DataFrame | None, fees: pl.DataFrame) -> pl.DataFrame:
    """For each cancel model: the stage-4 markout engine on the probes' first fills. The fill is
    at our price P, at the instant the queue reached us; h is read just before that instant."""
    R = R.join(fees, on="family", how="left").with_columns(
        (pl.col("fee_rate").fill_null(0) * pl.col("P") * (1 - pl.col("P"))).alias("taker_fee_ps"),
        (pl.col("fee_rebate_rate").fill_null(0) * pl.col("fee_rate").fill_null(0) * pl.col("P") * (1 - pl.col("P"))).alias("rebate_ps"))
    mids = tob.select("t", "yes", "mid").sort("t")
    out = R.select("pid", (100 * pl.col("rebate_ps")).alias("rebate_c"))
    for m in MODELS:
        f = R.filter(pl.col(f"t_first_{m}") >= 0).select(
            "pid", "yes", "s", "P", "outcome", "t_end_s", "rebate_ps", "taker_fee_ps",
            pl.col(f"t_first_{m}").alias("t_recv"),
            # Why the taker order's start: a sweep moves the mid before it reaches a deeper level;
            # h must be read before the whole order arrived, as in stage 4 (audit finding 2c)
            (pl.min_horizontal(pl.col(f"t_first_{m}"), pl.when(pl.col(f"t_pre_{m}") >= 0).then(pl.col(f"t_pre_{m}")).otherwise(pl.col(f"t_first_{m}"))) - 1).alias("t_pre"),
            pl.col("t_hi").alias("t_last"))
        if not f.height:
            continue
        f = f.sort("t_pre").join_asof(mids.rename({"t": "t_pre"}), on="t_pre", by="yes", strategy="backward")
        mk = add_markouts(f, tob.select("t", "yes", "mid"), fair, taus=TAUS)
        keep = ["h_fair", *[f"M{k}_fair" for k in TAUS], "h_mid", "M1_mid", "M60_mid", "M_res"]
        out = out.join(mk.select("pid", *[pl.col(c).alias(f"{c}_{m}") for c in keep]), on="pid", how="left")
    return R.drop("taker_fee_ps", "rebate_ps", "fee_rate", "fee_rebate_rate").join(out, on="pid", how="left")


# ------------------------------------------------------------------------------------------------
# quoting policies: what the order does when it goes stale
# ------------------------------------------------------------------------------------------------
POLICIES = ("passive", "alone", "stale")
CANCEL_LAT_MS = (0, 50, 128, 300, 1000)


def apply_policy(R: pl.DataFrame, policy: str, lc_ms: int = 128) -> pl.DataFrame:
    """passive  rest until the probe stops (no cancel rule): the sitting duck
    alone    cancel when the rest of our side has moved through our price (book signal)
    stale    cancel at the first of `alone` and the fair value crossing our price
    The cancel lands lc_ms later; a fill before it lands still happens (the latency cost).
    Why this is exact after the fact: until the cancel lands, the queue path is the same as the
    passive probe's, so a fill counts iff the passive first fill came before the cancel landed."""
    if policy == "passive":
        return R.with_columns(pl.lit(-1, pl.Int64).alias("t_cancel"))
    if policy == "alone":
        trig = pl.col("t_alone")
    else:
        trig = (pl.when((pl.col("t_alone") >= 0) & ((pl.col("t_stale") < 0) | (pl.col("t_alone") <= pl.col("t_stale")))).then(pl.col("t_alone"))
                .otherwise(pl.col("t_stale")))
    tc = pl.when(trig >= 0).then(trig + lc_ms * 1_000_000).otherwise(-1)
    R = R.with_columns(tc.alias("t_cancel"), tc.alias("_tc"))
    return R.with_columns([pl.when((pl.col(f"t_first_{m}") >= 0) & ((pl.col("_tc") < 0) | (pl.col(f"t_first_{m}") <= pl.col("_tc"))))
                           .then(pl.col(f"t_first_{m}")).otherwise(-1).alias(f"t_first_{m}") for m in MODELS
                           if f"t_first_{m}" in R.columns]).drop("_tc")


# ------------------------------------------------------------------------------------------------
# Q2 fill probability
# ------------------------------------------------------------------------------------------------
def _filled_by(m: str, T: int) -> pl.Expr:
    return (pl.col(f"t_first_{m}") >= 0) & (pl.col(f"t_first_{m}") - pl.col("t_arr") <= T * NS)


def _eligible(m: str, T: int) -> pl.Expr:
    """Not rejected, and observed for T seconds or resolved (filled / left behind / cancelled by
    the policy) before: a policy cancel is a decision, not missing data (audit finding 6)."""
    cancelled = (pl.col("t_cancel") >= 0) & (pl.col("t_cancel") - pl.col("t_arr") <= T * NS)
    return (pl.col("stop") != "rejected") & (
        _filled_by(m, T) | (pl.col("t_stop") - pl.col("t_arr") >= T * NS) | pl.col("stop").is_in(["left_behind", "filled"]) | cancelled)


def fill_prob(R: pl.DataFrame, by: list[str], models=MODELS, Ts=TS) -> pl.DataFrame:
    out = []
    for m in models:
        for T in Ts:
            d = R.filter(_eligible(m, T)).with_columns(_filled_by(m, T).cast(pl.Float64).alias("y"))
            g = grouped_mean(d, ["y"], by, cluster=CL)
            if g.height:
                out.append(g.with_columns(pl.lit(m).alias("model"), pl.lit(T).alias("T")).drop("col"))
    return pl.concat(out) if out else pl.DataFrame()


def stop_mix(R: pl.DataFrame) -> pl.DataFrame:
    return (R.group_by("family", "design", "type", "stop").len()
            .with_columns((pl.col("len") / pl.col("len").sum().over("family", "design", "type")).round(4).alias("share"))
            .sort("family", "design", "type", "stop"))


def intensity(R: pl.DataFrame, m: str) -> tuple[pl.DataFrame, dict]:
    """λ(δ): fills per minute of resting time, by distance inside the fair value δ = h_dec (¢).
    Resting time ends at the first fill or the stop. Fit ln λ = ln A − k δ on δ ≥ 0 buckets
    (Poisson-weighted least squares: weight = number of fills)."""
    d = R.filter((pl.col("stop") != "rejected") & pl.col("h_dec").is_not_null()).with_columns(
        pl.when(pl.col(f"t_first_{m}") >= 0).then(pl.col(f"t_first_{m}")).otherwise(pl.col("t_stop")).alias("_end"),
        (pl.col(f"t_first_{m}") >= 0).cast(pl.Int64).alias("_f"),
        ((pl.col("h_dec") * 2).floor() / 2).clip(-2, 4).alias("delta"))
    g = (d.group_by("family", "delta").agg(pl.col("_f").sum().alias("fills"),
                                           ((pl.col("_end") - pl.col("t_arr")).sum() / NS / 60).alias("minutes"), pl.len().alias("n"))
         .with_columns((pl.col("fills") / pl.col("minutes")).alias("lam_per_min"), pl.lit(m).alias("model")).sort("family", "delta"))
    fit = {}
    for fam in g["family"].unique().to_list():
        x = g.filter((pl.col("family") == fam) & (pl.col("delta") >= 0) & (pl.col("fills") >= 20))
        if x.height >= 3:
            w = x["fills"].to_numpy().astype(float)
            X = np.c_[np.ones(x.height), x["delta"].to_numpy() + 0.25]
            yv = np.log(x["lam_per_min"].to_numpy())
            b = np.linalg.solve(X.T @ (w[:, None] * X), X.T @ (w * yv))
            fit[fam] = {"A_per_min": round(float(np.exp(b[0])), 4), "k_per_cent": round(float(-b[1]), 4), "buckets": x.height}
    return g, fit


# ------------------------------------------------------------------------------------------------
# Q3 toxicity of our fills,  Q4 value of a placement
# ------------------------------------------------------------------------------------------------
def fills_long(R: pl.DataFrame, models=MODELS) -> pl.DataFrame:
    """One row per (probe, model) that filled: h, M(τ), a(τ) = h − M(τ), through flag."""
    parts = []
    for m in models:
        if f"h_fair_{m}" not in R.columns:
            continue
        parts.append(R.filter(pl.col(f"t_first_{m}") >= 0).select(
            "pid", "date", "family", "design", "type", "lat_ms", "x0b", "qpos", "qtb", "spread_b", "imb_b", "minute", CL, "rebate_c",
            pl.lit(m).alias("model"), pl.col(f"through_{m}").alias("through"),
            ((pl.col(f"t_first_{m}") - pl.col("t_arr")) / NS).alias("wait_s"),
            pl.col(f"h_fair_{m}").alias("h"), *[pl.col(f"M{k}_fair_{m}").alias(f"M{k}") for k in TAUS],
            pl.col(f"M_res_{m}").alias("M_res"))
            .with_columns([(pl.col("h") - pl.col(f"M{k}")).alias(f"a{k}") for k in TAUS]))
    return pl.concat(parts) if parts else pl.DataFrame()


def toxicity(F: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    cols = ["h", "M1", "M10", "M60", "a1", "a10", "a60", "wait_s"]
    return wide(grouped_mean(F.with_columns(pl.col("through").cast(pl.Float64)), cols + ["through"], by, cluster=CL), by)


def back_of_queue_penalty(tox: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    """D(x) = a(x) − a(0): how much worse a fill at queue position x is than one at the front
    ("0" = a price-improving order alone at the new best price)."""
    front = tox.filter(pl.col("qpos") == "0").select(*by, pl.col("a1").alias("a1_front"), pl.col("a60").alias("a60_front"))
    return tox.join(front, on=by, how="left").with_columns(
        (pl.col("a1") - pl.col("a1_front")).alias("D1"), (pl.col("a60") - pl.col("a60_front")).alias("D60"))


def value(R: pl.DataFrame, by: list[str], models=MODELS, taus=(1, 60), rebate: bool = True) -> pl.DataFrame:
    """V per placement: filled within the probe's life → M(τ) (+ rebate), else 0. Probes whose
    fill has no τ markout (window/market ended first) are dropped for that τ."""
    out = []
    for m in models:
        if f"h_fair_{m}" not in R.columns:
            continue
        for k in taus:
            Mk = pl.col(f"M{k}_fair_{m}") + (pl.col("rebate_c") if rebate else 0)
            # Why the full horizon must fit: dropping only filled probes without a markout (fills in
            # the window's last seconds, plausibly the most toxic) while keeping unfilled ones
            # biases V up (audit finding 5). A placement counts only if any fill in its 60 s life
            # still has its τ markout inside the window.
            d = (R.filter((pl.col("stop") != "rejected") & (pl.col("t_arr") + (60 + k) * NS <= pl.col("t_hi")))
                 .filter((pl.col(f"t_first_{m}") < 0) | pl.col(f"M{k}_fair_{m}").is_not_null())
                 .with_columns(pl.when(pl.col(f"t_first_{m}") >= 0).then(Mk).otherwise(0.0).alias("V")))
            g = grouped_mean(d, ["V"], by, cluster=CL)
            if g.height:
                out.append(g.drop("col").with_columns(pl.lit(m).alias("model"), pl.lit(k).alias("tau"), pl.lit(rebate).alias("rebate")))
    return pl.concat(out) if out else pl.DataFrame()


def x_star(V: pl.DataFrame) -> dict:
    """Per (family, model, tau): the first touch-size bucket at the decision (front to back) whose
    placement value is negative at its point estimate: the line is not worth joining from there on."""
    res = {}
    order = {b: i for i, b in enumerate(X_LABELS)}
    for (fam, m, k), g in V.group_by("family", "model", "tau"):
        g = g.with_columns(pl.col("qtb").replace_strict(order, default=99).alias("o")).sort("o")
        neg = g.filter(pl.col("mean") < 0)
        res[f"{fam}|{m}|{k}"] = neg["qtb"][0] if neg.height else "none negative"
    return res


# ------------------------------------------------------------------------------------------------
# latency, the Gate A slice for our own orders, capacity
# ------------------------------------------------------------------------------------------------
def latency(R: pl.DataFrame, models=("pess", "matched")) -> pl.DataFrame:
    """Spread openings: we try to step into the gap at latency L. Did someone get there first
    (x0 > 0), were we rejected (the gap closed), and what was the placement worth?"""
    d = R.filter(pl.col("design") == "opening")
    if not d.height:
        return pl.DataFrame()
    a = grouped_mean(d.with_columns((pl.col("stop") == "rejected").cast(pl.Float64).alias("rejected"),
                                    (pl.col("x0") > 0).cast(pl.Float64).alias("beaten")), ["rejected", "beaten"], ["family", "lat_ms"], cluster=CL)
    b = fill_prob(d, ["family", "lat_ms"], models=models, Ts=(1, 10, 60))
    v = value(d, ["family", "lat_ms"], models=models)
    return pl.concat([a.with_columns(pl.lit(None, pl.String).alias("model"), pl.lit(None, pl.Int32).alias("T"), pl.lit(None, pl.Int32).alias("tau")),
                      b.with_columns(pl.lit("P_fill").alias("col"), pl.lit(None, pl.Int32).alias("tau")),
                      v.with_columns(pl.lit("V").alias("col"), pl.lit(None, pl.Int32).alias("T")).drop("rebate")], how="diagonal_relaxed")


def gate_ours(R: pl.DataFrame, test_days: list[str], min_clusters: int = 10) -> tuple[pl.DataFrame, dict]:
    """Gate A's slice (quoted spread > 5¢) for OUR orders: step into the gap (improve, 128 ms,
    grid + openings). Positive placement value without the rebate at 95 % on every held-out day,
    at τ* = 1 s and at 60 s, under the pessimistic AND the matched cancel model."""
    d = R.filter((pl.col("type") == "improve") & (pl.col("lat_ms") == 128) & (pl.col("spread_b") == ">5c") & (pl.col("family") == "btc_15m"))
    if not d.height:
        return pl.DataFrame(), {"no_rebate": "NO DATA", "with_rebate": "NO DATA"}
    rows = []
    for reb in (False, True):
        V = value(d, ["date"], models=("pess", "matched"), rebate=reb)
        rows.append(V)
    rows = [r for r in rows if r.height]
    if not rows:
        return pl.DataFrame(), {"no_rebate": "NO DATA", "with_rebate": "NO DATA"}
    T = pl.concat(rows)
    verdict = {}
    for reb in (False, True):
        ok = True
        for m in ("pess", "matched"):
            for k in (1, 60):
                for day in test_days:
                    r = T.filter((pl.col("model") == m) & (pl.col("tau") == k) & (pl.col("date") == day) & (pl.col("rebate") == reb))
                    if not r.height or r["se"][0] is None or r["n_clusters"][0] < min_clusters or r["mean"][0] - 1.96 * r["se"][0] <= 0:
                        ok = False
        verdict["with_rebate" if reb else "no_rebate"] = "PASS" if ok else "FAIL"
    return T.sort("rebate", "model", "tau", "date"), verdict


def capacity(R: pl.DataFrame, m: str = "matched", q: int = 50) -> pl.DataFrame:
    """Spread openings at 128 ms, per day and spread bucket: how many, how many filled, and the
    USD a q-share order stepping into each gap would have made (filled shares × 60 s markout incl.
    rebate). One opening = one decision, so no double counting across probes."""
    if f"M60_fair_{m}" not in R.columns:
        return pl.DataFrame()
    d = R.filter((pl.col("design") == "opening") & (pl.col("lat_ms") == 128) & (pl.col("stop") != "rejected")
                 & ((pl.col(f"t_first_{m}") < 0) | pl.col(f"M60_fair_{m}").is_not_null()))
    d = d.with_columns(pl.when(pl.col(f"t_first_{m}") >= 0).then(pl.col(f"filled{q}_{m}") * (pl.col(f"M60_fair_{m}") + pl.col("rebate_c")) / 100).otherwise(0.0).alias("usd"))
    return (d.group_by("family", "date", "spread_b").agg(pl.len().alias("placements"), (pl.col(f"t_first_{m}") >= 0).sum().alias("fills"),
                                                         pl.col(f"filled{q}_{m}").sum().alias("shares"), pl.col("usd").sum().round(2).alias("usd"))
            .sort("family", "date", "spread_b"))


# ------------------------------------------------------------------------------------------------
# walk-forward fill model: P(fill within 10 s) from what is known at the decision
# ------------------------------------------------------------------------------------------------
# Why no x0: it is read on arrival, 128 ms after the decision (audit finding 3); q_touch is known
FILL_FEATS = ["is_touch", "is_behind", "spread_c", "imb_adv", "log_qt", "min_frac", "h_dec_c"]


def _fill_X(d: pl.DataFrame) -> pl.DataFrame:
    return d.with_columns(
        (pl.col("type") == "touch").cast(pl.Float64).alias("is_touch"),
        (pl.col("type") == "behind").cast(pl.Float64).alias("is_behind"), (pl.col("q_touch") + 1).log().alias("log_qt"),
        (pl.col("minute").fill_null(7) / 15).alias("min_frac"), pl.col("h_dec").clip(-5, 5).fill_null(0).alias("h_dec_c"))


def _logit_fit(X: np.ndarray, y: np.ndarray, lam: float = 1e-3, iters: int = 25) -> np.ndarray:
    b = np.zeros(X.shape[1])
    for _ in range(iters):
        p = 1 / (1 + np.exp(-(X @ b)))
        W = p * (1 - p)
        H = X.T @ (W[:, None] * X) + lam * len(y) * np.eye(X.shape[1])
        step = np.linalg.solve(H, X.T @ (y - p) - lam * len(y) * b)
        b += step
        if np.abs(step).max() < 1e-8:
            break
    return b


def _auc(y: np.ndarray, p: np.ndarray) -> float:
    o = np.argsort(p)
    r = np.empty(len(p))
    r[o] = np.arange(1, len(p) + 1)
    n1 = y.sum()
    n0 = len(y) - n1
    return float((r[y == 1].sum() - n1 * (n1 + 1) / 2) / max(n1 * n0, 1))


def fill_model(R: pl.DataFrame, dates: list[str], m: str = "matched", T: int = 10, fam: str = "btc_15m") -> tuple[pl.DataFrame, dict]:
    R = apply_policy(R, "passive")
    d = _fill_X(R.filter((pl.col("family") == fam) & (pl.col("design") == "grid") & _eligible(m, T))
                .with_columns(_filled_by(m, T).cast(pl.Float64).alias("y")))
    rows, coefs = [], {}
    for i, day in enumerate(dates[1:], start=1):
        tr, te = d.filter(pl.col("date").is_in(dates[:i])), d.filter(pl.col("date") == day)
        if not tr.height or not te.height:
            continue
        Xtr = tr.select(FILL_FEATS).to_numpy().astype(float)
        mu, sd = Xtr.mean(0), Xtr.std(0) + 1e-9
        Z = np.c_[np.ones(len(Xtr)), (Xtr - mu) / sd]
        b = _logit_fit(Z, tr["y"].to_numpy())
        Zte = np.c_[np.ones(te.height), (te.select(FILL_FEATS).to_numpy().astype(float) - mu) / sd]
        p = 1 / (1 + np.exp(-(Zte @ b)))
        y = te["y"].to_numpy()
        base = tr["y"].mean()
        rows.append({"family": fam, "test_day": day, "model": m, "T": T, "n": te.height, "rate": round(float(y.mean()), 4),
                     "auc": round(_auc(y, p), 4), "brier": round(float(((p - y) ** 2).mean()), 5),
                     "brier_base": round(float(((base - y) ** 2).mean()), 5)})
        coefs[day] = {f: round(float(c), 4) for f, c in zip(["const", *FILL_FEATS], b)}
    return pl.DataFrame(rows), coefs

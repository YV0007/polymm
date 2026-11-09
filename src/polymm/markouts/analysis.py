"""A2–A5: signature plots, slices, the conditional model, and Gate A.

Units: cents per share, maker's view (+ = the maker made money). P&L in USD = cents/100 · shares.
"""
from __future__ import annotations

import math

import numpy as np
import polars as pl

from .engine import TAUS
from .stats import grouped_mean, wide  # noqa: F401  (grouped_mean re-exported for run.py)

FAMS = ("btc_15m", "btc_daily_ladder", "fed_dec_2026")
SHORT = tuple(k for k in TAUS if k <= 60)  # the balanced panel: every fill has all of these


# ---- A2: signature --------------------------------------------------------------------------


def balanced(f: pl.DataFrame, ref: str) -> pl.DataFrame:
    """Fills with h and every short-horizon markout present. Why: comparing M(1) on one set of
    fills with M(60) on a smaller set (the ones far enough from a window end) mixes the decay
    with a change of sample. Long horizons are reported on their own samples, flagged."""
    return f.filter(pl.all_horizontal([pl.col(f"h_{ref}").is_not_null()] + [pl.col(f"M{k}_{ref}").is_not_null() for k in SHORT]))


def signature(f: pl.DataFrame, by: list[str], weight: str | None = None) -> pl.DataFrame:
    """Long: by…, ref, tau (0 = h, -1 = resolution), sample ('balanced' | 'own'), n, n_clusters, mean, se."""
    rows = []
    for ref in ("mid", "fair"):
        b = balanced(f, ref)
        cols = [f"h_{ref}"] + [f"M{k}_{ref}" for k in SHORT]
        t = grouped_mean(b, cols, by, weight).with_columns(pl.lit("balanced").alias("sample"))
        own = grouped_mean(f, [f"M{k}_{ref}" for k in TAUS if k > 60], by, weight).with_columns(pl.lit("own").alias("sample"))
        for x in (t, own):
            if x.height:
                rows.append(x.with_columns(pl.lit(ref).alias("ref")))
    res = grouped_mean(f.filter(pl.col("M_res").is_not_null()), ["M_res", "rebate_c", "fee_c"], by, weight)
    if res.height:
        rows.append(res.with_columns(pl.lit("own").alias("sample"), pl.lit("res").alias("ref")))
    t = pl.concat(rows, how="diagonal_relaxed")
    tau = (pl.when(pl.col("col").str.starts_with("h_")).then(0)
             .when(pl.col("col") == "M_res").then(-1)
             .when(pl.col("col").str.starts_with("M")).then(pl.col("col").str.extract(r"^M(\d+)_").cast(pl.Int64))
             .otherwise(None))
    return t.with_columns(tau.alias("tau")).sort(*by, "ref", "tau")


def tau_star(f_train: pl.DataFrame, min_fills: int = 2000, z: float = 2.0, default: int = 60, info: dict | None = None) -> dict[str, int]:
    """The information horizon per family, fixed on the TRAINING day before any edge is looked at.

    Cumulative rule: τ* = the first balanced horizon τ ∈ {0 (= h), 1, 2, 5, 10, 30} whose REMAINING
    decay to 60 s, d = M(τ) − M(60) per fill, is not significant (cluster t < z); 60 if even
    M(30) − M(60) is. A family whose TOTAL decay h − M(60) is not significant, or with too few
    fills, gets `default` and is recorded as a fallback in `info` (its adverse selection, if any,
    is slower than 60 s).
    Why cumulative and not "the last significant step" (v1 of this function): the SE grows with
    τ, so single later steps rarely pass and τ* was pushed short even while the curve kept
    falling (audit 2026-11-08: 11-07 went M1 −0.39 → M60 −1.02). Testing what is still left to
    lose compares every τ with the same far end."""
    out = {}
    info = info if info is not None else {}
    grid = (0, *[k for k in SHORT if k < 60])
    for fam in FAMS:
        d = balanced(f_train.filter(pl.col("family") == fam), "fair")
        if d.height < min_fills:
            out[fam], info[fam] = default, f"fallback: {d.height} fills"
            continue
        tau = None
        for k in grid:
            c = "h_fair" if k == 0 else f"M{k}_fair"
            r = grouped_mean(d.with_columns((pl.col(c) - pl.col("M60_fair")).alias("dd")), ["dd"], []).row(0, named=True)
            t = r["mean"] / r["se"] if r["se"] else 0.0
            if k == 0 and t < z:
                break  # no significant decay from h to 60 s at all
            if k > 0 and t < z:
                tau = k
                break
        else:
            tau = 60
        if tau is None:
            out[fam], info[fam] = default, "fallback: h − M(60) not significant"
        else:
            out[fam], info[fam] = tau, "estimated"
    return out


# ---- A3: slices -------------------------------------------------------------------------------


def _bucket(col: str, edges: list[float], labels: list[str]) -> pl.Expr:
    e = pl.lit(labels[-1])
    for edge, lab in reversed(list(zip(edges, labels[:-1]))):
        e = pl.when(pl.col(col) < edge).then(pl.lit(lab)).otherwise(e)
    return pl.when(pl.col(col).is_null()).then(None).otherwise(e)


# name → (expression, ex_ante). Ex-ante = known when the quote is resting, before anyone hits it
# (the quote's own price and place, the book, BTC, recent flow, the clock). Fill characteristics
# (the taker's size, whether it swept) are NOT: a quote cannot choose who fills it. They explain
# the cost; only ex-ante features can be used to avoid it.
SLICES: dict[str, tuple[pl.Expr, bool]] = {
    "minute": (pl.col("minute").cast(pl.Int64).cast(pl.String).str.zfill(2), True),
    "tte": (_bucket("tte_s", [3600, 3 * 3600, 12 * 3600], ["<1h", "1-3h", "3-12h", ">=12h"]), True),
    "price": (_bucket("P", [0.1, 0.3, 0.7, 0.9], ["<0.10", "0.10-0.30", "0.30-0.70", "0.70-0.90", ">=0.90"]), True),
    "spread": (_bucket("spread_c", [1.5, 2.5, 5.5], ["1c", "2c", "3-5c", ">5c"]), True),
    "level": (_bucket("level_c", [-0.05, 0.05, 1.05], ["inside", "touch", "1c through", ">1c through"]), True),
    "gap_fair": (_bucket("h_fair", [-1.75, -0.5, 0.5, 1.5], ["<=-1.75c", "-1.75..-0.5c", "-0.5..0.5c", "0.5..1.5c", ">1.5c"]), True),
    "btc5_adv": (_bucket("btc5_adv", [-1, -0.25, 0.25, 1], ["<-1σ", "-1..-0.25σ", "±0.25σ", "0.25..1σ", ">1σ"]), True),
    "imb_adv": (_bucket("imb_adv", [0.2, 0.4, 0.6, 0.8], ["<0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8", ">=0.8"]), True),
    "flow5_adv": (_bucket("flow5_adv", [-0.5, 0.5], ["for maker", "none", "against maker"]), True),
    "hour_utc": (((pl.col("hour") // 4) * 4).cast(pl.Int64).cast(pl.String).str.zfill(2), True),
    "size": (_bucket("size", [10, 50, 200, 1000], ["<10", "10-50", "50-200", "200-1000", ">=1000"]), False),
    "size_vs_touch": (_bucket("size_vs_touch", [0.25, 1.0], ["<25% of touch", "25-100%", ">touch"]), False),
    "sweep": (pl.when(pl.col("order_levels") > 1).then(pl.lit("multi-level")).otherwise(pl.lit("one level")), False),
}


def slices(f: pl.DataFrame, tstar: dict[str, int], names=None) -> pl.DataFrame:
    """Per family × slice value at τ*: h, M(τ*), a(τ*), M + rebate; count- and size-weighted."""
    out = []
    for fam in FAMS:
        k = tstar.get(fam, 60)
        d = f.filter(pl.col("family") == fam).with_columns(
            pl.col(f"M{k}_fair").alias("M"), (pl.col(f"M{k}_fair") + pl.col("rebate_c")).alias("M_reb"), pl.col(f"a{k}_fair").alias("a"))
        d = d.filter(pl.col("M").is_not_null())
        for name, (expr, exante) in SLICES.items():
            if names and name not in names:
                continue
            dd = d.with_columns(expr.alias("value")).filter(pl.col("value").is_not_null())
            if not dd.height:
                continue
            t = wide(grouped_mean(dd, ["h_fair", "M", "a", "M_reb"], ["value"]), ["value"])
            ts = grouped_mean(dd, ["M"], ["value"], weight="size").select("value", pl.col("mean").alias("M_size"), pl.col("se").alias("M_size_se"))
            out.append(t.join(ts, on="value", how="left").with_columns(pl.lit(fam).alias("family"), pl.lit(name).alias("slice"),
                                                                      pl.lit(exante).alias("ex_ante"), pl.lit(k).alias("tau")))
    return pl.concat(out, how="diagonal_relaxed").sort("family", "slice", "value") if out else pl.DataFrame()


# ---- A4: conditional model ----------------------------------------------------------------------

FEATS = ["h_fair", "h_mid", "spread_c", "level_c", "imb_adv", "log_q_touch", "btc5_adv", "flow5_adv_s", "abs_x", "min_frac", "last2min", "log_tte"]


def features(f: pl.DataFrame) -> pl.DataFrame:
    """Ex-ante design columns (see SLICES on why fill characteristics are excluded)."""
    x = (pl.col("P").clip(0.001, 0.999) / (1 - pl.col("P").clip(0.001, 0.999))).log()
    return f.with_columns(
        (pl.col("q_touch").fill_null(0) + 1).log().alias("log_q_touch"),
        (pl.col("flow5_adv").sign() * pl.col("flow5_adv").abs().log1p()).alias("flow5_adv_s"),
        x.abs().alias("abs_x"),
        pl.when(pl.col("family") == "btc_15m").then(pl.col("minute") / 15.0).otherwise(0.0).alias("min_frac"),
        pl.when((pl.col("family") == "btc_15m") & (pl.col("tte_s") < 120)).then(1.0).otherwise(0.0).alias("last2min"),
        (pl.col("tte_s").clip(1, None)).log().alias("log_tte"),
    )


def _ridge_fit(X: np.ndarray, y: np.ndarray, lam_rel: float) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    mu, sd = X.mean(0), X.std(0)
    sd[sd == 0] = 1
    Z = np.column_stack([np.ones(len(X)), (X - mu) / sd])
    P = np.eye(Z.shape[1]) * lam_rel * len(X)
    P[0, 0] = 0  # never shrink the intercept
    beta = np.linalg.solve(Z.T @ Z + P, Z.T @ y)
    return beta, mu, sd


def _ridge_pred(X: np.ndarray, beta, mu, sd) -> np.ndarray:
    return np.column_stack([np.ones(len(X)), (X - mu) / sd]) @ beta


def walk_forward(f: pl.DataFrame, dates: list[str], tstar: dict[str, int], lam_rel: float = 0.01, min_train: int = 3000) -> tuple[pl.DataFrame, dict]:
    """Predict the edge per fill, M̂ = E[M(τ*) | z] (equivalently â = h − M̂), training only on
    days before the test day. Three scorers, so the model has to beat something:
        const   the training mean (no information: every fill looks alike)
        gap     linear in h_fair only (the fair-value stale-quote signal, one number)
        ridge   all ex-ante features
    Families too thin to fit fall back to const. The target is winsorised at the training 1/99 %
    because one resolution jump can be 50 cents and would steer a least-squares fit alone."""
    f = features(f)
    preds, info = [], {}
    for i in range(1, len(dates)):
        train_days, d = dates[:i], dates[i]
        for fam in FAMS:
            k = tstar.get(fam, 60)
            tgt = f"M{k}_fair"
            ok = pl.all_horizontal([pl.col(c).is_not_null() & pl.col(c).is_finite() for c in FEATS + [tgt]])
            tr = f.filter(pl.col("date").is_in(train_days) & (pl.col("family") == fam) & ok)
            te = f.filter((pl.col("date") == d) & (pl.col("family") == fam) & ok)
            if not te.height:
                continue
            y = tr[tgt].to_numpy()
            if len(y):
                lo, hi = np.percentile(y, [1, 99])
                y = np.clip(y, lo, hi)
            const = float(y.mean()) if len(y) else 0.0
            p_const = np.full(te.height, const)
            if tr.height >= min_train:
                b1, m1, s1 = _ridge_fit(tr.select("h_fair").to_numpy(), y, lam_rel)
                p_gap = _ridge_pred(te.select("h_fair").to_numpy(), b1, m1, s1)
                b, mu, sd = _ridge_fit(tr.select(FEATS).to_numpy(), y, lam_rel)
                p_ridge = _ridge_pred(te.select(FEATS).to_numpy(), b, mu, sd)
                info[f"{d}|{fam}"] = {"train": tr.height, "test": te.height, "tau": k, "intercept": round(float(b[0]), 3),
                                      "coef_per_sd": {n: round(float(c), 3) for n, c in zip(FEATS, b[1:])}}
            else:
                p_gap = p_ridge = p_const
                info[f"{d}|{fam}"] = {"train": tr.height, "test": te.height, "tau": k, "fallback": "const"}
            preds.append(te.select("date", "family", "cluster", "size", "h_fair", "rebate_c", pl.col(tgt).alias("M"), pl.col(f"a{k}_fair").alias("a"),
                                   pl.col("M60_fair").alias("M60"))
                         .with_columns(pl.Series("pred_const", p_const), pl.Series("pred_gap", p_gap), pl.Series("pred_ridge", p_ridge)))
    return (pl.concat(preds, how="diagonal_relaxed") if preds else pl.DataFrame()), info


def final_params(f: pl.DataFrame, tstar: dict[str, int], lam_rel: float = 0.01, min_train: int = 3000) -> dict:
    """Ridge and gap fits on ALL days, per family and horizon (τ* and 60 s): what stage 5/7 read
    to turn a book state into â_side(z) = h − M̂. Same standardisation as walk_forward."""
    f = features(f)
    out = {"features": FEATS, "lam_rel": lam_rel, "families": {}}
    for fam in FAMS:
        for k in sorted({tstar.get(fam, 60), 60}):
            tgt = f"M{k}_fair"
            ok = pl.all_horizontal([pl.col(c).is_not_null() & pl.col(c).is_finite() for c in FEATS + [tgt]])
            d = f.filter((pl.col("family") == fam) & ok)
            if d.height < min_train:
                continue
            y = d[tgt].to_numpy()
            lo, hi = np.percentile(y, [1, 99])
            y = np.clip(y, lo, hi)
            b, mu, sd = _ridge_fit(d.select(FEATS).to_numpy(), y, lam_rel)
            bg, mg, sg = _ridge_fit(d.select("h_fair").to_numpy(), y, lam_rel)
            out["families"][f"{fam}|{k}"] = {"n": d.height, "tau": k,
                                             "ridge": {"beta": b.tolist(), "mu": mu.tolist(), "sd": sd.tolist()},
                                             "gap": {"beta": bg.tolist(), "mu": mg.tolist(), "sd": sg.tolist()}}
    return out


def decision_curve(p: pl.DataFrame, fracs=tuple(np.round(np.arange(0, 0.95, 0.05), 2))) -> pl.DataFrame:
    """Drop the fills each scorer likes least, keep the rest: what is the kept fills' realized edge?
    Ties (const) are broken at random, which is what 'no information' means.
    Why rank within each held-out day: every day has its own walk-forward fit, so the
    predictions of different days carry different intercepts (const was −0.05 on 11-07 and
    −0.19 on 11-08). Ranking the pooled days drops whole days by their intercept, which made
    the 'no information' line throw away the good day first (fixed 2026-11-09). A live rule
    also only compares fills of the same day against each other."""
    rng = np.random.default_rng(0)
    out = []
    for fam in p["family"].unique().to_list():
        d = p.filter(pl.col("family") == fam)
        M = d["M"].to_numpy() + d["rebate_c"].to_numpy()
        sz = d["size"].to_numpy()
        day = d["date"].to_numpy() if "date" in d.columns else np.zeros(d.height)
        for sc in ("pred_const", "pred_gap", "pred_ridge"):
            score = d[sc].to_numpy() + rng.uniform(0, 1e-9, d.height)
            # within-day rank in [0, 1): 0 = the fill this scorer likes best that day
            pct = np.empty(d.height)
            for u in np.unique(day):
                ix = np.flatnonzero(day == u)
                pct[ix[np.argsort(-score[ix])]] = np.arange(ix.size) / ix.size
            order = np.argsort(pct, kind="stable")
            for q in fracs:
                keep = order[: max(1, int(round(len(order) * (1 - q))))]
                out.append({"family": fam, "scorer": sc[5:], "drop": float(q), "kept": len(keep),
                            "edge_c": float(M[keep].mean()), "edge_size_c": float((M[keep] * sz[keep]).sum() / sz[keep].sum()),
                            "pnl_usd": float((M[keep] * sz[keep]).sum() / 100)})
    return pl.DataFrame(out)


def calibration(p: pl.DataFrame, col: str = "pred_ridge", bins: int = 10) -> pl.DataFrame:
    return (p.with_columns(pl.col(col).qcut(bins, labels=[str(i) for i in range(bins)], allow_duplicates=True).alias("bin"))
            .group_by("family", "bin").agg(pl.col(col).mean().alias("pred"), pl.col("M").mean().alias("obs"), pl.len().alias("n"))
            .sort("family", "bin"))


def model_metrics(p: pl.DataFrame) -> pl.DataFrame:
    """Out-of-sample R² of the edge vs the constant, per family and scorer."""
    rows = []
    for fam in p["family"].unique().to_list():
        d = p.filter(pl.col("family") == fam)
        y = d["M"].to_numpy()
        base = ((y - d["pred_const"].to_numpy()) ** 2).mean()
        for sc in ("pred_gap", "pred_ridge"):
            e = ((y - d[sc].to_numpy()) ** 2).mean()
            rows.append({"family": fam, "scorer": sc[5:], "n": len(y), "r2_vs_const": 1 - e / base if base > 0 else float("nan"),
                         "corr": float(np.corrcoef(y, d[sc].to_numpy())[0, 1]) if np.std(d[sc].to_numpy()) > 0 else float("nan")})
    return pl.DataFrame(rows)


# ---- A5: Gate A ----------------------------------------------------------------------------------

SCORERS = ("gap", "ridge")
PRIMARY = "gap"  # the pre-declared Gate-A scorer: one number (the fair-value gap), nothing to overfit


def _row(t: pl.DataFrame) -> dict:
    return t.row(0, named=True) if t.height else {}


def _pi(d: pl.DataFrame, col: str) -> dict:
    """π, SE, n, clusters of one column over a set of fills."""
    if not d.height:
        return {}
    return _row(wide(grouped_mean(d.with_columns(pl.lit(1).alias("k")), [col], ["k"]), ["k"]))


def gate(f: pl.DataFrame, p: pl.DataFrame, dates: list[str], tstar: dict[str, int], min_n: int = 300) -> dict:
    """Is there an EX-ANTE selection of fills where the captured half-spread beats adverse
    selection, π = h̄ − ā(τ*) = M̄(τ*) > 0? Three tests, all scored on held-out days:
      1. unconditional, per family and day (does the average maker make money?)
      2. single ex-ante slices SELECTED on the training days (π − 2 SE > 0, n ≥ min_n), then
         scored on the next day. Why select-then-confirm: ~50 slice values × 3 families × 2
         metrics means a few look positive by luck; only a slice that is selected on every fold
         AND positive at 95 % on every held-out day counts as confirmed.
      3. the fills a scorer keeps (predicted edge > 0), per held-out day
    Each with and without the maker rebate, at τ* and at 60 s. Liquidity rewards are stage 6."""
    out: dict = {"unconditional": [], "slices": [], "model": []}
    test_days = dates[1:]
    for fam in FAMS:
        k = tstar.get(fam, 60)
        d = f.filter(pl.col("family") == fam).with_columns(
            pl.col(f"M{k}_fair").alias("M"), (pl.col(f"M{k}_fair") + pl.col("rebate_c")).alias("M_reb"),
            pl.col("M60_fair").alias("M60"), (pl.col("M60_fair") + pl.col("rebate_c")).alias("M60_reb"))
        d = d.filter(pl.col("M").is_not_null())
        for scope, dd in [("all days", d)] + [(t, d.filter(pl.col("date") == t)) for t in dates]:
            r = {c: _pi(dd, c) for c in ("h_fair", "M", "M_reb", "M60", "M60_reb")}
            if r["M"]:
                out["unconditional"].append({"family": fam, "scope": scope, "tau": k, "n": r["M"]["n"], "n_clusters": r["M"]["n_clusters"],
                                             "h_fair": r["h_fair"].get("h_fair"),
                                             **{c: r[c].get(c) for c in ("M", "M_reb", "M60", "M60_reb")},
                                             **{c + "_se": r[c].get(c + "_se") for c in ("M", "M_reb", "M60", "M60_reb")}})
        for i, tday in enumerate(test_days, start=1):
            train = d.filter(pl.col("date").is_in(dates[:i]))
            test = d.filter(pl.col("date") == tday)
            for name, (expr, exante) in SLICES.items():
                if not exante:
                    continue
                trv = train.with_columns(expr.alias("value")).filter(pl.col("value").is_not_null())
                tev = test.with_columns(expr.alias("value")).filter(pl.col("value").is_not_null())
                if trv["value"].n_unique() < 2:
                    continue  # a constant "slice" is just the unconditional sample (e.g. btc5_adv on Fed)
                for col in ("M", "M_reb"):
                    tr = wide(grouped_mean(trv, [col], ["value"]), ["value"])
                    if not tr.height:
                        continue
                    sel = tr.filter((pl.col("n") >= min_n) & (pl.col(col) - 2 * pl.col(col + "_se") > 0))
                    for r in sel.iter_rows(named=True):
                        te = _pi(tev.filter(pl.col("value") == r["value"]), col)
                        te60 = _pi(tev.filter(pl.col("value") == r["value"]), "M60" + col[1:])
                        out["slices"].append({"family": fam, "test_day": tday, "slice": name, "value": r["value"], "metric": col, "tau": k,
                                              "train_n": r["n"], "train_pi": r[col], "train_se": r[col + "_se"],
                                              "test_n": te.get("n"), "test_clusters": te.get("n_clusters"), "test_pi": te.get(col),
                                              "test_se": te.get(col + "_se"), "test_pi60": te60.get("M60" + col[1:]),
                                              "test_se60": te60.get("M60" + col[1:] + "_se")})
    if p.height:
        p = p.with_columns((pl.col("M") + pl.col("rebate_c")).alias("M_reb"), (pl.col("M60") + pl.col("rebate_c")).alias("M60_reb"))
        for fam in p["family"].unique().to_list():
            for day in p.filter(pl.col("family") == fam)["date"].unique().sort().to_list():
                base = p.filter((pl.col("family") == fam) & (pl.col("date") == day))
                for sc in SCORERS:
                    for metric, add in (("M", 0.0), ("M_reb", 1.0)):
                        kept = base.filter(pl.col("pred_" + sc) + add * pl.col("rebate_c") > 0)
                        r, r60 = _pi(kept, metric), _pi(kept, "M60" + metric[1:])
                        out["model"].append({"family": fam, "test_day": day, "scorer": sc, "metric": metric,
                                             "kept_share": round(kept.height / max(base.height, 1), 4), "n": r.get("n"),
                                             "n_clusters": r.get("n_clusters"), "pi": r.get(metric), "se": r.get(metric + "_se"),
                                             "pi60": r60.get("M60" + metric[1:]), "se60": r60.get("M60" + metric[1:] + "_se")})
    return out


def _pos(m, s, z: float = 1.96) -> bool:
    return m is not None and s is not None and not math.isnan(s) and m - z * s > 0


def verdict(G: dict, tstar: dict[str, int], test_days: list[str], fallback: set[str] | None = None, min_clusters: int = 10) -> tuple[str, list[str], dict]:
    """Per family: GO if an ex-ante selection is positive at 95 % on EVERY held-out day, both at τ*
    and at 60 s, without the rebate; GO (rebate-dependent) if only with it; INCONCLUSIVE if τ* is a
    fallback (no measurable decay inside 60 s, so a ≤ 60 s markout cannot see this family's
    adverse selection); else NO-GO. Overall = best family.
    Why also 60 s at 95 % (v1 used a 60 s point estimate): when τ* is short, M(τ*) is mostly the
    half-spread; a selection only counts if it still pays once the price has had a minute to move."""
    fallback = fallback if fallback is not None else {f for f in FAMS if tstar.get(f) == 60}
    why, fam_v = [], {}
    def ok(pi, se, pi60, se60, nc):
        return _pos(pi, se) and _pos(pi60, se60) and (nc or 0) >= min_clusters
    for fam in FAMS:
        v = "NO-GO"
        for metric, label in (("M", "GO"), ("M_reb", "GO (rebate-dependent)")):
            hits = []
            sl = [r for r in G["slices"] if r["family"] == fam and r["metric"] == metric]
            for key in sorted({(r["slice"], r["value"]) for r in sl}):
                rows = {r["test_day"]: r for r in sl if (r["slice"], r["value"]) == key}
                if all(t in rows and ok(rows[t]["test_pi"], rows[t]["test_se"], rows[t]["test_pi60"], rows[t].get("test_se60"), rows[t]["test_clusters"])
                       for t in test_days):
                    hits.append(f"{fam} slice {key[0]}={key[1]}: " + "; ".join(
                        f"{t} π {rows[t]['test_pi']:+.2f}¢ ± {rows[t]['test_se']:.2f}, 60 s {rows[t]['test_pi60']:+.2f} ± {rows[t]['test_se60']:.2f} (n {rows[t]['test_n']})"
                        for t in test_days))
            md = {r["test_day"]: r for r in G["model"] if r["family"] == fam and r["metric"] == metric and r["scorer"] == PRIMARY}
            if all(t in md and ok(md[t]["pi"], md[t]["se"], md[t]["pi60"], md[t]["se60"], md[t]["n_clusters"]) for t in test_days):
                hits.append(f"{fam} fills the '{PRIMARY}' scorer keeps: " + "; ".join(
                    f"{t} keep {md[t]['kept_share']:.0%}, π {md[t]['pi']:+.2f}¢ ± {md[t]['se']:.2f}" for t in test_days))
            if hits:
                v = label
                why += [f"[{label}] {h}" for h in hits]
                break
        if fam in fallback:
            # Why INCONCLUSIVE even when a slice passes: with no measurable decay inside 60 s the
            # 60 s markout is mostly the half-spread itself; this family's adverse selection (if
            # any) arrives over minutes to hours, which the short-horizon gate cannot see.
            why.append(f"[{fam} → INCONCLUSIVE] no significant decay inside 60 s on the training day (τ* is a fallback); "
                       f"short-horizon result was {v}; see the 900/3600 s and resolution markouts")
            v = "INCONCLUSIVE"
        fam_v[fam] = v
    order = ["GO", "GO (rebate-dependent)", "INCONCLUSIVE", "NO-GO"]
    overall = min(fam_v.values(), key=order.index)
    return overall, why, fam_v

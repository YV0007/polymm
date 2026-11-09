"""F2–F5: the candidate fair values, each as log-odds of the YES price.

    x_mid       the mid (the baseline every candidate must beat)
    x_micro     microprice: P_ask·Q_bid/(Q_bid+Q_ask) + P_bid·Q_ask/(Q_bid+Q_ask)
    x_stoikov   x_mid + E[x_mid(t+h) − x_mid(t) | imbalance bucket, spread bucket], the table
                fitted on EARLIER days only (Stoikov's micro-price idea, empirical version)
    x_model     BTC digital: p = Φ(d), d = ln(E[A_T]/K_binance) / (σ √τ_eff)
                15m: A_T is the 60 s settlement average, K moved to the Binance scale with
                     the gap measured at window start; ladder: A_T = S_T, K = strike
    x_fed       Fed: microprices rescaled so the five outcomes sum to 1

Oracle variants of x_model replace one input by its true value (realized σ, true end gap,
zero-latency spot, spot one second ahead). They use the future: their score is an upper
bound on what perfecting that input could buy, never a usable fair value.
"""
from __future__ import annotations

import math

import numpy as np
import polars as pl

from .inputs import P_CLIP, logit

SETTLE_AVG_S = 60.0  # 15m settles on a 60 s TWAP


def norm_cdf(x: np.ndarray) -> np.ndarray:
    """Φ via erf (Abramowitz–Stegun 7.1.26, |error| < 1.5e-7). Why not scipy: one function
    does not justify a dependency, and 1e-7 is far below a 0.1¢ tick."""
    x = np.asarray(x, float)
    z = np.abs(x) / math.sqrt(2.0)
    t = 1.0 / (1.0 + 0.3275911 * z)
    poly = t * (0.254829592 + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))))
    erf = 1.0 - poly * np.exp(-z * z)
    out = 0.5 * (1.0 + np.sign(x) * erf)
    return np.where(np.isnan(x), np.nan, out)


def _phi_logodds_np(d: np.ndarray) -> np.ndarray:
    """log-odds of Φ(d), with the same clip as `logit`. Why numpy on the eager frame and not a
    polars UDF: several UDFs in one with_columns run in parallel threads, and polars' lazy numpy
    import is not thread-safe there (a run on 2026-11-09 died with "'module' object does not
    support item assignment"). A plain column-by-column loop has no threads to race."""
    p = np.clip(norm_cdf(d), P_CLIP, 1 - P_CLIP)
    return np.log(p / (1 - p))


def tau_eff_15m(tau: pl.Expr) -> pl.Expr:
    """Variance-equivalent time left when the payoff is the average of S over the last 60 s.
    τ > 60: τ − 40 (the averaging removes 2/3 of the last minute's variance);
    τ ≤ 60: τ³ / (3·60²) (only the part of the average still in the future is random).
    Both pieces give 20 s at τ = 60, so the model does not jump."""
    return pl.when(tau > SETTLE_AVG_S).then(tau - 40.0).otherwise(tau ** 3 / (3 * SETTLE_AVG_S ** 2))


# ---- F2: book ------------------------------------------------------------------------------


def add_book(g: pl.DataFrame) -> pl.DataFrame:
    I = pl.col("bid_sz") / (pl.col("bid_sz") + pl.col("ask_sz"))
    return g.with_columns(
        logit(pl.col("micro")).alias("x_micro"),
        I.alias("imb"),
        (I * 10).floor().clip(0, 9).cast(pl.Int8).alias("imb_b"),
        # spread bucket in cents; "0" = sub-cent (tick 0.001 near the extremes)
        pl.when(pl.col("spread") < 0.0095).then(0).otherwise((pl.col("spread") * 100).round().clip(1, 3)).cast(pl.Int8).alias("spr_b"),
    )


def fit_stoikov(train: pl.DataFrame, h: int = 5, n0: float = 200.0) -> pl.DataFrame:
    """Mean next-h-seconds log-odds mid move per (family, spread bucket, imbalance bucket).
    Why shrink by n/(n+n0): a bucket seen 30 times should not move the fair value as much
    as one seen 30,000 times."""
    return (train.filter(pl.col(f"x_mid_f{h}").is_not_null())
            .group_by("family", "spr_b", "imb_b")
            .agg((pl.col(f"x_mid_f{h}") - pl.col("x_mid")).mean().alias("adj_raw"), pl.len().alias("n"))
            .with_columns((pl.col("adj_raw") * pl.col("n") / (pl.col("n") + n0)).alias("adj")))


def apply_stoikov(g: pl.DataFrame, table: pl.DataFrame) -> pl.DataFrame:
    return (g.join(table.select("family", "spr_b", "imb_b", "adj"), on=["family", "spr_b", "imb_b"], how="left")
            .with_columns((pl.col("x_mid") + pl.col("adj").fill_null(0.0)).alias("x_stoikov")).drop("adj"))


# ---- F3/F4: BTC digital ---------------------------------------------------------------------------


def _d_15m(S: str, A: str, sigma: str, basis: pl.Expr) -> pl.Expr:
    tau = pl.col("tau")
    ea = pl.when(tau <= SETTLE_AVG_S).then(((SETTLE_AVG_S - tau) * pl.col(A) + tau * pl.col(S)) / SETTLE_AVG_S).otherwise(pl.col(S))
    ln_sk = ea.log() - basis - pl.col("K").log()
    return ln_sk / (pl.col(sigma) * tau_eff_15m(tau).sqrt())


def _d_ladder(S: str, sigma: str) -> pl.Expr:
    return (pl.col(S) / pl.col("K")).log() / (pl.col(sigma) * pl.col("tau").sqrt())


def add_models(g: pl.DataFrame) -> pl.DataFrame:
    """x_model plus oracle variants for the BTC families; null for the Fed."""
    is15 = pl.col("family") == "btc_15m"
    isL = pl.col("family") == "btc_daily_ladder"
    b0 = pl.col("basis0")
    # true gap path: linear from the start gap to the end gap (oracle)
    b_lin = b0 + (pl.col("basis_T") - b0) * (pl.col("sec") - pl.col("t_start_s")) / (pl.col("t_end_s") - pl.col("t_start_s"))

    def both(d15: pl.Expr, dL: pl.Expr, name: str) -> pl.Expr:
        return pl.when(is15).then(d15).when(isL).then(dL).otherwise(None).alias(name)

    g = g.with_columns(
        both(_d_15m("S", "A_known", "sigma_s", b0), _d_ladder("S", "sigma_s"), "d"),
        both(_d_15m("S", "A_known", "rv30_s", b0), pl.lit(None, pl.Float64), "d_or_sigma"),
        both(_d_15m("S", "A_known", "sigma_s", b_lin), pl.lit(None, pl.Float64), "d_or_basis"),
        both(_d_15m("S_zero", "A_known_zero", "sigma_s", b0), _d_ladder("S_zero", "sigma_s"), "d_or_zero"),
        both(_d_15m("S_lead1", "A_known", "sigma_s", b0), _d_ladder("S_lead1", "sigma_s"), "d_or_lead1"),
    )
    pairs = (("d", "x_model"), ("d_or_sigma", "x_or_sigma"), ("d_or_basis", "x_or_basis"),
             ("d_or_zero", "x_or_zero"), ("d_or_lead1", "x_or_lead1"))
    return g.with_columns([pl.Series(x, _phi_logodds_np(g[dc].cast(pl.Float64).to_numpy()), nan_to_null=True) for dc, x in pairs])


# ---- F6 helper: anchor the model to the book's level ------------------------------------------------


MODEL_COLS = ("x_model", "x_or_sigma", "x_or_basis", "x_or_zero", "x_or_lead1")


def add_anchor(g: pl.DataFrame, book: str, model: str, half_life_s: int, out: str) -> pl.DataFrame:
    """x_anch = x_model + EWMA_h(x_book − x_model), per token, time-aware on our 1 s clock.

    Why (found 2026-11-08): the model's CHANGES lead the mid (a ≥ 0.02 log-odds model move in
    the last second is followed by a same-direction mid move within 5 s 72 % of the time),
    but its LEVEL sits a median 0.35 log-odds off the market (σ, Chainlink gap, risk
    pricing). The EWMA offset carries the book's level; what remains of the model is the
    BTC news the book has not priced yet. A random-walk-offset Kalman filter, simplest form.
    The offset at t uses values at t and before only (ewm is causal)."""
    off = (pl.col(book) - pl.col(model)).ewm_mean_by("sec", half_life=f"{half_life_s}i").over("asset_id")
    return g.sort("asset_id", "sec").with_columns((pl.col(model) + off).alias(out))


# ---- F4: ladder monotonicity ------------------------------------------------------------------------


def pav_decreasing(y: np.ndarray, w: np.ndarray | None = None) -> np.ndarray:
    """Pool-adjacent-violators: the closest (least squares) non-increasing sequence to y."""
    y = np.asarray(y, float)
    w = np.ones_like(y) if w is None else np.asarray(w, float)
    vals, wts, cnt = [], [], []
    for yi, wi in zip(y, w):
        vals.append(yi); wts.append(wi); cnt.append(1)
        while len(vals) > 1 and vals[-2] < vals[-1]:  # violation of non-increasing
            v = (vals[-2] * wts[-2] + vals[-1] * wts[-1]) / (wts[-2] + wts[-1])
            wsum, c = wts[-2] + wts[-1], cnt[-2] + cnt[-1]
            vals[-2:], wts[-2:], cnt[-2:] = [v], [wsum], [c]
    return np.repeat(vals, cnt)


def ladder_monotone(g: pl.DataFrame, col: str, out: str) -> tuple[pl.DataFrame, dict]:
    """Make the ladder prices non-increasing in strike at every second (in probability space),
    and count how often the raw column violated it by more than half a cent."""
    L = g.filter(pl.col("family") == "btc_daily_ladder").select("asset_id", "sec", "event_slug", "K", col).drop_nulls(col)
    if not L.height:
        return g.with_columns(pl.col(col).alias(out)), {"groups": 0}
    L = L.with_columns((1 / (1 + (-pl.col(col)).exp())).alias("_p")).sort("event_slug", "sec", "K")
    p = L["_p"].to_numpy()
    keys = (L["event_slug"] + "|" + L["sec"].cast(pl.String)).to_numpy()
    fixed = p.copy()
    viol = 0
    starts = np.flatnonzero(np.r_[True, keys[1:] != keys[:-1]])
    ends = np.r_[starts[1:], len(p)]
    for a, b in zip(starts, ends):
        if b - a < 2:
            continue
        seg = p[a:b]
        if np.any(np.diff(seg) > 0.005):
            viol += 1
        if np.any(np.diff(seg) > 0):
            fixed[a:b] = pav_decreasing(seg)
    L = L.with_columns(logit(pl.Series(fixed)).alias(out))
    g = g.join(L.select("asset_id", "sec", out), on=["asset_id", "sec"], how="left") \
        .with_columns(pl.coalesce(pl.col(out), pl.col(col)).alias(out))
    return g, {"groups": len(starts), "groups_violating_0.5c": viol}


# ---- F5: Fed -------------------------------------------------------------------------------------


def add_fed(g: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    """Rescale the Fed microprices to sum to 1, only at seconds where all outcomes are in a
    clean window (a partial sum would be meaningless)."""
    F = g.filter(pl.col("family") == "fed_dec_2026")
    if not F.height:
        return g.with_columns(pl.lit(None, pl.Float64).alias("x_fed")), {}
    n_mk = F["condition_id"].n_unique()
    s = F.group_by("sec").agg(pl.col("micro").sum().alias("_sum"), pl.len().alias("_n"))
    full = s.filter(pl.col("_n") == n_mk)
    g = g.join(full.select("sec", "_sum"), on="sec", how="left").with_columns(
        pl.when(pl.col("family") == "fed_dec_2026").then(logit(pl.col("micro") / pl.col("_sum"))).otherwise(None).alias("x_fed")
    ).drop("_sum")
    stats = {"markets": n_mk, "seconds_complete": full.height, "seconds_total": s.height,
             "sum_mean": float(full["_sum"].mean()) if full.height else None,
             "sum_p01": float(full["_sum"].quantile(0.01)) if full.height else None,
             "sum_p99": float(full["_sum"].quantile(0.99)) if full.height else None}
    return g, stats


def p_of(x: pl.Expr) -> pl.Expr:
    return 1 / (1 + (-x).exp())


__all__ = ["add_book", "fit_stoikov", "apply_stoikov", "add_models", "ladder_monotone", "add_fed", "tau_eff_15m",
           "norm_cdf", "pav_decreasing", "p_of", "P_CLIP"]

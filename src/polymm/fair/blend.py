"""F6: x_fair = x_book + w · (x_model − x_book), with w fitted on EARLIER data only.

Least squares against where the mid goes τ seconds later:

    min_w Σ (y − w z)²,   y = x_mid(t+τ) − x_book(t),   z = x_model(t) − x_book(t)
    ŵ = Σ y z / Σ z²       the share of the model–book disagreement the market closes in τ

Three versions, each a step up in flexibility:
    w_bucket  ŵ per (regime bucket): the diagnostic table
    w_smooth  w = σ(β0 + β1·f1 + β2·f2), fitted by Gauss–Newton on the same loss; few
              parameters, so it does not overfit three days of data
    shrink    unconstrained x_mid(t+τ) = α x_model + γ x_book: α + γ < 1 means both inputs
              are overconfident and the fair value should be pulled toward 0.50
"""
from __future__ import annotations

import numpy as np
import polars as pl


def w_table(df: pl.DataFrame, model: str, book: str, tau: int, by: list[str]) -> pl.DataFrame:
    y = pl.col(f"x_mid_f{tau}") - pl.col(book)
    z = pl.col(model) - pl.col(book)
    d = df.filter(pl.col(f"x_mid_f{tau}").is_not_null() & pl.col(model).is_finite() & pl.col(book).is_finite()) \
        .with_columns(y.alias("_y"), z.alias("_z"))
    t = d.group_by(by).agg(
        ((pl.col("_y") * pl.col("_z")).sum() / (pl.col("_z") ** 2).sum()).alias("w_hat"),
        pl.len().alias("n"), pl.col("cluster").n_unique().alias("n_clusters"),
        pl.col("_z").abs().median().alias("median_abs_gap"))
    # cluster-robust SE of the no-intercept slope
    d = d.join(t.select(by + ["w_hat"]), on=by)
    se = d.group_by(by + ["cluster"]).agg((pl.col("_z") * (pl.col("_y") - pl.col("w_hat") * pl.col("_z"))).sum().alias("_u"),
                                          (pl.col("_z") ** 2).sum().alias("_zz")) \
        .group_by(by).agg(((pl.col("_u") ** 2).sum().sqrt() / pl.col("_zz").sum()).alias("w_se"))
    return t.join(se, on=by).sort(by)


def _design(df: pl.DataFrame, feats: list[str]) -> np.ndarray:
    X = np.column_stack([np.ones(df.height)] + [df[f].cast(pl.Float64).to_numpy() for f in feats])
    return X


def fit_w_smooth(df: pl.DataFrame, model: str, book: str, tau: int, feats: list[str],
                 iters: int = 50, lam: float = 1e-3) -> np.ndarray:
    """Gauss–Newton (Levenberg-damped) for β in w = σ(Xβ), minimising Σ (y − σ(Xβ) z)².
    Features should be roughly unit scale (the caller standardises)."""
    d = df.filter(pl.col(f"x_mid_f{tau}").is_not_null() & pl.col(model).is_finite() & pl.col(book).is_finite()
                  & pl.all_horizontal([pl.col(f).is_not_null() for f in feats]))
    y = (d[f"x_mid_f{tau}"] - d[book]).to_numpy()
    z = (d[model] - d[book]).to_numpy()
    X = _design(d, feats)
    if len(y) < 100:
        return np.zeros(X.shape[1])
    # Why a ridge term on β (scaled to the data): with a small training set the sigmoid can
    # saturate and β run off to ±1e5 (it did on the first fold); a weak pull toward 0
    # (w = 0.5) keeps it finite and costs nothing when the data are informative.
    ridge = 1e-4 * float(np.sum(z * z))

    def loss_of(b):
        r_ = y - (1 / (1 + np.exp(-X @ b))) * z
        return float(r_ @ r_) + ridge * float(b @ b)

    beta = np.zeros(X.shape[1])
    loss = loss_of(beta)
    for _ in range(iters):
        w = 1 / (1 + np.exp(-X @ beta))
        r = y - w * z
        J = -(z * w * (1 - w))[:, None] * X  # ∂r/∂β
        A = J.T @ J + ridge * np.eye(len(beta))
        grad = J.T @ r + ridge * beta
        # Levenberg–Marquardt: accept a step only if it lowers the loss, else damp harder
        for _ in range(20):
            step = np.linalg.solve(A + lam * np.diag(np.diag(A) + 1e-12), -grad)
            new = loss_of(beta + step)
            if new < loss:
                break
            lam *= 10
        else:
            break
        done = loss - new < 1e-10 * max(loss, 1.0)
        beta, loss, lam = beta + step, new, max(lam / 10, 1e-6)
        if done:
            break
    return beta


def apply_w_smooth(df: pl.DataFrame, beta: np.ndarray, feats: list[str], model: str, book: str, out: str) -> pl.DataFrame:
    ok = pl.all_horizontal([pl.col(f).is_not_null() for f in feats]) & pl.col(model).is_finite()
    eta = pl.lit(float(beta[0]))
    for b, f in zip(beta[1:], feats):
        eta = eta + float(b) * pl.col(f).cast(pl.Float64)
    w = 1 / (1 + (-eta).exp())
    return df.with_columns(
        pl.when(ok).then(w).otherwise(0.0).alias(f"w_{out}"),
    ).with_columns(
        pl.when(ok).then(pl.col(book) + pl.col(f"w_{out}") * (pl.col(model) - pl.col(book))).otherwise(pl.col(book)).alias(out))


def shrink_check(df: pl.DataFrame, model: str, book: str, tau: int) -> dict:
    d = df.filter(pl.col(f"x_mid_f{tau}").is_not_null() & pl.col(model).is_finite() & pl.col(book).is_finite())
    if d.height < 100:
        return {}
    X = np.column_stack([d[model].to_numpy(), d[book].to_numpy()])
    y = d[f"x_mid_f{tau}"].to_numpy()
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    return {"alpha_model": float(coef[0]), "gamma_book": float(coef[1]), "sum": float(coef.sum()), "n": d.height}

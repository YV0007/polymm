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


# ---- ŵ table and shrink check, one day at a time (D-035) ---------------------------------------
# Same idea as score_cells/score_pooled: per day keep only what adds up across days.


def w_cells(df: pl.DataFrame, model: str, book: str, tau: int, by: list[str]) -> pl.DataFrame:
    """For `w_table` on one day: per (by, cluster) Σyz and Σz²; ŵ and its clustered SE are built
    from these (`w_table_pooled`)."""
    d = df.filter(pl.col(f"x_mid_f{tau}").is_not_null() & pl.col(model).is_finite() & pl.col(book).is_finite()) \
        .with_columns((pl.col(f"x_mid_f{tau}") - pl.col(book)).alias("_y"), (pl.col(model) - pl.col(book)).alias("_z"))
    return d.group_by(by + ["cluster"]).agg(pl.len().alias("n"), (pl.col("_y") * pl.col("_z")).sum().alias("syz"),
                                             (pl.col("_z") ** 2).sum().alias("szz"))


def abs_gaps(df: pl.DataFrame, model: str, book: str, keys: list[str], taus) -> pl.DataFrame:
    """For `w_table`'s median gap on one day: every |model − book| as float32, its regime keys as
    categories, and one flag per τ saying whether that row is in τ's sample.
    Why keep the values: a median does not add up across days, and a histogram would make it
    approximate. Why one frame for all τ and groupings, compact: the first version kept 12 copies
    with text keys (~0.4 GB a day, and growing); this is ~10 MB a day. float32 moves the median
    by < 1e-7 (relative)."""
    ok = pl.col(model).is_finite() & pl.col(book).is_finite()
    return df.filter(ok).select([pl.col(k).cast(pl.Categorical) for k in keys]
                                + [(pl.col(model) - pl.col(book)).abs().cast(pl.Float32).alias("absz")]
                                + [pl.col(f"x_mid_f{k}").is_not_null().alias(f"ok_{k}") for k in taus])


def w_table_pooled(cells: pl.DataFrame, gaps: pl.DataFrame, by: list[str], tau: int) -> pl.DataFrame:
    """`w_table` rebuilt from stacked `w_cells` and `abs_gaps` of several days."""
    cells = cells.group_by(by + ["cluster"]).agg(pl.col("n").sum(), pl.col("syz").sum(), pl.col("szz").sum())  # a cluster seen on 2 days counts once
    t = cells.group_by(by).agg((pl.col("syz").sum() / pl.col("szz").sum()).alias("w_hat"), pl.col("n").sum().cast(pl.UInt32),
                               pl.len().cast(pl.UInt32).alias("n_clusters"))
    med = (gaps.filter(pl.col(f"ok_{tau}")).group_by(by).agg(pl.col("absz").median().cast(pl.Float64).alias("median_abs_gap"))
           .with_columns([pl.col(b).cast(t.schema[b]) for b in by]))
    t = t.join(med, on=by, nulls_equal=True)
    # per cluster u = Σ z(y − ŵz) = Σyz − ŵ Σz²; SE = √Σu² / Σz²
    se = cells.join(t.select(by + ["w_hat"]), on=by, nulls_equal=True).group_by(by).agg(
        ((pl.col("syz") - pl.col("w_hat") * pl.col("szz")) ** 2).sum().sqrt().truediv(pl.col("szz").sum()).alias("w_se"))
    return t.join(se, on=by, nulls_equal=True).sort(by)


def shrink_part(df: pl.DataFrame, model: str, book: str, tau: int) -> tuple[int, np.ndarray | None]:
    """For `shrink_check` on one day: the row count and the 3×3 R of a QR of [x_model, x_book, y].
    Why QR and not ΣX'X: the two regressors are nearly collinear, and X'X squares that problem;
    stacking per-day R factors and taking one more QR gives the least-squares fit of all rows
    with lstsq's accuracy ("tall-skinny QR")."""
    d = df.filter(pl.col(f"x_mid_f{tau}").is_not_null() & pl.col(model).is_finite() & pl.col(book).is_finite())
    if not d.height:
        return 0, None
    A = np.column_stack([d[model].to_numpy(), d[book].to_numpy(), d[f"x_mid_f{tau}"].to_numpy()])
    return d.height, np.linalg.qr(A, mode="r")


def shrink_pooled(parts: list[tuple[int, np.ndarray | None]]) -> dict:
    n = sum(p[0] for p in parts)
    if n < 100:
        return {}
    R = np.linalg.qr(np.vstack([p[1] for p in parts if p[1] is not None]), mode="r")
    coef = np.linalg.solve(R[:2, :2], R[:2, 2])
    return {"alpha_model": float(coef[0]), "gamma_book": float(coef[1]), "sum": float(coef.sum()), "n": n}

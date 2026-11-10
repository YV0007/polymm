"""S5: the experiment harness. Grids, walk-forward selection, the fill-model band, the latency
sweep, the trial log and the deflated Sharpe ratio (D-028).

Walk-forward without refitting the engine: every grid setting is run on every day ONCE (pass 1);
selection for test day k then looks only at days < k. That is the same as fitting on days < k and
testing on k, and lets the trial log record every setting that was ever tried. The last day is
the LOCKBOX: never used to choose anything, reported once.

Unit of independence: one market window (BTC 15m: 96 a day). Per-window P&L is the series every
standard error and Sharpe ratio is computed on.
"""
from __future__ import annotations

import hashlib
import itertools
import json
import math
from pathlib import Path

import numpy as np
import polars as pl

from ..fair.models import norm_cdf
from ..queue.analysis import apply_policy, value
from . import engine as E

NS = 1_000_000_000
SIZE = 10.0
LAT_BASE = 75.0        # D-031: ~25 ms one way + 50 ms assumed venue time
MODEL_BASE = "matched"
LATS = (0.0, 30.0, 75.0, 128.0, 300.0, 1000.0)
MODELS = ("pess", "prop", "matched", "opt")


def _grid(rung: int, **axes) -> list[dict]:
    keys = list(axes)
    return [{"rung": rung, **dict(zip(keys, v))} for v in itertools.product(*axes.values())]


# Why these grids: each rung varies only what it adds, on a coarse grid around the values the
# earlier stages point to (h: 0.5–3 ¢ covers 1-tick to 6-tick quotes; jump thresholds 0.5–2 ¢ in
# 0.5 s bracket the fair-tick moves, p99 0.16 ¢ per tick). γ is set by the skew it produces: the
# median σ_p²·(T − t) on BTC 15m is 0.033 (p90 0.18), so γ = 0.001 / 0.003 / 0.01 lean the quotes
# by ~0.3 / 1 / 3 ¢ at 100 shares (a first grid at γ = 5–80 leaned them by dollars: found on the
# first day of the first run, 2026-11-10, and rerun). A coarse grid keeps the trial count small.
GRIDS = {
    0: _grid(0, h=[0.5, 1.0, 2.0, 3.0]),
    1: _grid(1, h=[0.5, 1.0, 2.0, 3.0], hyst=[0.0, 1.0]),
    2: _grid(2, h=[1.0, 2.0, 3.0], hyst=[1.0], gamma=[0.001, 0.003, 0.01], end_s=[0.0, 60.0]),
    3: _grid(3, h=[1.0, 2.0, 3.0], hyst=[1.0], gamma=[0.0, 0.003], end_s=[60.0], jump_c=[0.5, 1.0, 2.0], hold_ms=[1000.0, 3000.0]),
    4: _grid(4, v_thr=[0.0, 0.1, 0.25], hyst=[0.0], gamma=[0.0, 0.003], end_s=[60.0], jump_c=[1e9, 1.0]),
}


def cfg_id(c: dict) -> str:
    return hashlib.sha1(json.dumps(c, sort_keys=True).encode()).hexdigest()[:10]


def all_configs() -> list[dict]:
    out = []
    for r, g in GRIDS.items():
        for c in g:
            out.append(c | {"cfg": cfg_id(c)})
    return out


def prm_of(c: dict) -> np.ndarray:
    return E.params(size=SIZE, **{k: v for k, v in c.items() if k not in ("rung", "cfg")})


# ------------------------------------------------------------------------------------------------
# rung 4's value table: stage-5 probes of EARLIER days only
# ------------------------------------------------------------------------------------------------
def _minb(m: pl.Expr) -> pl.Expr:
    return pl.when(m <= 4).then(0).when(m <= 9).then(1).when(m <= 12).then(2).otherwise(3)


def value_table(train_dates: list[str], queue_root: Path) -> tuple[np.ndarray, np.ndarray]:
    """V̂[type, spread bucket, minute bucket] in ¢ per share per placement: 1{fill}·(M60 + rebate),
    stale policy, matched cancels (stage 5's value()). n = placements behind each cell."""
    vt, vn = np.zeros((3, 4, 4)), np.zeros((3, 4, 4))
    fs = [queue_root / d / "probes.parquet" for d in train_dates if (queue_root / d / "probes.parquet").exists()]
    if not fs:
        return vt, vn
    R = pl.concat([pl.read_parquet(f).filter((pl.col("family") == "btc_15m") & (pl.col("design") == "grid") & (pl.col("lat_ms") == 128))
                   for f in fs], how="diagonal_relaxed")
    R = apply_policy(R, "stale").with_columns(_minb(pl.col("minute")).alias("minb"))
    V = value(R, ["type", "spread_b", "minb"], models=("matched",), taus=(60,))
    ti = {t: i for i, t in enumerate(E.TYPES[:3])}
    si = {s: i for i, s in enumerate(E.SPREAD_B)}
    for r in V.iter_rows(named=True):
        if r["type"] in ti and r["spread_b"] in si and r["minb"] is not None:
            vt[ti[r["type"]], si[r["spread_b"]], int(r["minb"])] = r["mean"]
            vn[ti[r["type"]], si[r["spread_b"]], int(r["minb"])] = r["n"]
    return vt, vn


# ------------------------------------------------------------------------------------------------
# selection, statistics
# ------------------------------------------------------------------------------------------------
def window_stats(w: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    return (w.group_by(by).agg(pl.len().alias("windows"), pl.col("pnl").sum().alias("pnl"), pl.col("pnl").mean().alias("mean"),
                               (pl.col("pnl").std() / pl.len().sqrt()).alias("se"), (pl.col("pnl").mean() / pl.col("pnl").std()).alias("sharpe_w"),
                               pl.col("fills").sum().alias("fills"), pl.col("shares").sum().alias("shares"),
                               *[pl.col(c).sum().alias(c) for c in ("c1_spread", "c2_adverse", "c3_inventory", "c4_rebate", "c4_rewards")],
                               pl.col("pnl").min().alias("worst_window"), pl.col("q_peak").max().alias("q_peak"))
            .sort(by))


def select(W: pl.DataFrame, dates: list[str]) -> pl.DataFrame:
    """Per test day k ≥ 1 and rung: the setting with the best mean P&L per window on days < k.
    Returns (date, rung, cfg, fold) with fold = 'test' or 'lockbox' (the last day)."""
    rows = []
    for k in range(1, len(dates)):
        train = dates[:k]
        assert dates[k] not in train
        g = window_stats(W.filter(pl.col("date").is_in(train)), ["rung", "cfg"])
        for r in g["rung"].unique().sort().to_list():
            best = g.filter(pl.col("rung") == r).sort("mean", descending=True).row(0, named=True)
            rows.append({"date": dates[k], "rung": r, "cfg": best["cfg"], "train_mean": best["mean"],
                         "train_days": ",".join(train), "fold": "lockbox" if k == len(dates) - 1 else "test"})
    return pl.DataFrame(rows)


def deflated_sharpe(sr: float, n_obs: int, skew: float, kurt: float, sr_trials: np.ndarray) -> dict:
    """Bailey & López de Prado (2014). The best of N trials has an expected Sharpe above zero even
    with no edge:  SR0 = sd(SR_trials) · [(1 − γ) Φ⁻¹(1 − 1/N) + γ Φ⁻¹(1 − 1/(N e))],
    DSR = Φ( (SR − SR0) √(T − 1) / √(1 − skew·SR + (kurt − 1)/4 · SR²) ).
    SR, SR0 per window (not annualised); kurt is the raw (non-excess) kurtosis."""
    from statistics import NormalDist
    N = max(len(sr_trials), 2)
    g = 0.5772156649
    inv = NormalDist().inv_cdf
    sd = float(np.nanstd(sr_trials, ddof=1)) if len(sr_trials) > 1 else 0.0
    sr0 = sd * ((1 - g) * inv(1 - 1 / N) + g * inv(1 - 1 / (N * math.e)))
    den = 1 - skew * sr + (kurt - 1) / 4 * sr ** 2
    z = (sr - sr0) * math.sqrt(max(n_obs - 1, 1)) / math.sqrt(den) if den > 0 else float("nan")
    return {"sr": round(sr, 4), "sr0": round(sr0, 4), "trials": N, "n": n_obs, "dsr": round(float(norm_cdf(np.array([z]))[0]), 4) if z == z else None}


def moments(x: np.ndarray) -> tuple[float, float]:
    x = np.asarray(x, float)
    if len(x) < 3 or x.std() == 0:
        return 0.0, 3.0
    z = (x - x.mean()) / x.std()
    return float((z ** 3).mean()), float((z ** 4).mean())


def plateau(W: pl.DataFrame, train_days: list[str], rung: int, axes: list[str], configs: list[dict]) -> dict:
    """Is the best setting on a plateau or a spike? Neighbours = settings that differ by one grid
    step on one axis. Report the best's mean, its neighbours' mean, and the share of neighbours
    within one SE of the best (≥ 0.5 = plateau)."""
    g = window_stats(W.filter(pl.col("date").is_in(train_days) & (pl.col("rung") == rung)), ["cfg"])
    if not g.height:
        return {}
    cf = {c["cfg"]: c for c in configs if c["rung"] == rung}
    best = g.sort("mean", descending=True).row(0, named=True)
    b = cf[best["cfg"]]
    levels = {a: sorted({c[a] for c in cf.values()}) for a in axes}
    nb = []
    for c in cf.values():
        diff = [a for a in axes if c[a] != b[a]]
        if len(diff) == 1 and abs(levels[diff[0]].index(c[diff[0]]) - levels[diff[0]].index(b[diff[0]])) == 1 \
                and all(c[k] == b[k] for k in c if k not in axes and k != "cfg"):
            nb.append(c["cfg"])
    n = g.filter(pl.col("cfg").is_in(nb))
    if not n.height:
        return {"best": best["cfg"], "neighbours": 0}
    close = n.filter(pl.col("mean") >= best["mean"] - (best["se"] or 0))
    return {"best": best["cfg"], "best_mean": round(best["mean"], 4), "best_se": round(best["se"] or 0, 4), "neighbours": n.height,
            "neighbours_mean": round(float(n["mean"].mean()), 4), "within_1se_share": round(close.height / n.height, 3)}


def n_needed(mean: float, sd: float) -> float | None:
    """Windows needed to tell a per-window mean from zero at ~95 %: n ≥ (2σ/μ)²."""
    if mean == 0 or sd != sd:
        return None
    return round((2 * sd / mean) ** 2, 0)

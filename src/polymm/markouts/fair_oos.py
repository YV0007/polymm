"""The fair-value series the markouts use, rebuilt WALK-FORWARD.

Why not reports/fair/<date>/fair.parquet: that file is the final fit on ALL days (what the live
engine would use going forward). Its Stoikov table, anchor half-life and blend weights have seen
11-07 and 11-08, so a markout model that scores those days with it would get a small in-sample
head start: the fair gap would look more predictive than it can be live. Here each day d uses
parameters fitted on the days before it only; the first day (training only) uses its own fit.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import polars as pl

from ..fair.inputs import build_inputs, measure_latency_ms
from ..fair.run import apply_params, candidates, fit_params

log = logging.getLogger(__name__)
COLS = ["date", "family", "condition_id", "asset_id", "sec", "mid", "p_book", "p_fair", "S", "sigma_s"]


def build_fair_oos(dates: list[str], fair_root: Path, spot_root: Path, out: Path) -> dict[str, Path]:
    fit_tau = json.loads((fair_root / "fair_params.json").read_text()).get("fit_tau", 30)
    ref = pl.read_parquet(fair_root / "reference" / "markets_ref.parquet")
    lat = measure_latency_ms(spot_root)
    G = pl.concat([build_inputs(d, fair_root / d, spot_root, ref, lat) for d in dates], how="diagonal_relaxed")
    G, _ = candidates(G)
    sig = lambda c: 1 / (1 + (-pl.col(c)).exp())
    paths = {}
    for i, d in enumerate(dates):
        train = dates[:i] or [d]
        params = fit_params(G.filter(pl.col("date").is_in(train)), fit_tau)
        T, _ = apply_params(G.filter(pl.col("date") == d), params)
        p = out / d / "fair_oos.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        T.select("date", "family", "condition_id", "asset_id", "sec", "mid", sig("x_stoikov").alias("p_book"),
                 sig("x_fair").alias("p_fair"), "S", "sigma_s").write_parquet(p)
        log.info("fair_oos %s (params from %s): %d rows", d, train, T.height)
        paths[d] = p
    return paths

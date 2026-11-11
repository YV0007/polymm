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

from ..fair.walk import walk_forward

log = logging.getLogger(__name__)
COLS = ["date", "family", "condition_id", "asset_id", "sec", "mid", "p_book", "p_fair", "S", "sigma_s"]


def build_fair_oos(dates: list[str], fair_root: Path, spot_root: Path, out: Path) -> dict[str, Path]:
    """One day in memory at a time (fair/walk.py, D-035); same numbers as fitting on the stacked days."""
    fit_tau = json.loads((fair_root / "fair_params.json").read_text()).get("fit_tau", 30)
    sig = lambda c: 1 / (1 + (-pl.col(c)).exp())
    paths = {}

    def write(day) -> None:
        p = out / day.date / "fair_oos.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        day.fair.select("date", "family", "condition_id", "asset_id", "sec", "mid", sig("x_stoikov").alias("p_book"),
                        sig("x_fair").alias("p_fair"), "S", "sigma_s").write_parquet(p)
        log.info("fair_oos %s (params from %s): %d rows", day.date, day.train, day.fair.height)
        paths[day.date] = p

    walk_forward(dates, fair_root, spot_root, fit_tau, write, first_day="self")
    return paths

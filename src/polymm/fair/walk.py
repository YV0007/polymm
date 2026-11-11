"""Walk-forward fair value with only one day in memory at a time (D-035).

The old way built every day's full 1 s grid (2.6 M rows × 71 columns ≈ 1.9 GB a day), stacked
them into one table and copied it again for each fold: memory grew ~6 GB per day, and
`polymm fair` was killed by the OS from 5 days on (2026-11-11).

What the fit really needs from a past day is small:

    Stoikov table   per (family, spread, imbalance) bucket: Σ next-5 s move and a count
                    → a ~100-row table per day (`stoikov_sums`)
    anchor + w      the BTC rows only (15m + active ladder) and 13 columns
                    → ~0.9 M rows, ~85 MB a day (`FitStore.add`)

So `walk_forward` builds one day, applies the parameters fitted on the days before it, hands
the result to the caller (`on_day`), keeps the small pieces above and drops the day. Fitted
numbers are the same as before: the fit sees exactly the same rows, in the same order.

    for each day d:
        grid = inputs + candidates(d)                  (~1.3 GB after dropping build-only columns)
        params = fit(store)  ← days before d           (store: Stoikov sums + fit rows)
        fair = apply(grid, params) → on_day(...)       (caller writes / scores, then forgets it)
        store.add(grid)                                 (keeps ~85 MB of it)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import polars as pl

from .inputs import build_inputs, check_inputs, measure_latency_ms
from .models import stoikov_from_sums, stoikov_sums
from .run import FEATS, FL, apply_params, candidates, fit_params

log = logging.getLogger(__name__)

# Columns only needed to BUILD the candidates (raw book, spot plumbing, oracle inputs). Why drop
# them right after `candidates`: they are ~30 % of a day's memory and nothing downstream reads
# them (fair.parquet, scoring, markouts and sim keep none of them).
BUILD_ONLY = ["wid", "t", "tob_ts", "bid", "ask", "bid_sz", "ask_sz", "spread", "mid_f1", "mid_f5", "mid_f30", "mid_f60",
              "final_price", "S_ts", "S_age_s", "S_src", "S60", "S_zero", "S_lead1", "S60_start", "A_known_zero", "rv30_s",
              "basis_T", "sigma_age_s", "sigma_rv15_1s", "minute", "imb", "d_or_sigma", "d_or_basis", "d_or_zero", "d_or_lead1"]


@dataclass
class Day:
    date: str
    grid: pl.DataFrame            # inputs + candidates, nothing fitted
    fair: pl.DataFrame | None     # grid + x_stoikov, anchors, x_fair with `params`; None = training-only day
    params: dict | None
    train: list[str]              # the days `params` were fitted on
    mono: dict | None             # ladder monotonicity stats of the apply
    fed: dict                     # Fed Σp stats of this day
    checks: dict                  # check_inputs of this day


class FitStore:
    """What the fit needs from past days, and nothing else."""

    def __init__(self, fit_tau: int):
        self.fit_tau = fit_tau
        self.dates: list[str] = []
        self.rows: list[pl.DataFrame] = []
        self.st: list[pl.DataFrame] = []

    def cols(self) -> list[str]:
        return ["date", "family", "asset_id", "sec", "active", "x_mid", "spr_b", "imb_b", "x_model",
                f"x_mid_f{self.fit_tau}", *sorted({f for fs in FEATS.values() for f in fs})]

    def add(self, date: str, g: pl.DataFrame) -> None:
        # Why exactly these rows: fit_params anchors and blends only BTC rows with a finite model
        # (ladder: active books only). Why all of a row's seconds, even those without a future
        # mid: the anchor's EWMA runs through them, so dropping them would change the fit.
        keep = g.filter(pl.col("family").is_in(list(FEATS)) & pl.col("x_model").is_finite()
                        & ((pl.col("family") != FL) | pl.col("active")))
        # Why Categorical: the 78-digit token id as text is ~90 bytes a row, as a category 4.
        # It sorts by the text (checked), so add_anchor's row order is unchanged.
        self.rows.append(keep.select(self.cols()).with_columns(pl.col("asset_id").cast(pl.Categorical)))
        self.st.append(stoikov_sums(g))
        self.dates.append(date)

    def fit(self) -> dict:
        rows = pl.concat(self.rows)
        return fit_params(rows, self.fit_tau, stoikov=stoikov_from_sums(pl.concat(self.st)))

    def nbytes(self) -> int:
        return sum(r.estimated_size() for r in self.rows)


def walk_forward(dates: list[str], fair_root: Path, spot_root: Path, fit_tau: int, on_day: Callable[[Day], None], *,
                 first_day: str = "skip", ref: pl.DataFrame | None = None, latency_ms: float | None = None) -> FitStore:
    """Day d gets parameters fitted on the days before it.

    first_day: "skip" = the first day is training only (fair=None; `polymm fair` scores it nowhere);
               "self" = it is fitted on itself (markouts and sim need a series for every day).
    Returns the store with all days, for a final fit (`store.fit()`).

    Why a callback and not a list of days: the caller's work on day d (write a file, score it)
    ends when the callback returns, so nothing keeps day d's 1–2 GB alive while d+1 is built."""
    ref = ref if ref is not None else pl.read_parquet(fair_root / "reference" / "markets_ref.parquet")
    latency_ms = latency_ms if latency_ms is not None else measure_latency_ms(spot_root)
    store = FitStore(fit_tau)
    for i, d in enumerate(dates):
        g = build_inputs(d, fair_root / d, spot_root, ref, latency_ms)
        checks = check_inputs(g)  # before the drop: it reads tob_ts / S_ts / sigma_age_s
        g, fed = candidates(g)
        g = g.drop([c for c in BUILD_ONLY if c in g.columns])
        if i == 0 and first_day == "self":
            store.add(d, g)
        train = list(store.dates)
        fair = params = mono = None
        if train:
            params = store.fit()
            fair, mono = apply_params(g, params)
        if not (i == 0 and first_day == "self"):
            store.add(d, g)
        log.info("walk-forward %s: params from %s; %d rows; fit store %.0f MB", d, train, g.height, store.nbytes() / 2**20)
        on_day(Day(d, g, fair, params, train, mono, fed, checks))
        del g, fair  # Why: free the day before the next build_inputs, not after it
    return store

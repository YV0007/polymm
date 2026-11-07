"""Volatility (step 5): the σ inputs for the fair-value model and the inventory-risk numbers.

`polymm vol --date D --data ROOT [--spot-data ROOT2] [--replay-out DIR] --out reports/D/vol`

Two sides, one module:

  BTC (Binance)        realized / EWMA / seasonal σ of BTC/USDT, forecast quality at the
                       15-minute horizon, jumps, and the 1 s σ(t) series step 6 reads
                       (`sigma_btc.parquet`).
  Polymarket           σ of the mid in log-odds x = ln(p/(1−p)) per family and horizon,
                       the σ_p ≈ p(1−p)σ_x check, jump rates, and for btc_15m the σ the
                       market implies vs what BTC realized.

Which series feeds which number (the question every table must answer):

  spot_trades_hist     every Binance trade of a finished UTC day (exchange clock, µs).
                       → 1 s last-trade series and 1-min OHLC bars: realized σ, EWMA,
                         hour-of-day seasonality, 15-minute move distribution, forecast
                         scores, 1-min jumps. Three full days so far.
  spot_trades (live)   same trades from our WebSocket (exchange clock, ms) for days not yet
                       in the history → stitched onto the history 1-min bars (history wins
                         where both exist).
  spot_quotes (live)   Binance bookTicker with OUR receive clock → 1 s mid series: the
                       noise check (mid vs last trade), 1 s moves ≥ 10 bp, and the only
                       series that can be aligned with the Polymarket books (same clock),
                       so it is what the implied-σ comparison uses.
  tob (replay)         Polymarket top of book after every event → last valid mid per second,
                       as-of inside strict clean windows only.

Conventions. σ_step is the standard deviation of a log return over one step; annualised with
a 365-day year because BTC trades 24/7:  σ_ann = σ_step · sqrt(31 536 000 / step_s).  A
horizon h seconds ahead:  σ_h = σ_step · sqrt(h / step_s)  (independent increments). The
15-minute horizon from an annual σ:  σ_15m = σ_ann · sqrt(900 / 31 536 000). All of this is
"provisional": three days of history and one afternoon of joint data.

Why the estimators are small pure-numpy functions: every number in the report must be
reproducible from a formula the reader can check, and the tests feed synthetic GBM paths
through exactly the functions the report uses.
"""
from __future__ import annotations

import logging
import math
import re
import time
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import numpy as np
import polars as pl

from .replay import NS, parse_ts

log = logging.getLogger(__name__)

SEC_PER_YEAR = 365 * 86_400  # 31 536 000: BTC has no closed days
MIN_PER_YEAR = SEC_PER_YEAR // 60  # 525 600
WINDOW_S = 900  # btc_15m window length
LAMBDAS = (0.94, 0.97, 0.99)
RV_WINDOWS_MIN = (5, 15, 60)  # rolling realized-variance windows on 1-min returns
SIG_DELTAS_S = (1, 2, 5, 10, 15, 30, 60, 120, 300, 600)  # signature plot sampling intervals
MOVE_THRESHOLDS_BP = (5.0, 10.0, 20.0)
X_JUMP_THRESHOLDS = (0.2, 0.5, 1.0)
P_JUMP_THRESHOLDS = (0.02, 0.05, 0.10)
HORIZONS_S = (1, 10, 60)
# Why clip at 2 % / 98 %: logit(0.005) = −5.3 and the 1 ¢ tick is 1.0 in x there, so Δx near
# the boundary measures the tick, not the diffusion; the p-space checks use the full range.
X_P_MIN, X_P_MAX = 0.02, 0.98


# ======================================================================================
# 1. Estimators (pure numpy; what the tests exercise)
# ======================================================================================


def log_returns(prices: np.ndarray) -> np.ndarray:
    """r_t = ln(P_t / P_{t−1}); length n−1."""
    p = np.asarray(prices, dtype=float)
    return np.diff(np.log(p))


def realized_sigma(r: np.ndarray) -> float:
    """σ_step = sqrt( (1/n) Σ r_t² ): the realized volatility over the sample, per step.

    Why not demeaned: over seconds or minutes the mean return is ~0 and estimating it adds
    noise (Merton); squared returns are the standard high-frequency estimator."""
    r = np.asarray(r, dtype=float)
    r = r[np.isfinite(r)]
    return float(np.sqrt(np.mean(r * r))) if r.size else float("nan")


def rolling_sigma(r: np.ndarray, window: int) -> np.ndarray:
    """σ_t = sqrt( (1/w) Σ_{i=t−w+1..t} r_i² ) aligned with r; NaN until w returns are in."""
    r = np.asarray(r, dtype=float)
    out = np.full(r.shape, np.nan)
    if r.size < window or window <= 0:
        return out
    c = np.cumsum(np.insert(r * r, 0, 0.0))
    out[window - 1:] = np.sqrt((c[window:] - c[:-window]) / window)
    return out


def ewma_variance(r: np.ndarray, lam: float, init: float | None = None) -> np.ndarray:
    """σ²_t = λ σ²_{t−1} + (1 − λ) r_t²   (RiskMetrics).  out[t] = σ²_t, i.e. the variance
    known after return t, which is the forecast for step t+1.

    Why init = mean of the first ~1/(1−λ) squared returns: starting at r_0² alone makes the
    first hundred values a random walk from one observation; the mean over one effective
    memory length is the unbiased choice and the report drops that burn-in anyway."""
    r = np.asarray(r, dtype=float)
    out = np.empty(r.shape)
    if r.size == 0:
        return out
    if init is None:
        n0 = max(1, min(r.size, int(round(1 / (1 - lam)))))
        init = float(np.mean(r[:n0] ** 2))
    v = init
    one_minus = 1.0 - lam
    for i, x in enumerate(r):
        v = lam * v + one_minus * x * x
        out[i] = v
    return out


def parkinson_variance(high: np.ndarray, low: np.ndarray) -> np.ndarray:
    """Per-bar variance from the range:  σ² = (ln(H/L))² / (4 ln 2)   (Parkinson 1980).
    Why: a bar's range uses every trade inside it, so it is ~5× as efficient as close-to-close
    for a diffusion; biased low when the bar has few trades (range under-sampled)."""
    return (np.log(np.asarray(high, float) / np.asarray(low, float)) ** 2) / (4.0 * math.log(2.0))


def garman_klass_variance(o: np.ndarray, h: np.ndarray, l: np.ndarray, c: np.ndarray) -> np.ndarray:
    """σ² = ½ (ln(H/L))² − (2 ln 2 − 1) (ln(C/O))²   (Garman–Klass 1980), per bar."""
    hl = np.log(np.asarray(h, float) / np.asarray(l, float))
    co = np.log(np.asarray(c, float) / np.asarray(o, float))
    return 0.5 * hl * hl - (2.0 * math.log(2.0) - 1.0) * co * co


def annualise(sigma_step: float | np.ndarray, step_s: float) -> float | np.ndarray:
    """σ_ann = σ_step · sqrt(SEC_PER_YEAR / step_s)."""
    return sigma_step * math.sqrt(SEC_PER_YEAR / step_s)


def scale_horizon(sigma_step: float | np.ndarray, step_s: float, horizon_s: float) -> float | np.ndarray:
    """σ_h = σ_step · sqrt(h / step_s): independent increments add in variance."""
    return sigma_step * math.sqrt(horizon_s / step_s)


def sigma_15m_from_annual(sigma_ann: float | np.ndarray) -> float | np.ndarray:
    """σ_15m = σ_ann · sqrt(900 / 31 536 000)  (= σ_ann / 187.2)."""
    return sigma_ann * math.sqrt(WINDOW_S / SEC_PER_YEAR)


def qlike(var_forecast: np.ndarray, realized_sq: np.ndarray) -> float:
    """QLIKE = mean( r²/σ̂² − ln(r²/σ̂²) − 1 ).  Zero iff σ̂² = r² everywhere; the loss is
    robust to noise in the realized proxy (Patton 2011) and penalises under-forecasts of
    variance more than over-forecasts, which is the right asymmetry for a market maker."""
    f = np.asarray(var_forecast, float)
    y = np.asarray(realized_sq, float)
    ok = np.isfinite(f) & np.isfinite(y) & (f > 0) & (y > 0)
    z = y[ok] / f[ok]
    return float(np.mean(z - np.log(z) - 1.0)) if z.size else float("nan")


def mse(var_forecast: np.ndarray, realized_sq: np.ndarray) -> float:
    """MSE = mean( (r² − σ̂²)² ) on variances."""
    f = np.asarray(var_forecast, float)
    y = np.asarray(realized_sq, float)
    ok = np.isfinite(f) & np.isfinite(y)
    return float(np.mean((y[ok] - f[ok]) ** 2)) if ok.any() else float("nan")


def variance_per_second(logp: np.ndarray, k: int) -> tuple[float, int]:
    """Signature-plot point: sample the 1 s log-price every k seconds, then
    var_per_s(k) = Σ (k-step returns)² / (n_k · k).  For a pure diffusion this is flat in k;
    bid-ask bounce (iid noise with variance η²) adds 2η²/k, so the curve falls toward the
    true value as k grows and the smallest flat k is the shortest usable sampling."""
    x = np.asarray(logp, float)
    if x.size <= k:
        return float("nan"), 0
    r = x[k::k] - x[:-k:k]
    r = r[np.isfinite(r)]
    return float(np.sum(r * r) / (r.size * k)), int(r.size)


def logit(p: np.ndarray | float) -> np.ndarray | float:
    """x = ln(p / (1 − p))."""
    return np.log(p / (1.0 - p))


def expit(x: np.ndarray | float) -> np.ndarray | float:
    """p = 1 / (1 + e^{−x})."""
    return 1.0 / (1.0 + np.exp(-x))


def sigma_p_from_sigma_x(p: np.ndarray | float, sigma_x: float) -> np.ndarray | float:
    """σ_p ≈ p(1 − p) σ_x: first-order delta method, dp/dx = p(1 − p)."""
    return p * (1.0 - p) * sigma_x


def norm_cdf(x: np.ndarray | float) -> np.ndarray | float:
    """Φ(x) = ½ (1 + erf(x / √2))."""
    x = np.asarray(x, float)
    return 0.5 * (1.0 + _erf(x / math.sqrt(2.0)))


_erf = np.vectorize(math.erf, otypes=[float])


def norm_ppf(p: np.ndarray | float) -> np.ndarray | float:
    """Φ⁻¹(p): Acklam's rational approximation (|err| < 1.2e−9) plus one Newton step.
    Why not scipy: it is not a dependency, and one inverse normal is all the model needs."""
    p = np.asarray(p, float)
    a = (-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00)
    b = (-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01)
    c = (-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00)
    d = (7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00)
    plow = 0.02425
    out = np.full(p.shape, np.nan)
    lo = (p > 0) & (p < plow)
    hi = (p > 1 - plow) & (p < 1)
    mid = (p >= plow) & (p <= 1 - plow)
    q = np.sqrt(-2 * np.log(p[lo]))
    out[lo] = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
              ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    q = np.sqrt(-2 * np.log(1 - p[hi]))
    out[hi] = -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    q = p[mid] - 0.5
    r = q * q
    out[mid] = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
               (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    ok = np.isfinite(out)
    # Newton: x ← x − (Φ(x) − p) / φ(x)
    e = norm_cdf(out[ok]) - p[ok]
    out[ok] -= e * math.sqrt(2 * math.pi) * np.exp(out[ok] ** 2 / 2)
    return out if out.ndim else float(out)


def implied_sigma_point(mid: np.ndarray, s_t: np.ndarray, s_0: float | np.ndarray, tau_s: np.ndarray,
                        min_abs_ret: float = 1e-4, min_abs_edge: float = 0.02) -> np.ndarray:
    """Solve mid = Φ( ln(S_t/S_0) / (σ √τ) ) for σ (per √second) at each instant:
        σ = ln(S_t/S_0) / ( Φ⁻¹(mid) · √τ ).
    NaN where the equation has no information: |ln(S_t/S_0)| < 1 bp or |mid − ½| < 0.02 (0/0),
    τ ≤ 0, or mid outside (0.03, 0.97). Negative values mean the market's side disagrees with
    the spot move, i.e. the Binance S_0 is not the market's reference price at that instant."""
    mid = np.asarray(mid, float)
    x = np.log(np.asarray(s_t, float) / s_0)
    tau = np.asarray(tau_s, float)
    z = norm_ppf(np.clip(mid, 1e-6, 1 - 1e-6))
    ok = (np.abs(x) >= min_abs_ret) & (np.abs(mid - 0.5) >= min_abs_edge) & (tau > 0) & (mid > 0.03) & (mid < 0.97)
    out = np.full(mid.shape, np.nan)
    out[ok] = x[ok] / (z[ok] * np.sqrt(tau[ok]))
    return out


def implied_sigma_window(mid: np.ndarray, s_t: np.ndarray, s_0: float, tau_s: np.ndarray,
                         min_abs_edge: float = 0.02) -> tuple[float, float, int, float]:
    """One σ per window by least squares. Rewriting the model,
        Φ⁻¹(mid_t) · √τ_t  =  a  +  (1/σ) · ln(S_t/S_0),
    so y_t = Φ⁻¹(mid_t)√τ_t regressed on x_t = ln(S_t/S_0) has slope 1/σ. The intercept a
    absorbs a constant offset between Binance's S_0 and the market's (Chainlink) start price,
    which a point-wise solve cannot. Returns (σ per √s, intercept a, n, r²); NaN if < 30
    points or the slope is ≤ 0 (market moving against spot)."""
    mid = np.asarray(mid, float)
    x = np.log(np.asarray(s_t, float) / s_0)
    tau = np.asarray(tau_s, float)
    ok = np.isfinite(mid) & np.isfinite(x) & (tau > 0) & (mid > 0.03) & (mid < 0.97) & (np.abs(mid - 0.5) >= min_abs_edge)
    if ok.sum() < 30:
        return float("nan"), float("nan"), int(ok.sum()), float("nan")
    y = norm_ppf(mid[ok]) * np.sqrt(tau[ok])
    xx = x[ok]
    xm, ym = xx.mean(), y.mean()
    sxx = np.sum((xx - xm) ** 2)
    if sxx <= 0:
        return float("nan"), float("nan"), int(ok.sum()), float("nan")
    slope = float(np.sum((xx - xm) * (y - ym)) / sxx)
    a = float(ym - slope * xm)
    resid = y - (a + slope * xx)
    r2 = float(1 - np.sum(resid ** 2) / max(np.sum((y - ym) ** 2), 1e-300))
    sigma = 1.0 / slope if slope > 0 else float("nan")
    return sigma, a, int(ok.sum()), r2


def bp(x: float | np.ndarray) -> float | np.ndarray:
    """log return → basis points."""
    return x * 1e4


# ======================================================================================
# 2. Binance series (DuckDB → polars)
# ======================================================================================


def _pq_list(files: list[Path]) -> str:
    return "[" + ", ".join(f"'{f}'" for f in files) + "]"


def _spot_files(spot_root: Path, kind: str) -> list[Path]:
    base = {"hist": spot_root / "backfill" / "spot_trades_hist",
            "trades": spot_root / "spot" / "spot_trades",
            "quotes": spot_root / "spot" / "spot_quotes"}[kind]
    return sorted(base.rglob("*.parquet")) if base.exists() else []


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("set memory_limit = '3GB'")
    con.execute("set preserve_insertion_order = false")
    return con


def load_trades_1s(spot_root: Path, symbol: str = "BTCUSDT") -> pl.DataFrame:
    """Last trade price per exchange second: (sec, px, n, src).  src = 'hist' for days in
    the Binance daily history, 'live' for days only our own WebSocket recorded.

    Why the exchange clock here and not ours: the history has no receive time, and the
    live trades carry the same exchange `ts_exch` (ms vs µs), so one clock stitches the two.
    Why history wins where both exist: it is complete (checksummed daily zip); the live
    stream can have gaps at reconnects."""
    con = _con()
    hist, live = _spot_files(spot_root, "hist"), _spot_files(spot_root, "trades")
    parts = []
    if hist:
        parts.append(con.sql(f"""
            select ts_exch_us // 1000000 as sec, last(price order by ts_exch_us, trade_id) as px,
                   count(*) as n, 'hist' as src
            from read_parquet({_pq_list(hist)}) where symbol = '{symbol}' group by 1""").pl())
    if live:
        lv = con.sql(f"""
            select ts_exch_ms // 1000 as sec, last(price order by ts_exch_ms, trade_id) as px,
                   count(*) as n, 'live' as src
            from read_parquet({_pq_list(live)}, union_by_name=true)
            where symbol = '{symbol}' and price is not null group by 1""").pl()
        if parts:
            hist_days = set((parts[0]["sec"] // 86400).unique().to_list())
            lv = lv.filter(~(pl.col("sec") // 86400).is_in(list(hist_days)))
        parts.append(lv)
    if not parts:
        return pl.DataFrame(schema={"sec": pl.Int64, "px": pl.Float64, "n": pl.Int64, "src": pl.String})
    return pl.concat(parts).sort("sec")


def load_bars_1m(spot_root: Path, symbol: str = "BTCUSDT") -> pl.DataFrame:
    """1-minute OHLC bars keyed by the minute's start second: (minute, open, high, low, close,
    n, qty, src). Same source preference as `load_trades_1s`."""
    con = _con()
    hist, live = _spot_files(spot_root, "hist"), _spot_files(spot_root, "trades")
    q = """select ({ts}) // 60 * 60 as minute, first(price order by {ts}, trade_id) as open, max(price) as high,
                  min(price) as low, last(price order by {ts}, trade_id) as close, count(*) as n, sum(qty) as qty,
                  '{src}' as src
           from read_parquet({files}, union_by_name=true) where symbol = '{symbol}' and price is not null group by 1"""
    parts = []
    if hist:
        parts.append(con.sql(q.format(ts="ts_exch_us // 1000000", src="hist", files=_pq_list(hist), symbol=symbol)).pl())
    if live:
        lv = con.sql(q.format(ts="ts_exch_ms // 1000", src="live", files=_pq_list(live), symbol=symbol)).pl()
        if parts:
            hist_days = set((parts[0]["minute"] // 86400).unique().to_list())
            lv = lv.filter(~(pl.col("minute") // 86400).is_in(list(hist_days)))
        parts.append(lv)
    if not parts:
        return pl.DataFrame(schema={"minute": pl.Int64, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
                                    "close": pl.Float64, "n": pl.Int64, "qty": pl.Float64, "src": pl.String})
    return pl.concat(parts).sort("minute")


def load_quotes_1s(spot_root: Path, symbol: str = "BTCUSDT") -> pl.DataFrame:
    """Last Binance mid per second of OUR clock: (sec, mid, n). As-of: quotes with
    ts_recv_ns in [sec, sec+1) → the value standing at the end of the second."""
    con = _con()
    files = _spot_files(spot_root, "quotes")
    if not files:
        return pl.DataFrame(schema={"sec": pl.Int64, "mid": pl.Float64, "n": pl.Int64})
    return con.sql(f"""
        select ts_recv_ns // {NS} as sec, arg_max((bid + ask) / 2, ts_recv_ns) as mid, count(*) as n
        from read_parquet({_pq_list(files)}, union_by_name=true)
        where symbol = '{symbol}' and bid is not null and ask is not null group by 1 order by 1""").pl()


def dense_grid(df: pl.DataFrame, key: str, cols: list[str], step: int = 1) -> pl.DataFrame:
    """Fill every `step` between min and max of `key`, forward-filling `cols`; adds `has_obs`
    and `gap_s` (seconds since the last observation, 0 on an observed row).
    Why: returns are taken on a regular grid; a second without a trade keeps the last price
    (as-of), and `gap_s` lets a caller drop returns that span a feed outage."""
    if not df.height:
        return df.with_columns(pl.lit(True).alias("has_obs"), pl.lit(0).cast(pl.Int64).alias("gap_s"))
    lo, hi = int(df[key].min()), int(df[key].max())
    grid = pl.DataFrame({key: np.arange(lo, hi + 1, step, dtype=np.int64)})
    out = grid.join(df.with_columns(pl.lit(True).alias("has_obs"), pl.col(key).alias("_obs")), on=key, how="left")
    out = out.with_columns(pl.col("has_obs").fill_null(False), *[pl.col(c).forward_fill() for c in cols + ["_obs"]])
    return out.with_columns((pl.col(key) - pl.col("_obs")).fill_null(0).alias("gap_s")).drop("_obs")


def _hour_utc(sec: pl.Expr) -> pl.Expr:
    return (sec % 86400) // 3600


def _day(sec: pl.Expr) -> pl.Expr:
    return pl.from_epoch(sec, time_unit="s").dt.date()


# ======================================================================================
# 3. BTC analysis
# ======================================================================================


@dataclass
class BtcResult:
    tables: dict[str, pl.DataFrame] = field(default_factory=dict)
    series: dict[str, pl.DataFrame] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    chosen: dict = field(default_factory=dict)
    sigma_1s: pl.DataFrame | None = None  # the sigma_btc.parquet frame


def _returns_1s(grid: pl.DataFrame, col: str, max_gap_s: int = 60) -> np.ndarray:
    """1 s log returns on a dense grid, NaN where the step crosses a feed gap > max_gap_s."""
    px = grid[col].to_numpy()
    gap = grid["gap_s"].to_numpy()
    r = np.diff(np.log(px))
    r[gap[1:] > max_gap_s] = np.nan
    return r


def _stats_sigma(r_step: np.ndarray, step_s: float) -> dict:
    r = r_step[np.isfinite(r_step)]
    s = realized_sigma(r)
    return {"n": int(r.size), "sigma_step_bp": bp(s), "sigma_ann": annualise(s, step_s),
            "sigma_15m_bp": bp(scale_horizon(s, step_s, WINDOW_S))}


def _bars_returns(bars: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """(minute, r_1m) with r NaN where the previous bar is not the previous minute."""
    m = bars["minute"].to_numpy()
    c = bars["close"].to_numpy()
    r = np.full(m.shape, np.nan)
    ok = np.diff(m) == 60
    r[1:][ok] = np.log(c[1:][ok] / c[:-1][ok])
    return m, r


def _forecast_table(bars: pl.DataFrame, lambdas=LAMBDAS, rv_windows=RV_WINDOWS_MIN) -> pl.DataFrame:
    """Per 1-min bar: σ²_1m forecasts known at the END of the bar (for the next step):
    ewma_<λ>, rv<w> (rolling mean of r² over the last w bars). NaN during the burn-in."""
    m, r = _bars_returns(bars)
    out = {"minute": m, "r_1m": r}
    r0 = np.where(np.isfinite(r), r, 0.0)
    # Why zero-fill inside the recursion: a missing minute (feed gap) contributes no shock
    # and the variance decays as if the minute were quiet; the alternative, restarting the
    # recursion at every gap, throws away the burn-in each time.
    for lam in lambdas:
        out[f"ewma_{lam:.2f}"] = ewma_variance(r0, lam)
    for w in rv_windows:
        out[f"rv{w}"] = rolling_sigma(r0, w) ** 2
    return pl.DataFrame(out)


def btc_analysis(spot_root: Path, date: str, lambdas=LAMBDAS) -> BtcResult:
    res = BtcResult()
    t = time.time()
    tr = load_trades_1s(spot_root)
    bars = load_bars_1m(spot_root)
    qt = load_quotes_1s(spot_root)
    log.info("btc: loaded trades_1s %d s, bars %d, quotes_1s %d s in %.1fs", tr.height, bars.height, qt.height, time.time() - t)
    if not tr.height:
        res.notes.append("no Binance trades found under " + str(spot_root))
        return res
    hist_days = sorted((tr.filter(pl.col("src") == "hist")["sec"] // 86400).unique().to_list())
    res.notes.append(f"history days: {[datetime.fromtimestamp(d * 86400, timezone.utc).date().isoformat() for d in hist_days]}; "
                     f"live trades {tr.filter(pl.col('src') == 'live').height} s; quotes {qt.height} s")

    # ---- 3.1 realized σ per day from 1 s and 1 min returns, Parkinson / GK from bars ------
    grid = dense_grid(tr, "sec", ["px", "src"])
    grid = grid.with_columns(_day(pl.col("sec")).alias("day"))
    r1 = _returns_1s(grid, "px")
    m, r1m = _bars_returns(bars)
    rows = []
    days = grid["day"].unique().sort().to_list()
    for d in days + ["all"]:
        if d == "all":
            sel1, selm, selb = np.ones(r1.shape, bool), np.ones(r1m.shape, bool), np.ones(bars.height, bool)
        else:
            dmask = (grid["day"] == d).to_numpy()
            sel1 = dmask[1:]
            bday = bars.with_columns(_day(pl.col("minute")).alias("day"))["day"] == d
            selm = selb = bday.to_numpy()
        s1 = _stats_sigma(r1[sel1], 1)
        sm = _stats_sigma(r1m[selm], 60)
        b = bars.filter(pl.Series(selb))
        pk = float(np.nanmean(parkinson_variance(b["high"].to_numpy(), b["low"].to_numpy()))) if b.height else float("nan")
        gk = float(np.nanmean(garman_klass_variance(b["open"].to_numpy(), b["high"].to_numpy(), b["low"].to_numpy(), b["close"].to_numpy()))) if b.height else float("nan")
        rows.append({"day": str(d), "n_1s": s1["n"], "sigma_1s_bp": s1["sigma_step_bp"], "ann_from_1s": s1["sigma_ann"],
                     "n_1m": sm["n"], "sigma_1m_bp": sm["sigma_step_bp"], "ann_from_1m": sm["sigma_ann"],
                     "sigma_15m_from_1m_bp": sm["sigma_15m_bp"],
                     "parkinson_1m_bp": bp(math.sqrt(pk)) if pk == pk else float("nan"), "ann_parkinson": annualise(math.sqrt(pk), 60) if pk == pk else float("nan"),
                     "garman_klass_1m_bp": bp(math.sqrt(gk)) if gk == gk and gk > 0 else float("nan"), "ann_garman_klass": annualise(math.sqrt(gk), 60) if gk == gk and gk > 0 else float("nan"),
                     "n_bars": int(b.height)})
    res.tables["realized_by_day"] = pl.DataFrame(rows)

    # ---- 3.2 rolling windows and EWMA ---------------------------------------------------
    rows = []
    for w_s in (300, 900, 3600):
        rs = rolling_sigma(np.where(np.isfinite(r1), r1, 0.0), w_s)
        rs = rs[np.isfinite(rs) & (rs > 0)]
        rows.append({"estimator": f"rolling {w_s // 60} min on 1 s returns", "n": int(rs.size),
                     "p10_ann": annualise(float(np.quantile(rs, 0.10)), 1), "p50_ann": annualise(float(np.quantile(rs, 0.50)), 1),
                     "p90_ann": annualise(float(np.quantile(rs, 0.90)), 1), "p50_15m_bp": bp(scale_horizon(float(np.quantile(rs, 0.5)), 1, WINDOW_S))})
    fc = _forecast_table(bars, lambdas)
    burn = int(round(1 / (1 - max(lambdas))))  # 100 bars for λ = 0.99
    for col in [c for c in fc.columns if c.startswith("rv") or c.startswith("ewma")]:
        v = fc[col].to_numpy()[burn:]
        v = v[np.isfinite(v) & (v > 0)]
        s = np.sqrt(v)
        label = f"EWMA λ={col[5:]} on 1 min returns" if col.startswith("ewma") else f"rolling {col[2:]} min on 1 min returns"
        rows.append({"estimator": label, "n": int(s.size), "p10_ann": annualise(float(np.quantile(s, 0.10)), 60),
                     "p50_ann": annualise(float(np.quantile(s, 0.50)), 60), "p90_ann": annualise(float(np.quantile(s, 0.90)), 60),
                     "p50_15m_bp": bp(scale_horizon(float(np.quantile(s, 0.5)), 60, WINDOW_S))})
    res.tables["rolling_ewma"] = pl.DataFrame(rows)
    res.series["forecasts_1m"] = fc

    # ---- 3.3 seasonality: σ by hour of day (UTC) and by day ----------------------------
    bm = pl.DataFrame({"minute": m, "r": r1m}).filter(pl.col("r").is_not_nan()).with_columns(_hour_utc(pl.col("minute")).alias("hour"), _day(pl.col("minute")).alias("day"))
    hist_only = bm.filter((pl.col("minute") // 86400).is_in(hist_days)) if hist_days else bm
    by_hour = hist_only.group_by("hour").agg(pl.len().alias("n_1m"), (pl.col("r") ** 2).mean().sqrt().alias("sigma_1m")).sort("hour")
    by_hour = by_hour.with_columns(bp(pl.col("sigma_1m")).alias("sigma_1m_bp"), (pl.col("sigma_1m") * math.sqrt(MIN_PER_YEAR)).alias("sigma_ann"),
                                   bp(pl.col("sigma_1m") * math.sqrt(15)).alias("sigma_15m_bp"),
                                   (pl.col("hour") - 5).alias("hour_et_std")).drop("sigma_1m")
    res.tables["by_hour_utc"] = by_hour
    g1 = pl.DataFrame({"sec": grid["sec"][1:], "r": r1}).filter(pl.col("r").is_not_nan()).with_columns(_hour_utc(pl.col("sec")).alias("hour"))
    g1 = g1.filter((pl.col("sec") // 86400).is_in(hist_days)) if hist_days else g1
    res.tables["by_hour_utc_1s"] = g1.group_by("hour").agg(pl.len().alias("n_1s"), (pl.col("r") ** 2).mean().sqrt().alias("s")).sort("hour") \
        .with_columns(bp(pl.col("s")).alias("sigma_1s_bp"), (pl.col("s") * math.sqrt(SEC_PER_YEAR)).alias("sigma_ann")).drop("s")
    by_day = bm.group_by("day").agg(pl.len().alias("n_1m"), (pl.col("r") ** 2).mean().sqrt().alias("s")).sort("day") \
        .with_columns(pl.col("day").dt.strftime("%a").alias("weekday"), bp(pl.col("s")).alias("sigma_1m_bp"),
                      (pl.col("s") * math.sqrt(MIN_PER_YEAR)).alias("sigma_ann")).drop("s")
    res.tables["by_day"] = by_day

    # ---- 3.4 signature plot: variance per second vs sampling interval -------------------
    rows = []
    logp_days = [np.log(grid.filter(pl.col("day") == d)["px"].to_numpy()) for d in days
                 if grid.filter(pl.col("day") == d).height > 3600 and (grid.filter(pl.col("day") == d)["src"] == "hist").all()]
    qgrid = dense_grid(qt, "sec", ["mid"]) if qt.height else None
    logq = np.log(qgrid["mid"].to_numpy()) if qgrid is not None and qgrid.height > 3600 else None
    for k in SIG_DELTAS_S:
        vt, nt = [], 0
        for lp in logp_days:
            v, n = variance_per_second(lp, k)
            if n:
                vt.append(v * n); nt += n
        v_tr = sum(vt) / nt if nt else float("nan")
        v_q, n_q = variance_per_second(logq, k) if logq is not None else (float("nan"), 0)
        rows.append({"delta_s": k, "var_per_s_trades_bp2": v_tr * 1e8, "n_trades": nt, "ann_trades": annualise(math.sqrt(v_tr), 1) if v_tr == v_tr else float("nan"),
                     "var_per_s_quotes_bp2": v_q * 1e8, "n_quotes": n_q, "ann_quotes": annualise(math.sqrt(v_q), 1) if v_q == v_q else float("nan")})
    sig = pl.DataFrame(rows)
    ref = sig.filter(pl.col("delta_s") == 300)
    if ref.height:
        sig = sig.with_columns((pl.col("var_per_s_trades_bp2") / ref["var_per_s_trades_bp2"][0]).alias("ratio_to_300s_trades"),
                               (pl.col("var_per_s_quotes_bp2") / ref["var_per_s_quotes_bp2"][0]).alias("ratio_to_300s_quotes"))
    res.tables["signature"] = sig

    # ---- 3.5 15-minute moves and forecast quality --------------------------------------
    sec = grid["sec"].to_numpy()
    px = grid["px"].to_numpy()
    gap = grid["gap_s"].to_numpy()
    first_b = (int(sec[0]) // WINDOW_S + 1) * WINDOW_S
    bounds = np.arange(first_b, int(sec[-1]) - WINDOW_S + 1, WINDOW_S)
    i0 = np.searchsorted(sec, bounds)
    i1 = np.searchsorted(sec, bounds + WINDOW_S)
    ok = (gap[i0] <= 60) & (gap[i1] <= 60)
    bounds, i0, i1 = bounds[ok], i0[ok], i1[ok]
    move = np.log(px[i1] / px[i0])
    # realized variance inside the window from 1 s returns (for the implied-σ comparison)
    c2 = np.cumsum(np.insert(np.where(np.isfinite(r1), r1 * r1, 0.0), 0, 0.0))
    rv_in = c2[i1] - c2[i0]
    mv = pl.DataFrame({"start_s": bounds, "move": move, "move_bp": bp(move), "abs_bp": np.abs(bp(move)), "rv_in_window": rv_in,
                       "s_start": px[i0], "s_end": px[i1]}).with_columns(_hour_utc(pl.col("start_s")).alias("hour"), _day(pl.col("start_s")).alias("day"))
    # forecasts known at the window start = the bar that closes at start_s (minute = start_s − 60)
    fc_at = fc.with_columns((pl.col("minute") + 60).alias("start_s")).drop("minute", "r_1m")
    mv = mv.join(fc_at, on="start_s", how="left")
    res.series["moves_15m"] = mv
    a = mv["abs_bp"].to_numpy()
    dist = {"n_windows": int(mv.height), "std_move_bp": float(np.std(mv["move_bp"].to_numpy())), "mean_abs_bp": float(a.mean()),
            "p50_abs_bp": float(np.quantile(a, 0.5)), "p90_abs_bp": float(np.quantile(a, 0.9)), "p99_abs_bp": float(np.quantile(a, 0.99)),
            "max_abs_bp": float(a.max())}
    for th in MOVE_THRESHOLDS_BP:
        dist[f"share_gt_{int(th)}bp"] = float((a > th).mean())
    res.tables["moves_15m_dist"] = pl.DataFrame([dist])
    res.tables["moves_15m_by_hour"] = mv.group_by("hour").agg(pl.len().alias("n"), (pl.col("move") ** 2).mean().sqrt().alias("s"), pl.col("abs_bp").max().alias("max_abs_bp")) \
        .sort("hour").with_columns(bp(pl.col("s")).alias("rms_move_bp")).drop("s")
    # scoring: variance per 15 min = 15 · σ²_1m (independent minutes); constant and seasonal are in-sample references
    y = move * move
    scored = mv.filter(pl.col("ewma_0.99").is_not_null())
    y_s = scored["move"].to_numpy() ** 2
    var_const = float(np.nanmean(r1m ** 2)) * 15
    hv = {int(h): float(s) ** 2 * 15 for h, s in zip(by_hour["hour"].to_list(), (by_hour["sigma_1m_bp"] / 1e4).to_list())}
    cands = {f"EWMA λ={c[5:]}": scored[c].to_numpy() * 15 for c in fc.columns if c.startswith("ewma")}
    cands.update({f"rolling {c[2:]} min RV": scored[c].to_numpy() * 15 for c in fc.columns if c.startswith("rv")})
    for lam in (0.97, 0.99):
        cands[f"EWMA λ={lam:.2f} deseasonalised (LOO hour profile)"] = _deseasonalised_ewma_at(bars, m, r1m, lam, scored["start_s"].to_numpy())
    cands["constant (in-sample)"] = np.full(y_s.shape, var_const)
    cands["hour-of-day (in-sample)"] = np.array([hv.get(int(h), var_const) for h in scored["hour"].to_list()])
    rows = []
    for name, f in cands.items():
        okf = np.isfinite(f) & (f > 0)
        rows.append({"estimator": name, "n": int(okf.sum()), "qlike": qlike(f, y_s), "mse_bp4": mse(f * 1e8, y_s * 1e8),
                     "mean_forecast_sigma_bp": bp(float(np.sqrt(np.mean(f[okf])))), "realized_std_bp": bp(float(np.sqrt(np.mean(y_s[okf])))),
                     "bias_var_ratio": float(np.mean(f[okf]) / np.mean(y_s[okf])),
                     "share_abs_gt_2sigma": float(np.mean(np.abs(scored["move"].to_numpy()[okf]) > 2 * np.sqrt(f[okf])))})
    score = pl.DataFrame(rows).sort("qlike")
    res.tables["forecast_scores"] = score
    oos = score.filter(~pl.col("estimator").str.contains("in-sample"))
    best = oos.row(0, named=True)
    res.chosen = {"estimator": best["estimator"], "qlike": best["qlike"], "mse_bp4": best["mse_bp4"], "n": best["n"]}
    mlam = re.search(r"λ=(\d\.\d+)", best["estimator"])
    if mlam:
        res.chosen["lambda"] = float(mlam.group(1))
    else:
        res.chosen["lambda"] = 0.97  # EWMA still drives the 1 s series; the report says why
        res.notes.append(f"best out-of-sample 15-min forecast was {best['estimator']}; the σ(t) series uses EWMA λ=0.97 as the fallback")
    res.chosen["deseasonalised"] = "deseasonalised" in best["estimator"]

    # ---- 3.6 jumps ------------------------------------------------------------------------
    lam_j = 0.97
    e = fc[f"ewma_{lam_j:.2f}"].to_numpy()
    sig_prev = np.sqrt(np.concatenate([[np.nan], e[:-1]]))  # forecast made before the bar
    z = np.abs(r1m) / sig_prev
    jm = pl.DataFrame({"minute": m, "r_bp": bp(r1m), "z": z}).filter(pl.col("z").is_not_nan() & (pl.col("z") > 4)).with_columns(_hour_utc(pl.col("minute")).alias("hour"), _day(pl.col("minute")).alias("day"))
    res.tables["jumps_1m"] = jm.sort("z", descending=True).head(20).with_columns(pl.from_epoch(pl.col("minute"), time_unit="s").dt.strftime("%Y-%m-%d %H:%M").alias("utc"))
    n_days_1m = max(1, len(days))
    res.tables["jumps_1m_by_day"] = jm.group_by("day").agg(pl.len().alias("n_jumps_gt4sigma"), pl.col("r_bp").abs().max().alias("max_abs_bp")).sort("day")
    res.tables["jumps_1m_by_hour"] = jm.group_by("hour").agg(pl.len().alias("n")).sort("hour")
    j1 = pl.DataFrame({"sec": sec[1:], "r_bp": bp(r1)}).filter(pl.col("r_bp").is_not_nan() & (pl.col("r_bp").abs() >= 10)).with_columns(_hour_utc(pl.col("sec")).alias("hour"), _day(pl.col("sec")).alias("day"))
    res.tables["jumps_1s_by_day"] = j1.group_by("day").agg(pl.len().alias("n_1s_moves_ge10bp"), pl.col("r_bp").abs().max().alias("max_abs_bp")).sort("day")
    res.tables["jumps_1s_top"] = j1.sort(pl.col("r_bp").abs(), descending=True).head(15).with_columns(pl.from_epoch(pl.col("sec"), time_unit="s").dt.strftime("%Y-%m-%d %H:%M:%S").alias("utc"))
    if qgrid is not None:
        rq = _returns_1s(qgrid, "mid", max_gap_s=5)
        jq = pl.DataFrame({"sec": qgrid["sec"][1:], "r_bp": bp(rq)}).filter(pl.col("r_bp").is_not_nan() & (pl.col("r_bp").abs() >= 10))
        res.tables["jumps_1s_quotes"] = pl.DataFrame([{"n_secs": int(np.isfinite(rq).sum()), "n_moves_ge10bp": int(jq.height),
                                                       "max_abs_bp": float(jq["r_bp"].abs().max()) if jq.height else 0.0,
                                                       "sigma_1s_quotes_bp": bp(realized_sigma(rq)), "sigma_1s_trades_same_span_bp": _same_span_sigma(grid, qgrid, r1)}])
    res.chosen["n_days_1m"] = n_days_1m

    # ---- 3.7 the σ(t) series for `date` -----------------------------------------------------
    d_num = int(datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) // 86400
    prof = hour_profile(m, r1m, exclude_day=d_num)
    res.tables["hour_profile_used"] = pl.DataFrame({"hour": np.arange(24), "var_ratio": prof, "sigma_mult": np.sqrt(prof)})
    res.sigma_1s = sigma_series_for_day(grid, qgrid, fc, date, res.chosen["lambda"], bars=bars, profile=prof)
    res.series["btc_px_1s"] = grid.select("sec", "px", "gap_s")
    res.series["quotes_1s"] = qt
    return res


def hour_profile(m: np.ndarray, r1m: np.ndarray, exclude_day: int | None = None) -> np.ndarray:
    """Variance ratio per UTC hour, prof[h] = mean r² in hour h / mean r² overall, from the
    1-min returns of every day except `exclude_day` (unix day number). All ones if fewer
    than one full day remains. s_h = sqrt(prof[h]) is the σ multiplier of hour h."""
    day = m // 86400
    hour = (m % 86400) // 3600
    r2 = np.where(np.isfinite(r1m), r1m * r1m, np.nan)
    use = np.isfinite(r2) if exclude_day is None else (np.isfinite(r2) & (day != exclude_day))
    if use.sum() < 1440:
        return np.ones(24)
    overall = np.nanmean(r2[use])
    return np.array([np.nanmean(r2[use & (hour == h)]) / overall if (use & (hour == h)).any() else 1.0 for h in range(24)])


def _deseasonalised_ewma_at(bars: pl.DataFrame, m: np.ndarray, r1m: np.ndarray, lam: float, starts: np.ndarray) -> np.ndarray:
    """15-min variance forecast at each window start from an EWMA on DESEASONALISED 1-min
    returns:  r̃_t = r_t / s_{h(t)},  σ̃²_t = λσ̃²_{t−1} + (1−λ) r̃_t²,  forecast = 15 · σ̃²_t · s²_{h}.
    s_h = sqrt( mean r² in hour h / mean r² overall ) is the hour-of-day profile, computed
    WITHOUT the day being forecast (leave-one-day-out), so the score is out of sample.
    Why: the plain EWMA lags the intraday pattern by its memory (~30 min at λ=0.97) and
    under-forecasts the US-open hours; the profile tells it in advance."""
    day = m // 86400
    hour = (m % 86400) // 3600
    r2 = np.where(np.isfinite(r1m), r1m * r1m, np.nan)
    out = np.full(starts.shape, np.nan)
    days = np.unique(day)
    for d in days:
        others = (day != d) & np.isfinite(r2)
        if others.sum() < 1440:  # need at least one other full day for a profile
            continue
        overall = np.nanmean(r2[others])
        prof = np.array([np.nanmean(r2[others & (hour == h)]) / overall if (others & (hour == h)).any() else 1.0 for h in range(24)])
        s = np.sqrt(prof)[hour]
        rt = np.where(np.isfinite(r1m), r1m / s, 0.0)
        v = ewma_variance(rt, lam)
        # forecast known at the end of bar `minute` applies to the window starting at minute + 60
        avail = m + 60
        idx = np.searchsorted(avail, starts, side="right") - 1
        sel = (day[np.clip(idx, 0, len(day) - 1)] == d) & (idx >= 0)
        for j in np.flatnonzero(sel):
            i = idx[j]
            if avail[i] == starts[j]:
                h_next = int((starts[j] % 86400) // 3600)
                out[j] = v[i] * prof[h_next] * 15
    return out


def _same_span_sigma(grid: pl.DataFrame, qgrid: pl.DataFrame, r1: np.ndarray) -> float:
    lo, hi = int(qgrid["sec"].min()), int(qgrid["sec"].max())
    sec = grid["sec"].to_numpy()[1:]
    sel = (sec > lo) & (sec <= hi)
    return bp(realized_sigma(r1[sel])) if sel.any() else float("nan")


def sigma_series_for_day(grid: pl.DataFrame, qgrid: pl.DataFrame | None, fc: pl.DataFrame, date: str, lam: float,
                         bars: pl.DataFrame | None = None, profile: np.ndarray | None = None) -> pl.DataFrame:
    """1 s grid over the UTC day: price (quote mid where our feed has it, else last trade),
    EWMA σ of the chosen λ (updated once per minute, from the bar that closed) and the
    rolling 15-min σ from the last 900 1 s returns; each per-step, annualised and per 15 min.

    Why both: the EWMA is the forecast (what the fair-value model plugs into Φ), the rolling
    15-min RV is the realized σ of the recent past (what the inventory-risk term wants to
    compare against); when they diverge, vol is changing."""
    d0 = int(datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    d1 = d0 + 86400
    g = grid.filter((pl.col("sec") >= d0 - WINDOW_S) & (pl.col("sec") < d1)).select("sec", pl.col("px").alias("px_trade"), "src", pl.col("gap_s").alias("gap_trade"))
    if qgrid is not None:
        q = qgrid.filter((pl.col("sec") >= d0 - WINDOW_S) & (pl.col("sec") < d1)).select("sec", pl.col("mid").alias("px_quote"), pl.col("gap_s").alias("gap_quote"))
        g = g.join(q, on="sec", how="full", coalesce=True).sort("sec")
    else:
        g = g.with_columns(pl.lit(None, pl.Float64).alias("px_quote"), pl.lit(None, pl.Int64).alias("gap_quote"))
    g = g.with_columns(
        pl.when(pl.col("px_quote").is_not_null() & (pl.col("gap_quote") <= 5)).then(pl.col("px_quote")).otherwise(pl.col("px_trade")).alias("px"),
        pl.when(pl.col("px_quote").is_not_null() & (pl.col("gap_quote") <= 5)).then(pl.lit("quote"))
          .when(pl.col("src") == "hist").then(pl.lit("trade_hist")).otherwise(pl.lit("trade_live")).alias("src"),
        pl.min_horizontal(pl.col("gap_trade").fill_null(10 ** 6), pl.col("gap_quote").fill_null(10 ** 6)).alias("gap_s"))
    if not g.height:
        return pl.DataFrame()
    px = g["px"].to_numpy()
    gap = g["gap_s"].to_numpy()
    r = np.diff(np.log(px))
    r[gap[1:] > 60] = np.nan
    r[~np.isfinite(r)] = np.nan
    # rolling 15-min realized σ per second: sqrt( Σ_{last 900 s} r² / n_valid ), NaN if < 450 valid returns
    r2 = np.where(np.isfinite(r), r * r, 0.0)
    valid = np.isfinite(r).astype(float)
    c2 = np.cumsum(np.insert(r2, 0, 0.0))
    cv = np.cumsum(np.insert(valid, 0, 0.0))
    n = len(r)
    rv = np.full(n + 1, np.nan)
    idx = np.arange(WINDOW_S, n + 1)
    nv = cv[idx] - cv[idx - WINDOW_S]
    with np.errstate(invalid="ignore", divide="ignore"):
        rv[idx] = np.where(nv >= WINDOW_S / 2, np.sqrt((c2[idx] - c2[idx - WINDOW_S]) / nv), np.nan)
    # EWMA: the value of bar `minute` is known from minute + 60 on
    e = fc.select((pl.col("minute") + 60).alias("avail_s"), pl.col(f"ewma_{lam:.2f}").sqrt().alias("sigma_ewma_1m"), pl.col("minute").alias("ewma_bar"))
    if bars is not None and profile is not None:
        # deseasonalised EWMA: r̃ = r / s_h, variance in "average-hour" units; re-seasonalised
        # below with the multiplier of the hour the forecast is for
        mm, rr = _bars_returns(bars)
        s_h = np.sqrt(profile)[(mm % 86400) // 3600]
        v = ewma_variance(np.where(np.isfinite(rr), rr / s_h, 0.0), lam)
        e = e.join(pl.DataFrame({"avail_s": mm + 60, "sigma_dewma_1m": np.sqrt(v)}), on="avail_s", how="left")
    else:
        e = e.with_columns(pl.lit(None, pl.Float64).alias("sigma_dewma_1m"))
    out = pl.DataFrame({"sec": g["sec"], "px": g["px"], "src": g["src"], "gap_s": g["gap_s"], "sigma_rv15_1s": rv}) \
        .join_asof(e.sort("avail_s"), left_on="sec", right_on="avail_s", strategy="backward") \
        .filter(pl.col("sec") >= d0)
    s_mult = np.sqrt(profile) if profile is not None else np.ones(24)
    return out.with_columns(
        pl.when(pl.col("gap_s") > 60).then(None).otherwise(pl.col("px")).alias("px"),
        pl.when(pl.col("gap_s") > 60).then(None).otherwise(pl.col("src")).alias("src"),
        (pl.col("sec") - pl.col("ewma_bar") - 60).alias("ewma_age_s"),
        (pl.col("sigma_ewma_1m") * math.sqrt(MIN_PER_YEAR)).alias("sigma_ewma_ann"),
        (pl.col("sigma_ewma_1m") * math.sqrt(15)).alias("sigma_ewma_15m"),
        (pl.col("sigma_rv15_1s") * math.sqrt(SEC_PER_YEAR)).alias("sigma_rv15_ann"),
        (pl.col("sigma_rv15_1s") * math.sqrt(WINDOW_S)).alias("sigma_rv15_15m"),
        pl.lit(lam).alias("lambda")).with_columns(
        # re-seasonalise with the multiplier of the hour the forecast is for (the current hour)
        (pl.col("sigma_dewma_1m") * pl.Series("s_h", s_mult[((out["sec"].to_numpy() % 86400) // 3600)])).alias("sigma_fc_1m")).with_columns(
        (pl.col("sigma_fc_1m") * math.sqrt(MIN_PER_YEAR)).alias("sigma_fc_ann"),
        (pl.col("sigma_fc_1m") * math.sqrt(15)).alias("sigma_fc_15m")).drop("avail_s", "ewma_bar")


# ======================================================================================
# 4. Polymarket side: σ of the mid in log-odds, inside strict clean windows
# ======================================================================================


@dataclass
class PolyResult:
    tables: dict[str, pl.DataFrame] = field(default_factory=dict)
    series: dict[str, pl.DataFrame] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


def load_assets_vol(data_dir: Path) -> pl.DataFrame:
    """quality.load_assets plus what the vol tables need: the 15-minute window start
    (unix seconds in the slug), the ladder strike (dollar amount in the question) and the
    resolution instant (`end_date`, 12:00 ET = 17:00 UTC in November for the ladder)."""
    from .quality import load_assets
    a = load_assets(data_dir)
    files = sorted((data_dir / "reference" / "markets").rglob("*.parquet"))
    q = pl.concat([pl.read_parquet(f, columns=["asset_id", "question"]) for f in files], how="diagonal_relaxed") \
        .group_by("asset_id").agg(pl.col("question").first()) if files else pl.DataFrame(schema={"asset_id": pl.String, "question": pl.String})
    a = a.join(q, on="asset_id", how="left")
    return a.with_columns(
        pl.when(pl.col("market_slug").str.starts_with("btc-updown-15m-")).then(pl.col("market_slug").str.extract(r"(\d+)$").cast(pl.Int64)).alias("start_s"),
        pl.col("question").str.extract(r"\$([\d,]+)").str.replace_all(",", "").cast(pl.Float64).alias("strike"),
        pl.col("end_date").str.to_datetime("%Y-%m-%dT%H:%M:%SZ", strict=False, time_zone="UTC").dt.epoch("s").alias("end_s"))


def last_mid_per_second(derived_dir: Path) -> pl.DataFrame:
    """(asset_id, sec, mid, best_bid, best_ask): the last VALID top of book of each second,
    from the replay's `tob`. Why arg_max on ts_recv_ns: `tob` has a row per event; the mid
    standing at the end of the second is what a 1 s grid should carry (as-of)."""
    files = sorted((derived_dir / "tob").rglob("*.parquet"))
    if not files:
        return pl.DataFrame(schema={"asset_id": pl.String, "sec": pl.Int64, "mid": pl.Float64, "best_bid": pl.Float64, "best_ask": pl.Float64})
    con = _con()
    return con.sql(f"""
        select asset_id, ts_recv_ns // {NS} as sec, arg_max(mid, ts_recv_ns) as mid,
               arg_max(best_bid, ts_recv_ns) as best_bid, arg_max(best_ask, ts_recv_ns) as best_ask
        from read_parquet({_pq_list(files)}, union_by_name=true)
        where valid and event_type <> 'reconnect' and mid is not null and mid > 0 and mid < 1
        group by 1, 2""").pl().sort("asset_id", "sec")


def strict_grid(strict: pl.DataFrame, assets: pl.DataFrame, last_mid: pl.DataFrame) -> pl.DataFrame:
    """1 s grid of every strict clean window of every outcome-0 token (Yes / Up), with the
    as-of mid. Columns: asset_id, family, win_id, t_start, sec, mid, x = logit(mid), plus
    start_s / strike / end_s from the market. Only outcome 0: outcome 1 is its mirror
    (p → 1 − p, x → −x) and would double every count."""
    a0 = assets.filter(pl.col("outcome_index") == 0).select("asset_id", "start_s", "strike", "end_s", "market_slug")
    sw = strict.join(a0, on="asset_id", how="inner").with_row_index("win_id")
    sw = sw.with_columns(((pl.col("t_start") + NS - 1) // NS).alias("s0"), (pl.col("t_end") // NS).alias("s1")).filter(pl.col("s1") >= pl.col("s0"))
    grid = sw.select("asset_id", "family", "win_id", "t_start", "start_s", "strike", "end_s", "market_slug",
                     pl.int_ranges(pl.col("s0"), pl.col("s1") + 1).alias("sec")).explode("sec").sort("asset_id", "sec")
    lm = last_mid.with_columns(pl.col("sec").alias("mid_sec"))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # polars cannot verify sortedness with `by`; both frames are sorted above
        grid = grid.join_asof(lm, on="sec", by="asset_id", strategy="backward")
    # Why require mid_sec inside the window: a mid carried over from before the window start
    # belongs to the book state replay did not trust.
    grid = grid.filter(pl.col("mid").is_not_null() & (pl.col("mid_sec") * NS >= pl.col("t_start")))
    return grid.with_columns(logit(pl.col("mid").clip(0.001, 0.999)).alias("x")).sort("asset_id", "win_id", "sec")


def _dx(grid: pl.DataFrame, k: int) -> pl.DataFrame:
    """Δx and Δp over k seconds inside the same window (contiguous grid ⇒ shift(−k) = sec + k)."""
    g = grid.with_columns(
        (pl.col("x").shift(-k).over(["asset_id", "win_id"]) - pl.col("x")).alias("dx"),
        (pl.col("mid").shift(-k).over(["asset_id", "win_id"]) - pl.col("mid")).alias("dp"),
        pl.col("mid").shift(-k).over(["asset_id", "win_id"]).alias("mid_k"))
    return g.filter(pl.col("dx").is_not_null())


def _sigma_rows(df: pl.DataFrame, by: list[str], k: int) -> pl.DataFrame:
    """std(Δx) over k s for mids inside [X_P_MIN, X_P_MAX] at both ends, with n, the share of
    zero changes and std/√k (the per-√s rate, comparable across horizons)."""
    d = df.filter(pl.col("mid").is_between(X_P_MIN, X_P_MAX) & pl.col("mid_k").is_between(X_P_MIN, X_P_MAX))
    return d.group_by(by).agg(pl.len().alias("n"), pl.col("dx").std().alias("sigma_x"), (pl.col("dx") == 0).mean().alias("share_zero"),
                              pl.col("dx").abs().quantile(0.99).alias("abs_dx_p99")) \
        .with_columns(pl.lit(k).alias("k_s"), (pl.col("sigma_x") / math.sqrt(k)).alias("sigma_x_per_sqrt_s")).sort(by)


def poly_analysis(data_dir: Path, derived_dir: Path, strict: pl.DataFrame, btc_px: pl.DataFrame | None,
                  quotes_1s: pl.DataFrame | None, sigma_1s: pl.DataFrame | None) -> PolyResult:
    res = PolyResult()
    t = time.time()
    assets = load_assets_vol(data_dir)
    lm = last_mid_per_second(derived_dir)
    grid = strict_grid(strict, assets, lm)
    log.info("poly: grid %d s over %d windows, %d tokens in %.1fs", grid.height, grid["win_id"].n_unique(), grid["asset_id"].n_unique(), time.time() - t)
    if not grid.height:
        res.notes.append("no strict-window grid rows")
        return res
    res.tables["grid_coverage"] = grid.group_by("family").agg(pl.len().alias("n_sec"), pl.col("asset_id").n_unique().alias("n_tokens"),
                                                               pl.col("win_id").n_unique().alias("n_windows")).sort("family")

    # ---- 4.1 σ_x per family and horizon ---------------------------------------------------
    parts = [_sigma_rows(_dx(grid, k), ["family"], k) for k in HORIZONS_S]
    res.tables["sigma_x_family"] = pl.concat(parts).sort("family", "k_s")

    # ---- 4.2 btc_15m: by minute in the live window and by |mid − ½| ----------------------
    d10 = _dx(grid, 10)
    b15 = d10.filter((pl.col("family") == "btc_15m") & pl.col("start_s").is_not_null()) \
        .with_columns(((pl.col("sec") - pl.col("start_s")) // 60).alias("minute"), (pl.col("mid") - 0.5).abs().alias("edge"))
    live = b15.filter(pl.col("minute").is_between(0, 14))
    res.tables["btc15_by_minute"] = _sigma_rows(live, ["minute"], 10)
    bins = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
    live = live.with_columns(pl.col("edge").cut(bins[1:-1], labels=[f"{bins[i]:.1f}-{bins[i + 1]:.1f}" for i in range(len(bins) - 1)]).alias("edge_bin"))
    res.tables["btc15_by_edge"] = _sigma_rows(live, ["edge_bin"], 10)
    pre = b15.filter(pl.col("minute") < 0)
    if pre.height:
        res.tables["btc15_pre_window"] = _sigma_rows(pre.with_columns(pl.lit("before start").alias("phase")), ["phase"], 10)

    # ---- 4.3 ladder: by hours to 12:00 ET and by strike distance ------------------------
    lad = d10.filter((pl.col("family") == "btc_daily_ladder") & pl.col("end_s").is_not_null()) \
        .with_columns(((pl.col("end_s") - pl.col("sec")) / 3600).alias("h_to_exp"))
    hb = [0, 1, 3, 6, 12, 24, 48, 96]
    lad = lad.with_columns(pl.col("h_to_exp").cut(hb[1:-1], labels=[f"{hb[i]}-{hb[i + 1]}h" for i in range(len(hb) - 1)]).alias("h_bin"))
    res.tables["ladder_by_hours_to_expiry"] = _sigma_rows(lad, ["h_bin"], 10)
    if btc_px is not None and btc_px.height:
        px = btc_px.select("sec", pl.col("px").alias("btc"), pl.col("gap_s").alias("btc_gap"))
        lad = lad.join(px, on="sec", how="left").filter(pl.col("btc").is_not_null() & (pl.col("btc_gap") <= 60) & pl.col("strike").is_not_null()) \
            .with_columns((pl.col("strike") / pl.col("btc")).log().alias("ln_k_s"))
        db = [-1.0, -0.05, -0.025, -0.01, 0.0, 0.01, 0.025, 0.05, 1.0]
        lab = ["<-5%", "-5..-2.5%", "-2.5..-1%", "-1..0%", "0..1%", "1..2.5%", "2.5..5%", ">5%"]
        lad = lad.with_columns(pl.col("ln_k_s").cut(db[1:-1], labels=lab).alias("dist_bin"))
        res.tables["ladder_by_strike_distance"] = _sigma_rows(lad, ["dist_bin"], 10)
        res.tables["ladder_by_distance_and_hours"] = _sigma_rows(lad.with_columns(pl.col("ln_k_s").abs().cut([0.01, 0.025, 0.05], labels=["<1%", "1-2.5%", "2.5-5%", ">5%"]).alias("abs_dist")), ["abs_dist", "h_bin"], 10)
    else:
        res.notes.append("no BTC price series: ladder strike-distance table skipped")

    # ---- 4.4 σ_p ≈ p(1−p) σ_x, binned by p -------------------------------------------------
    pe = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]
    lab = [f"{pe[i]:.2f}-{pe[i + 1]:.2f}" for i in range(len(pe) - 1)]
    dpb = d10.filter(pl.col("mid").is_between(0.01, 0.99) & pl.col("mid_k").is_between(0.01, 0.99)) \
        .with_columns(pl.col("mid").cut(pe[1:-1], labels=lab).alias("p_bin"))

    def _pcheck(df: pl.DataFrame, by: list[str]) -> pl.DataFrame:
        return df.group_by(by).agg(pl.len().alias("n"), pl.col("mid").mean().alias("p_mean"), pl.col("dp").std().alias("sigma_p"),
                                   pl.col("dx").std().alias("sigma_x")) \
            .with_columns((pl.col("p_mean") * (1 - pl.col("p_mean"))).alias("p(1-p)"), (pl.col("sigma_p") / pl.col("sigma_x")).alias("sigma_p/sigma_x")) \
            .with_columns((pl.col("sigma_p/sigma_x") / pl.col("p(1-p)")).alias("ratio_to_p(1-p)")).sort(by)
    res.tables["sigma_p_check_all"] = _pcheck(dpb, ["p_bin"])
    res.tables["sigma_p_check_family"] = _pcheck(dpb, ["family", "p_bin"])

    # ---- 4.5 jumps in x (and p) per hour of grid time ---------------------------------------
    d1 = _dx(grid, 1)
    hours = d1.group_by("family").agg((pl.len() / 3600).alias("grid_hours"))
    inx = d1.filter(pl.col("mid").is_between(X_P_MIN, X_P_MAX) & pl.col("mid_k").is_between(X_P_MIN, X_P_MAX))
    jx = inx.group_by("family").agg(pl.len().alias("n_x"), *[(pl.col("dx").abs() >= th).sum().alias(f"n_dx_ge_{th}") for th in X_JUMP_THRESHOLDS])
    jp = d1.group_by("family").agg(pl.len().alias("n_p"), *[(pl.col("dp").abs() >= th).sum().alias(f"n_dp_ge_{th}") for th in P_JUMP_THRESHOLDS])
    j = hours.join(jx, on="family", how="left").join(jp, on="family", how="left")
    j = j.with_columns(*[(pl.col(f"n_dx_ge_{th}") / (pl.col("n_x") / 3600)).alias(f"per_h_dx_ge_{th}") for th in X_JUMP_THRESHOLDS],
                       *[(pl.col(f"n_dp_ge_{th}") / pl.col("grid_hours")).alias(f"per_h_dp_ge_{th}") for th in P_JUMP_THRESHOLDS]).sort("family")
    res.tables["jumps_x"] = j
    b1 = d1.filter((pl.col("family") == "btc_15m") & pl.col("start_s").is_not_null()).with_columns(((pl.col("sec") - pl.col("start_s")) // 60).alias("minute")) \
        .filter(pl.col("minute").is_between(0, 14))
    res.tables["btc15_jumps_by_minute"] = b1.group_by("minute").agg(pl.len().alias("n"), (pl.col("dp").abs() >= 0.02).sum().alias("n_dp_ge_0.02"),
                                                                     (pl.col("dp").abs() >= 0.05).sum().alias("n_dp_ge_0.05"),
                                                                     pl.col("dp").abs().max().alias("max_abs_dp")).sort("minute")

    # ---- 4.6 implied σ of the 15-minute market vs realized ---------------------------------
    if quotes_1s is not None and quotes_1s.height:
        res.tables["implied_15m"], res.series["implied_points"] = implied_vs_realized(grid, quotes_1s, sigma_1s)
        if res.tables["implied_15m"].height:
            t_ = res.tables["implied_15m"]
            ok = t_.filter(pl.col("sigma_impl_15m_bp").is_not_nan())
            res.tables["implied_15m_summary"] = pl.DataFrame([{
                "n_windows_fit": int(ok.height), "n_windows_total": int(t_.height),
                "median_implied_15m_bp": float(ok["sigma_impl_15m_bp"].median()) if ok.height else float("nan"),
                "median_realized_1s_15m_bp": float(ok["realized_1s_15m_bp"].median()) if ok.height else float("nan"),
                "median_realized_60s_15m_bp": float(ok["realized_60s_15m_bp"].median()) if ok.height else float("nan"),
                "median_ratio_impl_over_realized_1s": float(ok["impl_over_realized_1s"].median()) if ok.height else float("nan"),
                "median_ratio_impl_over_realized_60s": float(ok["impl_over_realized_60s"].median()) if ok.height else float("nan"),
                "share_windows_impl_gt_realized_60s": float((ok["impl_over_realized_60s"] > 1).mean()) if ok.height else float("nan"),
                "median_ratio_impl_over_season_fc": float(ok["impl_over_fc"].median()) if ok.height and "impl_over_fc" in ok.columns else float("nan"),
                "median_point_sigma_15m_bp": float(ok["point_median_15m_bp"].median()) if ok.height else float("nan"),
                "share_sign_disagree_ge3bp": float(t_["share_sign_disagree_ge3bp"].fill_nan(None).mean() or float("nan"))}])
    else:
        res.notes.append("no Binance quotes on this day: implied σ skipped")
    return res


def implied_vs_realized(grid: pl.DataFrame, quotes_1s: pl.DataFrame, sigma_1s: pl.DataFrame | None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Per live btc_15m market window: σ implied by the Up mid vs BTC realized.
    S_0 = Binance mid as of the window start (our clock, ≤ 5 s stale); S_t the mid at each
    strict-grid second; τ = seconds to the window end. Realized σ over the whole window from
    the quote grid (independent of the Polymarket windows): sqrt(Σ r²) with 1 s returns and
    with 60 s returns (the signature plot says 1 s returns understate variance by ~⅓, 60 s
    does not). Everything per 15 min in bp: σ_15m = σ_√s · √900 · 1e4.
    Why one fit per market window and not per strict piece: a reconnect splits a window's
    grid into pieces that share S_0 and τ; the fit needs all of them. Windows in which
    Binance barely moved (std of ln S_t/S_0 < 1 bp) have no information about σ: NaN."""
    q = dense_grid(quotes_1s, "sec", ["mid"]).rename({"mid": "s_t", "gap_s": "q_gap"})
    g = grid.filter((pl.col("family") == "btc_15m") & pl.col("start_s").is_not_null()) \
        .filter((pl.col("sec") >= pl.col("start_s")) & (pl.col("sec") < pl.col("start_s") + WINDOW_S)) \
        .join(q, on="sec", how="inner").filter(pl.col("q_gap") <= 5)
    s0 = q.rename({"sec": "start_s", "s_t": "s_0", "q_gap": "s0_gap"})
    g = g.join(s0, on="start_s", how="inner").filter(pl.col("s0_gap") <= 5)
    if not g.height:
        return pl.DataFrame(), pl.DataFrame()
    g = g.with_columns((pl.col("start_s") + WINDOW_S - pl.col("sec")).cast(pl.Float64).alias("tau_s"),
                       (pl.col("s_t") / pl.col("s_0")).log().alias("ret"))
    pts = implied_sigma_point(g["mid"].to_numpy(), g["s_t"].to_numpy(), g["s_0"].to_numpy(), g["tau_s"].to_numpy())
    g = g.with_columns(pl.Series("sigma_point", pts)).with_columns((pl.col("sigma_point") * math.sqrt(WINDOW_S) * 1e4).alias("point_15m_bp"),
                                                                    ((pl.col("mid") - 0.5).sign() != pl.col("ret").sign()).alias("sign_disagree"))
    fc = None
    if sigma_1s is not None and sigma_1s.height:
        fc = sigma_1s.select("sec", pl.col("sigma_ewma_15m").alias("ewma_15m"), pl.col("sigma_fc_15m").alias("fc_15m"))
    qs, qm, qg = q["sec"].to_numpy(), np.log(q["s_t"].to_numpy()), q["q_gap"].to_numpy()
    rows = []
    for (aid, st), w in g.group_by(["asset_id", "start_s"], maintain_order=True):
        w = w.sort("sec")
        st = int(st)
        sig, a, n, rsq = implied_sigma_window(w["mid"].to_numpy(), w["s_t"].to_numpy(), float(w["s_0"][0]), w["tau_s"].to_numpy())
        if np.std(w["ret"].to_numpy()) < 1e-4:
            sig, rsq = float("nan"), float("nan")  # spot did not move: no information about σ
        i0, i1 = np.searchsorted(qs, st), np.searchsorted(qs, st + WINDOW_S)
        lp, gp = qm[i0:i1 + 1], qg[i0:i1 + 1]
        r1 = np.diff(lp)
        r1[gp[1:] > 5] = np.nan
        rv1 = float(np.nansum(r1 * r1))
        r60 = lp[60::60] - lp[:-60:60]
        rv60 = float(np.nansum(r60 * r60))
        ok_pts = w.filter(pl.col("sigma_point").is_not_nan() & (pl.col("sigma_point") > 0))
        big = w.filter(pl.col("ret").abs() >= 3e-4)
        row = {"window_start_utc": datetime.fromtimestamp(st, timezone.utc).strftime("%Y-%m-%d %H:%M"), "start_s": st,
               "n_sec": int(w.height), "n_fit": n, "s_0": float(w["s_0"][0]), "move_bp": bp(float(lp[-1] - lp[0])) if len(lp) > 1 else float("nan"),
               "sigma_impl_15m_bp": sig * math.sqrt(WINDOW_S) * 1e4 if sig == sig else float("nan"), "sigma_impl_ann": annualise(sig, 1) if sig == sig else float("nan"),
               "intercept": a, "r2": rsq, "point_median_15m_bp": float(ok_pts["point_15m_bp"].median()) if ok_pts.height else float("nan"), "n_points": int(ok_pts.height),
               "realized_1s_15m_bp": bp(math.sqrt(rv1)), "realized_60s_15m_bp": bp(math.sqrt(rv60)),
               "share_sign_disagree_ge3bp": float(big["sign_disagree"].mean()) if big.height else float("nan"), "n_ge3bp": int(big.height)}
        if fc is not None:
            f = fc.filter(pl.col("sec") == st)
            if f.height:
                row["ewma_fc_15m_bp"] = bp(float(f["ewma_15m"][0])) if f["ewma_15m"][0] is not None else float("nan")
                row["season_fc_15m_bp"] = bp(float(f["fc_15m"][0])) if f["fc_15m"][0] is not None else float("nan")
        rows.append(row)
    out = pl.DataFrame(rows).with_columns((pl.col("sigma_impl_15m_bp") / pl.col("realized_1s_15m_bp")).alias("impl_over_realized_1s"),
                                          (pl.col("sigma_impl_15m_bp") / pl.col("realized_60s_15m_bp")).alias("impl_over_realized_60s"))
    if "season_fc_15m_bp" in out.columns:
        out = out.with_columns((pl.col("sigma_impl_15m_bp") / pl.col("season_fc_15m_bp")).alias("impl_over_fc"))
    return out.sort("start_s"), g.select("asset_id", "start_s", "sec", "mid", "s_t", "s_0", "tau_s", "ret", "sigma_point", "point_15m_bp")


# ======================================================================================
# 5. Outputs: tables, plots, the σ(t) Parquet, one summary page; CLI driver
# ======================================================================================


@dataclass
class VolConfig:
    date: str
    data_dir: Path  # Polymarket dataset root (events/, reference/)
    out_dir: Path
    spot_dir: Path | None = None  # root holding spot/ and backfill/ (default: data_dir)
    replay_out: Path | None = None  # dir holding derived/ (default: reports/<date>/replay)
    strict_path: Path | None = None  # clean_windows_strict.parquet (default: reports/<date>/)
    plots: bool = True


def _fmt(v) -> str:
    if v is None:
        return "–"
    if isinstance(v, float):
        if v != v:
            return "nan"
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        if abs(v) >= 10:
            return f"{v:.2f}"
        return f"{v:.4g}"
    return str(v)


def md_table(df: pl.DataFrame, max_rows: int = 60) -> str:
    if df is None or not df.height:
        return "_(empty)_\n"
    cols = df.columns
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for row in df.head(max_rows).iter_rows():
        lines.append("| " + " | ".join(_fmt(v) for v in row) + " |")
    if df.height > max_rows:
        lines.append(f"| … {df.height - max_rows} more rows | " + " | ".join("" for _ in cols[1:]) + " |")
    return "\n".join(lines) + "\n"


def write_tables(out: Path, prefix: str, tables: dict[str, pl.DataFrame]) -> None:
    (out / "tables").mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        if df is not None and df.height:
            df.write_csv(out / "tables" / f"{prefix}_{name}.csv")


def make_plots(out: Path, btc: BtcResult, poly: PolyResult | None, date: str) -> list[str]:
    """PNGs under `out`; returns the file names. matplotlib is imported here so the module
    (and the tests) do not need it: run with `uv run --with matplotlib polymm vol …`."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        log.warning("matplotlib not installed: no plots (run with `uv run --with matplotlib`)")
        return []
    made = []

    def save(fig, name):
        fig.tight_layout()
        fig.savefig(out / name, dpi=110)
        plt.close(fig)
        made.append(name)

    fc = btc.series.get("forecasts_1m")
    if fc is not None and fc.height:
        fig, ax = plt.subplots(figsize=(12, 4))
        tt = [datetime.fromtimestamp(s, timezone.utc) for s in fc["minute"].to_list()]
        for lam in LAMBDAS:
            ax.plot(tt, np.sqrt(fc[f"ewma_{lam:.2f}"].to_numpy()) * math.sqrt(MIN_PER_YEAR), lw=0.9, label=f"EWMA λ={lam}")
        ax.plot(tt, np.sqrt(fc["rv60"].to_numpy()) * math.sqrt(MIN_PER_YEAR), lw=0.7, alpha=0.6, label="rolling 60 min RV")
        ax.set_ylabel("σ annualised"); ax.set_title(f"BTC/USDT σ from 1-min returns (Binance, exchange clock) — provisional, {len(tt)} minutes")
        ax.legend(fontsize=8); ax.grid(alpha=0.3)
        save(fig, "btc_sigma_timeline.png")
    bh = btc.tables.get("by_hour_utc")
    if bh is not None and bh.height:
        fig, ax = plt.subplots(figsize=(9, 3.5))
        ax.bar(bh["hour"].to_numpy(), bh["sigma_15m_bp"].to_numpy(), color="#4477aa")
        ax.set_xlabel("hour of day (UTC); 12:00 ET = 17 UTC"); ax.set_ylabel("σ per 15 min (bp)")
        ax.set_title(f"BTC σ by hour of day from 1-min returns, {int(bh['n_1m'].sum())} minutes over {btc.chosen.get('n_days_1m', '?')} days — provisional")
        ax.grid(alpha=0.3, axis="y")
        save(fig, "btc_hour_of_day.png")
    sg = btc.tables.get("signature")
    if sg is not None and sg.height:
        fig, ax = plt.subplots(figsize=(7, 3.8))
        ax.plot(sg["delta_s"].to_numpy(), sg["var_per_s_trades_bp2"].to_numpy(), "o-", label="last trade (history, 3 days)")
        if sg["var_per_s_quotes_bp2"].is_not_nan().any():
            ax.plot(sg["delta_s"].to_numpy(), sg["var_per_s_quotes_bp2"].to_numpy(), "s--", label="quote mid (live, our clock)")
        ax.set_xscale("log"); ax.set_xlabel("sampling interval Δ (s)"); ax.set_ylabel("variance per second (bp²/s)")
        ax.set_title("Signature plot: Σ r_Δ² / (n·Δ) vs Δ — flat = no microstructure noise"); ax.legend(fontsize=8); ax.grid(alpha=0.3)
        save(fig, "btc_signature.png")
    mv = btc.series.get("moves_15m")
    if mv is not None and mv.height:
        fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
        axes[0].hist(mv["move_bp"].to_numpy(), bins=40, color="#4477aa")
        sd = float(np.std(mv["move_bp"].to_numpy()))
        for s_ in (-sd, sd):
            axes[0].axvline(s_, color="k", ls="--", lw=0.8)
        axes[0].set_xlabel("15-min log move (bp)"); axes[0].set_title(f"15-min moves, n={mv.height}, std={sd:.1f} bp")
        lam = btc.chosen.get("lambda", 0.97)
        col = f"ewma_{lam:.2f}"
        if col in mv.columns:
            f = np.sqrt(mv[col].to_numpy() * 15) * 1e4
            axes[1].scatter(f, mv["abs_bp"].to_numpy(), s=8, alpha=0.6)
            lim = max(np.nanmax(f), np.nanmax(mv["abs_bp"].to_numpy())) * 1.05
            axes[1].plot([0, lim], [0, lim], "k--", lw=0.8, label="|move| = σ forecast")
            axes[1].plot([0, lim], [0, 2 * lim], "r:", lw=0.8, label="2σ")
            axes[1].set_xlabel(f"EWMA λ={lam} forecast σ_15m at window start (bp)"); axes[1].set_ylabel("|move| (bp)")
            axes[1].legend(fontsize=8); axes[1].set_title("forecast vs realized")
        save(fig, "btc_moves_15m.png")
    if poly is not None:
        sx = poly.tables.get("sigma_x_family")
        bm = poly.tables.get("btc15_by_minute")
        lh = poly.tables.get("ladder_by_hours_to_expiry")
        if sx is not None and sx.height:
            fig, axes = plt.subplots(1, 3, figsize=(14, 3.8))
            s10 = sx.filter(pl.col("k_s") == 10)
            axes[0].bar(s10["family"].to_list(), s10["sigma_x"].to_numpy(), color="#4477aa")
            axes[0].set_yscale("log"); axes[0].set_ylabel("std Δx over 10 s (log-odds)"); axes[0].set_title("σ_x(10 s) per family, mid in [0.02, 0.98]")
            for i, (f_, n_) in enumerate(zip(s10["family"].to_list(), s10["n"].to_list())):
                axes[0].text(i, s10["sigma_x"][i], f"n={n_:,}", ha="center", va="bottom", fontsize=7)
            if bm is not None and bm.height:
                axes[1].bar(bm["minute"].to_numpy(), bm["sigma_x"].to_numpy(), color="#cc6677")
                axes[1].set_xlabel("minute within the 15-min window"); axes[1].set_title("btc_15m: σ_x(10 s) vs minute")
            if lh is not None and lh.height:
                axes[2].bar(lh["h_bin"].cast(pl.String).to_list(), lh["sigma_x"].to_numpy(), color="#228833")
                axes[2].set_xlabel("hours to 12:00 ET"); axes[2].set_title("ladder: σ_x(10 s) vs time to resolution")
            for ax in axes:
                ax.grid(alpha=0.3, axis="y")
            save(fig, "poly_sigma_x.png")
        pc = poly.tables.get("sigma_p_check_all")
        if pc is not None and pc.height:
            fig, ax = plt.subplots(figsize=(7, 3.8))
            pm = pc["p_mean"].to_numpy()
            ax.plot(pm, pc["sigma_p/sigma_x"].to_numpy(), "o", label="std(Δp)/std(Δx) per p bin (10 s)")
            pp = np.linspace(0.01, 0.99, 200)
            ax.plot(pp, pp * (1 - pp), "k--", lw=0.9, label="p(1−p)")
            ax.set_xlabel("p (mid)"); ax.set_ylabel("σ_p / σ_x"); ax.legend(fontsize=8); ax.grid(alpha=0.3)
            ax.set_title("σ_p ≈ p(1−p)·σ_x check, all families, strict windows")
            save(fig, "poly_sigma_p_check.png")
        im = poly.tables.get("implied_15m")
        if im is not None and im.height:
            ok = im.filter(pl.col("sigma_impl_15m_bp").is_not_nan())
            fig, ax = plt.subplots(figsize=(11, 3.8))
            xs = np.arange(ok.height)
            w = 0.27
            ax.bar(xs - w, ok["sigma_impl_15m_bp"].to_numpy(), w, label="implied (window regression)")
            ax.bar(xs, ok["realized_60s_15m_bp"].to_numpy(), w, label="realized in window (60 s RV)")
            if "season_fc_15m_bp" in ok.columns:
                ax.bar(xs + w, ok["season_fc_15m_bp"].to_numpy(), w, label="forecast at start (chosen estimator)")
            ax.set_xticks(xs); ax.set_xticklabels(ok["window_start_utc"].to_list(), rotation=60, fontsize=7)
            ax.set_ylabel("σ per 15 min (bp)"); ax.legend(fontsize=8); ax.grid(alpha=0.3, axis="y")
            ax.set_title(f"btc_15m: σ implied by the Up mid vs BTC realized, {ok.height} windows — provisional")
            save(fig, "btc15m_implied_vs_realized.png")
    return made


def render_summary(cfg: VolConfig, btc: BtcResult, poly: PolyResult | None, plots: list[str]) -> str:
    L = [f"# Volatility — {cfg.date}", "",
         f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} by `polymm vol`. Every number is provisional "
         "(three days of Binance history, one afternoon of joint Binance/Polymarket data). n is given with each table.", "",
         "Formulas: σ_step = sqrt(mean r²); σ_ann = σ_step·sqrt(31 536 000/step_s) (365-day year); "
         "σ_15m = σ_step·sqrt(900/step_s); EWMA σ²_t = λσ²_{t−1} + (1−λ)r_t²; QLIKE = mean(r²/σ̂² − ln(r²/σ̂²) − 1); "
         "x = ln(p/(1−p)); σ_p ≈ p(1−p)σ_x; implied σ solves mid = Φ(ln(S_t/S_0)/(σ√τ)).", ""]
    L += ["## BTC (Binance)", ""]
    for n in btc.notes:
        L.append(f"- {n}")
    L.append(f"- chosen 15-min estimator: **{btc.chosen.get('estimator')}** (QLIKE {_fmt(btc.chosen.get('qlike'))}, n={btc.chosen.get('n')})")
    L.append("")
    order = ["realized_by_day", "rolling_ewma", "by_hour_utc", "by_day", "signature", "moves_15m_dist", "moves_15m_by_hour",
             "forecast_scores", "hour_profile_used", "jumps_1m_by_day", "jumps_1m", "jumps_1m_by_hour", "jumps_1s_by_day", "jumps_1s_top", "jumps_1s_quotes"]
    for k in order:
        if k in btc.tables:
            L += [f"### btc · {k}", "", md_table(btc.tables[k])]
    if poly is not None:
        L += ["## Polymarket (strict clean windows, outcome-0 tokens)", ""]
        for n in poly.notes:
            L.append(f"- {n}")
        L.append("")
        for k, df in poly.tables.items():
            L += [f"### poly · {k}", "", md_table(df)]
    if plots:
        L += ["## Plots", ""] + [f"![{p}]({p})" for p in plots] + [""]
    return "\n".join(L)


def run_vol(cfg: VolConfig) -> tuple[BtcResult, PolyResult | None]:
    out = cfg.out_dir
    out.mkdir(parents=True, exist_ok=True)
    spot_dir = cfg.spot_dir or cfg.data_dir
    t0 = time.time()
    btc = btc_analysis(spot_dir, cfg.date)
    log.info("btc analysis done in %.1fs", time.time() - t0)
    poly = None
    derived = (cfg.replay_out or (Path("reports") / cfg.date / "replay")) / "derived"
    strict_path = cfg.strict_path or (Path("reports") / cfg.date / "clean_windows_strict.parquet")
    if derived.exists() and strict_path.exists():
        strict = pl.read_parquet(strict_path)
        t1 = time.time()
        poly = poly_analysis(cfg.data_dir, derived, strict, btc.series.get("btc_px_1s"), btc.series.get("quotes_1s"), btc.sigma_1s)
        log.info("poly analysis done in %.1fs", time.time() - t1)
    else:
        log.warning("Polymarket side skipped: derived=%s exists=%s, strict=%s exists=%s", derived, derived.exists(), strict_path, strict_path.exists())
    write_tables(out, "btc", btc.tables)
    if btc.sigma_1s is not None and btc.sigma_1s.height:
        btc.sigma_1s.write_parquet(out / "sigma_btc.parquet")
    if "moves_15m" in btc.series:
        btc.series["moves_15m"].write_parquet(out / "moves_15m.parquet")
    if poly is not None:
        write_tables(out, "poly", poly.tables)
        if "implied_points" in poly.series and poly.series["implied_points"].height:
            poly.series["implied_points"].write_parquet(out / "implied_points.parquet")
    plots = make_plots(out, btc, poly, cfg.date) if cfg.plots else []
    (out / "summary.md").write_text(render_summary(cfg, btc, poly, plots))
    return btc, poly

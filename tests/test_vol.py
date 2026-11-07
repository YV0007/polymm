"""Volatility estimators on synthetic paths: every function the report uses must recover
a known input. GBM paths are generated with a fixed seed; tolerances are a few per cent,
which is the sampling error of the path lengths used, not a loose bound."""
from __future__ import annotations

import math

import numpy as np
import polars as pl
import pytest

from polymm.vol import (
    MIN_PER_YEAR,
    SEC_PER_YEAR,
    WINDOW_S,
    annualise,
    dense_grid,
    ewma_variance,
    expit,
    garman_klass_variance,
    hour_profile,
    implied_sigma_point,
    implied_sigma_window,
    log_returns,
    logit,
    mse,
    norm_cdf,
    norm_ppf,
    parkinson_variance,
    qlike,
    realized_sigma,
    rolling_sigma,
    scale_horizon,
    sigma_15m_from_annual,
    sigma_p_from_sigma_x,
    variance_per_second,
)


def gbm(sigma_ann: float, step_s: float, n: int, seed: int = 0, s0: float = 75_000.0) -> np.ndarray:
    """Discretised GBM: ln S_{t+1} = ln S_t + (−σ²/2)Δt + σ√Δt ε. Returns the price path."""
    rng = np.random.default_rng(seed)
    dt = step_s / SEC_PER_YEAR
    r = -0.5 * sigma_ann ** 2 * dt + sigma_ann * math.sqrt(dt) * rng.standard_normal(n)
    return s0 * np.exp(np.cumsum(np.insert(r, 0, 0.0)))


# ---- realized σ and scaling -------------------------------------------------------------


def test_realized_sigma_recovers_gbm_input_at_1s_and_1min():
    sigma = 0.5
    p1 = gbm(sigma, 1, 86_400 * 2, seed=1)  # two days of seconds
    est = annualise(realized_sigma(log_returns(p1)), 1)
    assert abs(est / sigma - 1) < 0.02  # std error of σ̂ ≈ 1/sqrt(2n) ≈ 0.2 %
    p60 = gbm(sigma, 60, 1440 * 10, seed=2)  # ten days of minutes
    est60 = annualise(realized_sigma(log_returns(p60)), 60)
    assert abs(est60 / sigma - 1) < 0.03


def test_rolling_sigma_matches_full_sample_on_constant_vol():
    r = log_returns(gbm(0.4, 60, 5000, seed=3))
    rs = rolling_sigma(r, 500)
    assert np.isnan(rs[:499]).all() and np.isfinite(rs[499:]).all()
    assert abs(rs[499:].mean() / realized_sigma(r) - 1) < 0.05
    # window of 1 is |r|
    # cumsum differences lose a few digits on the smallest |r|; the identity holds to 1e-5
    np.testing.assert_allclose(rolling_sigma(r, 1), np.abs(r), rtol=1e-5, atol=1e-9)


def test_horizon_scaling_and_annualisation_are_consistent():
    s_step = 7e-4  # per minute
    assert annualise(s_step, 60) == pytest.approx(s_step * math.sqrt(MIN_PER_YEAR))
    assert scale_horizon(s_step, 60, WINDOW_S) == pytest.approx(s_step * math.sqrt(15))
    # σ_15m from the annual number equals σ_15m from the per-minute number
    assert sigma_15m_from_annual(annualise(s_step, 60)) == pytest.approx(scale_horizon(s_step, 60, WINDOW_S))
    # GBM check: 15-min moves have std σ·sqrt(900/year)
    sigma = 0.6
    p = gbm(sigma, WINDOW_S, 20_000, seed=4)
    assert abs(np.std(log_returns(p)) / sigma_15m_from_annual(sigma) - 1) < 0.03


# ---- EWMA --------------------------------------------------------------------------------


def test_ewma_recursion_matches_explicit_loop_and_converges():
    rng = np.random.default_rng(5)
    r = rng.standard_normal(500) * 1e-3
    lam = 0.94
    out = ewma_variance(r, lam, init=1e-6)
    v = 1e-6
    for i, x in enumerate(r):
        v = lam * v + (1 - lam) * x * x
        assert out[i] == pytest.approx(v)
    # constant |r|: σ² → r² geometrically, within (λ^n) of the limit
    c = ewma_variance(np.full(300, 2e-3), 0.97, init=0.0)
    assert abs(c[-1] / 4e-6 - 1) < 0.97 ** 300 * 1.01 + 1e-9
    # on a constant-vol GBM the EWMA σ averages to the true σ
    r = log_returns(gbm(0.5, 60, 20_000, seed=6))
    s = np.sqrt(ewma_variance(r, 0.97)[200:])
    assert abs(annualise(s.mean(), 60) / 0.5 - 1) < 0.05


def test_ewma_default_init_is_mean_of_first_memory_length():
    r = np.array([1e-3, 2e-3, 3e-3, 4e-3])
    out = ewma_variance(r, 0.5)  # 1/(1-λ) = 2 → init = mean(r[:2]²)
    init = np.mean(r[:2] ** 2)
    assert out[0] == pytest.approx(0.5 * init + 0.5 * r[0] ** 2)


# ---- range estimators ----------------------------------------------------------------------


def _bars(sigma: float, n_bars: int, steps_per_bar: int = 600, seed: int = 7):
    p = gbm(sigma, 60 / steps_per_bar, n_bars * steps_per_bar, seed=seed)
    o = p[:-1].reshape(n_bars, steps_per_bar)[:, 0]
    c = p[1:].reshape(n_bars, steps_per_bar)[:, -1]
    hi = p[1:].reshape(n_bars, steps_per_bar).max(axis=1)
    lo = p[1:].reshape(n_bars, steps_per_bar).min(axis=1)
    return o, hi, lo, c


def test_parkinson_and_garman_klass_recover_sigma_from_1min_bars():
    sigma = 0.5
    o, h, l, c = _bars(sigma, 4000)
    pk = annualise(math.sqrt(np.mean(parkinson_variance(h, l))), 60)
    gk = annualise(math.sqrt(np.mean(garman_klass_variance(o, h, l, c))), 60)
    # discrete sampling inside the bar under-samples the range (10 steps/bar here is coarse-ish): allow 8 %
    assert abs(pk / sigma - 1) < 0.08
    assert abs(gk / sigma - 1) < 0.08


# ---- forecast losses and the signature plot ---------------------------------------------------


def test_qlike_and_mse_are_minimised_at_the_true_variance():
    rng = np.random.default_rng(8)
    true_var = 4e-6
    y = (rng.standard_normal(20_000) * math.sqrt(true_var)) ** 2
    losses = {f: qlike(np.full(y.shape, f * true_var), y) for f in (0.5, 0.8, 1.0, 1.25, 2.0)}
    assert min(losses, key=losses.get) == 1.0
    assert losses[0.5] > losses[2.0]  # under-forecasting variance is penalised more
    m = {f: mse(np.full(y.shape, f * true_var), y) for f in (0.5, 1.0, 2.0)}
    assert min(m, key=m.get) == 1.0
    assert qlike(np.array([1.0, 1.0]), np.array([1.0, 1.0])) == pytest.approx(0.0)


def test_variance_per_second_is_flat_for_diffusion_and_rises_with_noise():
    sigma = 0.5
    p = gbm(sigma, 1, 86_400 * 3, seed=9)
    lp = np.log(p)
    v1, n1 = variance_per_second(lp, 1)
    v60, n60 = variance_per_second(lp, 60)
    assert n1 == len(lp) - 1 and n60 == (len(lp) - 1) // 60
    assert abs(v1 / v60 - 1) < 0.05
    assert abs(annualise(math.sqrt(v1), 1) / sigma - 1) < 0.02
    # add iid noise (bid-ask bounce): short intervals inflate by 2η²/k
    eta = 1e-4  # 2η²/k: ×2.5 at k = 1 s, +4 % at k = 60 s
    noisy = lp + np.random.default_rng(10).standard_normal(lp.shape) * eta
    vn1, _ = variance_per_second(noisy, 1)
    vn60, _ = variance_per_second(noisy, 60)
    assert vn1 > 1.5 * v1 and abs(vn60 / v60 - 1) < 0.1


# ---- log-odds ------------------------------------------------------------------------------------


def test_logit_expit_roundtrip_and_sigma_p_relation():
    p = np.array([0.02, 0.1, 0.5, 0.9, 0.98])
    np.testing.assert_allclose(expit(logit(p)), p)
    # x Brownian with σ_x: for small steps std(Δp) ≈ p(1−p)σ_x
    rng = np.random.default_rng(11)
    sigma_x = 0.02
    for p0 in (0.1, 0.5, 0.8):
        x0 = logit(p0)
        dx = rng.standard_normal(200_000) * sigma_x
        dp = expit(x0 + dx) - p0
        assert abs(np.std(dp) / sigma_p_from_sigma_x(p0, sigma_x) - 1) < 0.01


def test_norm_cdf_ppf_roundtrip():
    x = np.linspace(-5, 5, 101)
    np.testing.assert_allclose(norm_ppf(norm_cdf(x)), x, atol=1e-7)
    assert norm_cdf(0.0) == pytest.approx(0.5)
    assert norm_ppf(np.array([0.975]))[0] == pytest.approx(1.959964, abs=1e-6)


def test_implied_sigma_recovers_the_sigma_that_priced_the_window():
    sigma_s = 0.5 / math.sqrt(SEC_PER_YEAR)  # per √second
    s0 = 75_000.0
    tau = np.arange(899, 0, -1, dtype=float)
    path = gbm(0.5, 1, 899, seed=12, s0=s0)[1:]
    mid = norm_cdf(np.log(path / s0) / (sigma_s * np.sqrt(tau)))
    pt = implied_sigma_point(mid, path, s0, tau)
    ok = np.isfinite(pt)
    assert ok.sum() > 100
    np.testing.assert_allclose(pt[ok], sigma_s, rtol=1e-6)
    sig, a, n, r2 = implied_sigma_window(mid, path, s0, tau)
    assert sig == pytest.approx(sigma_s, rel=1e-6) and abs(a) < 1e-6 and r2 > 0.999
    # a reference-price offset between Binance and the market's S_0 goes into the intercept, not σ
    mid2 = norm_cdf(np.log(path / (s0 * 1.0005)) / (sigma_s * np.sqrt(tau)))
    sig2, a2, _, _ = implied_sigma_window(mid2, path, s0, tau)
    assert sig2 == pytest.approx(sigma_s, rel=1e-6) and a2 < 0


# ---- grid helpers ---------------------------------------------------------------------------


def test_dense_grid_forward_fills_and_tracks_gaps():
    df = pl.DataFrame({"sec": [10, 11, 15], "px": [1.0, 2.0, 3.0]})
    g = dense_grid(df, "sec", ["px"])
    assert g["sec"].to_list() == [10, 11, 12, 13, 14, 15]
    assert g["px"].to_list() == [1.0, 2.0, 2.0, 2.0, 2.0, 3.0]
    assert g["gap_s"].to_list() == [0, 0, 1, 2, 3, 0]
    assert g["has_obs"].to_list() == [True, True, False, False, False, True]


def test_hour_profile_is_normalised_and_excludes_a_day():
    m = np.arange(0, 3 * 86_400, 60)
    rng = np.random.default_rng(13)
    hour = (m % 86_400) // 3600
    r = rng.standard_normal(m.shape) * 1e-3 * np.where(hour == 15, 3.0, 1.0)
    prof = hour_profile(m, r)
    assert prof[15] > 5 * prof[3]
    assert abs(np.mean(prof) - 1) < 0.1
    assert np.allclose(hour_profile(m, r, exclude_day=0), hour_profile(m[m >= 86_400], r[m >= 86_400]))

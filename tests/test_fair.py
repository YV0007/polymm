"""Fair-value tests: the scoring ruler on planted series, the model's limits, PAV, Fed
normalisation, the blend recovering a planted weight, and the no-lookahead guard."""
from __future__ import annotations

import math

import numpy as np
import polars as pl
import pytest

from polymm.fair.blend import fit_w_smooth, w_table
from polymm.fair.inputs import check_inputs, logit
from polymm.fair.models import add_fed, add_models, norm_cdf, pav_decreasing, tau_eff_15m
from polymm.fair.score import score

RNG = np.random.default_rng(7)


def _walk(n_clusters=60, n=300):
    """Random-walk log-odds mids, one cluster per 'window', with future mids attached."""
    rows = []
    for c in range(n_clusters):
        x = np.cumsum(RNG.normal(0, 0.05, n + 61))
        for i in range(n):
            rows.append((f"c{c}", i, x[i], x[i + 1], x[i + 5], x[i + 30], x[i + 60]))
    df = pl.DataFrame(rows, schema=["cluster", "sec", "x_mid", "x_mid_f1", "x_mid_f5", "x_mid_f30", "x_mid_f60"], orient="row")
    return df.with_columns((1 / (1 + (-pl.col("x_mid")).exp())).alias("mid"), pl.lit(None, pl.Float64).alias("outcome"))


def test_score_mid_has_no_skill():
    df = _walk()
    s = score(df, "x_mid")
    assert np.allclose(s["skill"].to_numpy(), 0.0)


def test_score_planted_perfect_forecast():
    df = _walk().with_columns(pl.col("x_mid_f5").alias("x_perfect"))
    s = score(df, "x_perfect", taus=(5,)).row(0, named=True)
    assert s["skill"] == pytest.approx(1.0) and s["r2"] == pytest.approx(1.0) and s["b"] == pytest.approx(1.0)
    assert s["hit"] == pytest.approx(1.0)


def test_score_noise_has_no_skill():
    df = _walk().with_columns((pl.col("x_mid") + pl.Series(RNG.normal(0, 0.05, 18000))).alias("x_noise"))
    s = score(df, "x_noise", taus=(5,)).row(0, named=True)
    assert s["skill"] < 0 and abs(s["b"]) < 0.1 and abs(s["hit"] - 0.5) < 0.03 and s["r2"] < 0.01
    assert s["n_clusters"] == 60 and s["skill_se"] > 0


def test_score_half_signal_gives_b_two():
    # a candidate that predicts half of the next move: the mid moves twice as far as the gap, b ≈ 2
    df = _walk().with_columns((pl.col("x_mid") + 0.5 * (pl.col("x_mid_f1") - pl.col("x_mid"))).alias("x_half"))
    s = score(df, "x_half", taus=(1,)).row(0, named=True)
    assert s["b"] == pytest.approx(2.0, rel=1e-6) and s["skill"] == pytest.approx(0.75, abs=0.01)


def test_tau_eff_continuous_and_limits():
    t = pl.DataFrame({"tau": [900.0, 61.0, 60.0, 59.999, 30.0, 0.0]})
    v = t.select(tau_eff_15m(pl.col("tau"))).to_series().to_list()
    assert v[0] == 860 and v[2] == pytest.approx(20) and v[3] == pytest.approx(20, abs=1e-3)
    assert v[4] == pytest.approx(30 ** 3 / 10800) and v[5] == 0


def test_norm_cdf_accuracy():
    for x in (-3.0, -1.0, 0.0, 0.5, 2.0):
        assert norm_cdf(np.array([x]))[0] == pytest.approx(0.5 * (1 + math.erf(x / math.sqrt(2))), abs=2e-7)


def _model_frame(**over):
    base = dict(family="btc_15m", sec=1000, t_start_s=500, t_end_s=1400, tau=400.0, K=70000.0, S=70000.0, A_known=None,
                sigma_s=0.0001, basis0=0.0, basis_T=0.0, rv30_s=0.0001, S_zero=70000.0, A_known_zero=None, S_lead1=70000.0)
    base.update(over)
    return pl.DataFrame([base]).with_columns(pl.col("A_known").cast(pl.Float64), pl.col("A_known_zero").cast(pl.Float64))


def test_model_at_the_money_is_half_and_basis_shifts_it():
    x = add_models(_model_frame())["x_model"].item()
    assert x == pytest.approx(0.0, abs=1e-6)
    # Binance 10 bp above Chainlink at the start: the same Binance price is exactly at the strike
    x2 = add_models(_model_frame(S=70000 * math.exp(0.001), basis0=0.001))["x_model"].item()
    assert x2 == pytest.approx(0.0, abs=1e-6)


def test_model_last_minute_uses_known_average():
    # 30 s left, the first 30 s of the average sat 20 bp above the strike, spot now at the strike:
    # E[A] = K·(1 + 0.001) > K, and with little variance left the model is confident in Up
    f = _model_frame(tau=30.0, sec=1370, S=70000.0, A_known=70000 * 1.002)
    p = 1 / (1 + math.exp(-add_models(f)["x_model"].item()))
    assert p > 0.99


def test_ladder_model_decreasing_in_strike():
    f = pl.DataFrame({"family": ["btc_daily_ladder"] * 3, "K": [68000.0, 70000.0, 72000.0], "S": [70000.0] * 3,
                      "tau": [36000.0] * 3, "sigma_s": [0.0001] * 3, "sec": [0] * 3, "t_start_s": [None] * 3, "t_end_s": [36000] * 3})
    f = f.with_columns([pl.lit(None, pl.Float64).alias(c) for c in ("A_known", "basis0", "basis_T", "rv30_s", "A_known_zero")]
                       ).with_columns(pl.col("S").alias("S_zero"), pl.col("S").alias("S_lead1"), pl.col("t_start_s").cast(pl.Int64))
    x = add_models(f)["x_model"].to_list()
    assert x[0] > x[1] > x[2] and x[1] == pytest.approx(0.0, abs=1e-6)


def test_pav_decreasing():
    assert pav_decreasing(np.array([0.9, 0.7, 0.8, 0.2])).tolist() == pytest.approx([0.9, 0.75, 0.75, 0.2])
    assert pav_decreasing(np.array([0.1, 0.2, 0.3])).tolist() == pytest.approx([0.2, 0.2, 0.2])


def test_fed_normalised_sums_to_one():
    g = pl.DataFrame({"family": ["fed_dec_2026"] * 6, "condition_id": ["a", "b", "c"] * 2, "sec": [1, 1, 1, 2, 2, 2],
                      "micro": [0.5, 0.3, 0.3, 0.6, 0.3, 0.2]})
    out, st = add_fed(g)
    p = out.with_columns((1 / (1 + (-pl.col("x_fed")).exp())).alias("p")).group_by("sec").agg(pl.col("p").sum())
    assert np.allclose(p["p"].to_numpy(), 1.0, atol=1e-9) and st["markets"] == 3


def test_blend_recovers_planted_weight():
    n = 20000
    xb = RNG.normal(0, 1, n)
    xm = xb + RNG.normal(0, 0.5, n)
    fut = xb + 0.7 * (xm - xb) + RNG.normal(0, 0.2, n)
    df = pl.DataFrame({"x_micro": xb, "x_model": xm, "x_mid_f30": fut, "cluster": [f"c{i % 100}" for i in range(n)],
                       "f1": RNG.uniform(-1, 1, n), "all": ["a"] * n})
    t = w_table(df, "x_model", "x_micro", 30, ["all"]).row(0, named=True)
    assert t["w_hat"] == pytest.approx(0.7, abs=0.02)
    beta = fit_w_smooth(df, "x_model", "x_micro", 30, ["f1"])
    assert 1 / (1 + math.exp(-beta[0])) == pytest.approx(0.7, abs=0.02) and abs(beta[1]) < 0.1


def test_check_inputs_catches_lookahead():
    g = pl.DataFrame({"sec": [10], "tob_ts": [11 * 10 ** 9], "S_ts": [9 * 10 ** 9], "sigma_age_s": [5.0], "family": ["x"],
                      "S": [1.0], "S_src": ["live"], "S_age_s": [1.0], "sigma_s": [0.1], "basis0": [0.0]})
    with pytest.raises(AssertionError, match="lookahead"):
        check_inputs(g)


def test_logit_clips():
    v = pl.DataFrame({"p": [0.0, 0.5, 1.0]}).select(logit(pl.col("p"))).to_series().to_list()
    assert v[1] == 0 and v[0] == pytest.approx(math.log(1e-3 / (1 - 1e-3))) and v[2] == pytest.approx(-v[0])


def test_dates_between_includes_middle_day():
    from polymm.fair.inputs import _dates_between, day_bounds
    d0, d1 = day_bounds("2026-11-06")
    assert _dates_between(d0 - 1200, d1 + 2) == ["2026-11-05", "2026-11-06", "2026-11-07"]


def test_anchor_takes_level_from_book_and_moves_from_model():
    from polymm.fair.models import add_anchor
    n = 400
    model = np.r_[np.full(n // 2, 1.0), np.full(n // 2, 1.3)]  # model level 1.0 off the book, then jumps +0.3
    g = pl.DataFrame({"asset_id": ["a"] * n, "sec": np.arange(n), "x_book": np.zeros(n), "x_model": model})
    a = add_anchor(g, "x_book", "x_model", 5, "x_anch")["x_anch"].to_numpy()
    assert abs(a[n // 2 - 1]) < 1e-6                  # settled: anchored model = book level
    assert 0.25 < a[n // 2] < 0.3                      # the jump passes through at once (minus one step of decay)
    assert abs(a[-1]) < 1e-3                           # and is absorbed into the level later


def test_w_fit_stays_finite_on_tiny_degenerate_data():
    df = pl.DataFrame({"x_micro": [0.0] * 200, "x_model": [1e-9] * 200, "x_mid_f30": [0.0] * 200,
                       "f1": np.linspace(-1, 1, 200), "cluster": ["c"] * 200})
    beta = fit_w_smooth(df, "x_model", "x_micro", 30, ["f1"])
    assert np.all(np.isfinite(beta)) and np.all(np.abs(beta) < 50)

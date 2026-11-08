"""Markout tests: the YES-equivalent sign (the one place a sign can flip), the engine's
reference/horizon/window rules, planted informed vs uninformed flow, cluster SEs, the τ* rule,
and the Gate-A rule that a slice must hold on every held-out day."""
from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from polymm.markouts.analysis import tau_star, verdict
from polymm.markouts.engine import add_markouts
from polymm.markouts.fills import NS, to_yes_equivalent
from polymm.markouts.stats import grouped_mean

RNG = np.random.default_rng(11)


def test_yes_equivalent_four_cases():
    tr = pl.DataFrame({"oi": [0, 0, 1, 1], "side": ["BUY", "SELL", "BUY", "SELL"], "price": [0.52, 0.48, 0.30, 0.70]})
    f = to_yes_equivalent(tr)
    # taker buys YES at .52 → maker sold YES at .52
    # taker sells YES at .48 → maker bought YES at .48
    # taker buys NO at .30  = sells YES at .70 → maker bought YES at .70
    # taker sells NO at .70 = buys YES at .30 → maker sold YES at .30
    assert f["s"].to_list() == [-1, 1, 1, -1]
    assert f["taker_dir"].to_list() == [1, -1, -1, 1]
    assert f["P"].to_list() == pytest.approx([0.52, 0.48, 0.70, 0.30])


def _fills(rows):
    base = {"yes": "Y", "outcome": None, "rebate_ps": 0.0, "taker_fee_ps": 0.0, "fair0": None}
    return pl.DataFrame([{**base, **r} for r in rows], schema_overrides={"outcome": pl.Int8, "fair0": pl.Float64})


def test_engine_half_spread_horizons_and_window():
    t0 = 1000 * NS
    tob = pl.DataFrame({"t": [t0 - 5, t0 + 1, t0 + 5 * NS, t0 + 61 * NS], "yes": "Y", "mid": [0.515, 0.515, 0.55, 0.60]})
    f = _fills([{"t_recv": t0, "t_pre": t0 - 1, "t_last": t0 + 30 * NS, "t_end_s": 5000, "s": -1, "P": 0.52, "mid": 0.515}])
    m = add_markouts(f, tob, None, taus=(1, 5, 60)).row(0, named=True)
    assert m["h_mid"] == pytest.approx(0.5)           # sold at the ask, half a cent above the mid
    assert m["M1_mid"] == pytest.approx(0.5)          # nothing moved in 1 s
    assert m["M5_mid"] == pytest.approx(-3.0)         # mid 0.55 five seconds later: −3¢
    assert m["a5_mid"] == pytest.approx(3.5)          # h − M = adverse selection
    assert m["M60_mid"] is None                       # t + 60 s is past the clean window: no value


def test_engine_horizon_stops_at_market_close():
    t0 = 1000 * NS
    tob = pl.DataFrame({"t": [t0 - 5, t0 + 5 * NS], "yes": "Y", "mid": [0.50, 0.99]})
    # clean window runs on past the close (as for 15m books after their end); the close wins
    f = _fills([{"t_recv": t0, "t_pre": t0 - 1, "t_last": t0 + 100 * NS, "t_end_s": 1003, "s": 1, "P": 0.49, "mid": 0.50}])
    m = add_markouts(f, tob, None, taus=(1, 5)).row(0, named=True)
    assert m["M1_mid"] == pytest.approx(1.0)
    assert m["M5_mid"] is None                        # t + 5 s is after the market closed


def test_engine_fair_is_exact_mid_plus_grid_premium():
    """fair(t) = exact mid(t) + the fair-minus-mid premium of the last grid second ≤ t (log-odds)."""
    lg = lambda p: np.log(p / (1 - p))
    sg = lambda x: 1 / (1 + np.exp(-x))
    t0 = 1000 * NS + 300_000_000  # 0.3 s into second 1000
    grid = pl.DataFrame({"asset_id": "Y", "sec": [999, 1000, 1001, 1002], "mid": [0.50] * 4, "p_fair": [0.50, 0.52, 0.54, 0.56]})
    tob = pl.DataFrame({"t": [t0 - 10, t0 + 900_000_000], "yes": "Y", "mid": [0.505, 0.53]})
    f = _fills([{"t_recv": t0, "t_pre": t0 - 1, "t_last": t0 + 100 * NS, "t_end_s": 5000, "s": 1, "P": 0.50, "mid": 0.505}])
    m = add_markouts(f, tob, grid, taus=(1,)).row(0, named=True)
    assert m["m0_fair"] == pytest.approx(sg(lg(0.505) + lg(0.52) - lg(0.50)))   # premium of second 1000, mid at t_pre
    assert m["m1_fair"] == pytest.approx(sg(lg(0.53) + lg(0.54) - lg(0.50)))    # t + 1 s = 1001.3 → premium of 1001, never 1002


def _planted(informed: bool, n_paths=80, n=200):
    """Random-walk mids; takers trade every 5 s. Informed takers trade in the direction of the
    next 10 s move (so the maker is hit right before the price runs away); noise takers flip a coin."""
    tobs, fills = [], []
    for c in range(n_paths):
        mid = 0.5 + np.cumsum(RNG.normal(0, 0.003, n))
        for i in range(n):
            tobs.append((f"Y{c}", (1000 + i) * NS, float(mid[i])))
        for i in range(5, n - 15, 5):
            move = mid[i + 10] - mid[i]
            taker_buys = (move > 0) if informed else bool(RNG.integers(0, 2))
            s = -1 if taker_buys else 1
            fills.append({"yes": f"Y{c}", "t_recv": (1000 + i) * NS + 1, "t_pre": (1000 + i) * NS, "t_last": (1000 + n) * NS, "s": s,
                          "P": float(mid[i]) - s * 0.005, "mid": float(mid[i]), "cluster": f"Y{c}"})
    tob = pl.DataFrame(tobs, schema=["yes", "t", "mid"], orient="row").sort("t")
    f = pl.DataFrame(fills).with_columns(pl.lit(None, pl.Int8).alias("outcome"), pl.lit(0.0).alias("rebate_ps"),
                                         pl.lit(0.0).alias("taker_fee_ps"), pl.lit(None, pl.Float64).alias("fair0"))
    return add_markouts(f, tob, None, taus=(10,))


def test_planted_informed_flow_shows_adverse_selection():
    inf = grouped_mean(_planted(True), ["a10_mid"], []).row(0, named=True)
    noise = grouped_mean(_planted(False), ["a10_mid"], []).row(0, named=True)
    assert inf["mean"] - 3 * inf["se"] > 0            # informed: the price runs away after the fill
    assert abs(noise["mean"]) < 3 * noise["se"]        # noise: no systematic move


def test_cluster_se_is_not_the_naive_se():
    c = np.repeat(np.arange(200), 50)
    x = RNG.normal(0, 1, 200)[c] + RNG.normal(0, 1, c.size)   # a common shock per cluster
    r = grouped_mean(pl.DataFrame({"cluster": c.astype(str), "x": x}), ["x"], []).row(0, named=True)
    true_se = np.sqrt((1 + 1 / 50) / 200)
    assert 0.75 * true_se < r["se"] < 1.3 * true_se
    assert r["se"] > 3 * x.std() / np.sqrt(x.size)


def test_tau_star_finds_a_one_second_decay():
    n = 5000
    cl = RNG.integers(0, 100, n).astype(str)
    cols = {"family": ["btc_15m"] * n, "cluster": cl, "h_fair": RNG.normal(1.0, 1, n)}
    m = RNG.normal(0.0, 1, n)                        # all the decay happens between h and 1 s,
    for k in (1, 2, 5, 10, 30, 60):                  # after that the markout only diffuses
        m = m + RNG.normal(0.0, 0.3, n)
        cols[f"M{k}_fair"] = m.copy()
    for k in (120, 300, 900, 3600):
        cols[f"M{k}_fair"] = [None] * n
    info = {}
    out = tau_star(pl.DataFrame(cols, schema_overrides={f"M{k}_fair": pl.Float64 for k in (120, 300, 900, 3600)}), info=info)
    assert out["btc_15m"] == 1 and info["btc_15m"] == "estimated"
    assert out["fed_dec_2026"] == 60 and info["fed_dec_2026"].startswith("fallback")   # no fills


def test_verdict_needs_every_held_out_day():
    days = ["d2", "d3"]
    row = lambda d, pi: {"family": "btc_15m", "test_day": d, "slice": "spread", "value": ">5c", "metric": "M",
                         "test_pi": pi, "test_se": 0.1, "test_n": 500, "test_clusters": 40, "test_pi60": pi, "test_se60": 0.3}
    weak60 = {"slices": [{**row("d2", 1.0), "test_pi60": 0.1}, row("d3", 0.8)], "model": [], "unconditional": []}
    one_day = {"slices": [row("d2", 1.0), row("d3", -0.5)], "model": [], "unconditional": []}
    both = {"slices": [row("d2", 1.0), row("d3", 0.8)], "model": [], "unconditional": []}
    ts = {"btc_15m": 1, "btc_daily_ladder": 60, "fed_dec_2026": 60}
    assert verdict(one_day, ts, days)[2]["btc_15m"] == "NO-GO"
    assert verdict(both, ts, days)[2]["btc_15m"] == "GO"
    assert verdict(weak60, ts, days)[2]["btc_15m"] == "NO-GO"   # positive at τ* but not at 60 s with its SE
    assert verdict(both, ts, days, fallback={"fed_dec_2026"})[2]["fed_dec_2026"] == "INCONCLUSIVE"

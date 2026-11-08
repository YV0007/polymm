"""Plots for the fair-value report. Each answers one question; the caption is in the
research note, not baked into the image."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

NAMES = {"x_mid": "mid", "x_micro": "microprice", "x_stoikov": "Stoikov", "x_model": "BTC model", "x_fair": "fair (blend)",
         "x_fed": "Fed Σ=1", "x_or_sigma": "oracle σ", "x_or_basis": "oracle gap", "x_or_zero": "oracle zero-lag",
         "x_or_lead1": "oracle +1 s", "x_anch": "anchored model", "x_or_sigma_a": "oracle σ", "x_or_basis_a": "oracle gap",
         "x_or_zero_a": "oracle zero-lag", "x_or_lead1_a": "oracle +1 s"}
TAU_POS = {1: 0, 5: 1, 30: 2, 60: 3}  # evenly spaced categories, not a log axis (an empty panel broke log scaling)
COL = {"x_mid": "#9aa0a6", "x_micro": "#1f77b4", "x_stoikov": "#17becf", "x_model": "#d62728", "x_fair": "#2ca02c",
       "x_fed": "#9467bd", "x_anch": "#bcbd22", "x_or_sigma_a": "#ff7f0e", "x_or_basis_a": "#8c564b",
       "x_or_zero_a": "#e377c2", "x_or_lead1_a": "#7f7f7f", "x_or_sigma": "#ff7f0e", "x_or_basis": "#8c564b", "x_or_zero": "#e377c2", "x_or_lead1": "#7f7f7f"}


def make_plots(oos: pl.DataFrame, T: dict, G: pl.DataFrame, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    out.mkdir(parents=True, exist_ok=True)
    head = T.get("headline", pl.DataFrame())

    # 1. skill vs horizon per family
    # Why skip a family with no finite skill: the Fed mid did not move at all on the test days
    # (MSE of the mid = 0), so "skill vs mid" is undefined there.
    fams = [f for f in ("btc_15m", "btc_daily_ladder", "fed_dec_2026")
            if head.height and head.filter((pl.col("family") == f) & pl.col("skill").is_finite() & (pl.col("cand") != "x_mid")).height]
    if fams:
        fig, axes = plt.subplots(1, len(fams), figsize=(5.2 * len(fams), 4), squeeze=False)
        for ax, fam in zip(axes[0], fams):
            h = head.filter(pl.col("family") == fam)
            for c in h["cand"].unique(maintain_order=True).to_list():
                if c.startswith("x_or"):
                    continue
                r = h.filter(pl.col("cand") == c).sort("tau")
                ax.errorbar([TAU_POS[t] for t in r["tau"]], r["skill"], yerr=r["skill_se"], marker="o", capsize=3, label=NAMES.get(c, c), color=COL.get(c))
            ax.axhline(0, color="k", lw=0.8)
            ax.set_xticks(range(4)); ax.set_xticklabels(["1", "5", "30", "60"])
            # Why a zoomed axis: the raw model scores −150 to −250,000 at short horizons (its
            # level is off), which would flatten the candidates that matter into one line.
            top = float(h.filter(pl.col("skill").is_finite())["skill"].max())
            ax.set_ylim(-0.25, max(0.04, top * 1.4))
            ax.text(0.02, 0.03, "below the axis: raw and anchored model at short τ", transform=ax.transAxes, fontsize=7, color="#555")
            ax.set_title(fam); ax.set_xlabel("horizon τ (s)"); ax.set_ylabel("skill vs mid (held-out)")
            ax.legend(fontsize=8)
        fig.tight_layout(); fig.savefig(out / "skill_by_horizon.png", dpi=130); plt.close(fig)

    # 2. 15m skill by minute of the window (τ = 30)
    bm = T.get("by_minute_15m", pl.DataFrame())
    if bm.height:
        fig, ax = plt.subplots(figsize=(7, 4))
        b = bm.filter(pl.col("tau") == 30)
        mins = sorted(b["min_b"].unique().to_list())
        xs = np.arange(len(mins))
        cands = [c for c in ("x_micro", "x_stoikov", "x_anch", "x_fair") if c in b["cand"].to_list()]
        wbar = 0.8 / len(cands)
        for i, c in enumerate(cands):
            r = b.filter(pl.col("cand") == c)
            v = [r.filter(pl.col("min_b") == m)["skill"].item() if r.filter(pl.col("min_b") == m).height else np.nan for m in mins]
            e = [r.filter(pl.col("min_b") == m)["skill_se"].item() if r.filter(pl.col("min_b") == m).height else 0 for m in mins]
            ax.bar(xs + (i - (len(cands) - 1) / 2) * wbar, v, wbar, yerr=e, capsize=2, label=NAMES[c], color=COL[c])
        ax.axhline(0, color="k", lw=0.8); ax.set_xticks(xs); ax.set_xticklabels([f"min {m}" for m in mins])
        ax.set_ylabel("skill vs mid at τ = 30 s"); ax.set_title("btc_15m: who predicts the mid, by minute of the window"); ax.legend(fontsize=8)
        fig.tight_layout(); fig.savefig(out / "skill_by_minute_15m.png", dpi=130); plt.close(fig)

    # 3. ŵ(τ) curves
    w = T.get("w_hat", pl.DataFrame())
    if w.height:
        fig, axes = plt.subplots(1, 2, figsize=(11, 4))
        for ax, (fam, key) in zip(axes, (("btc_15m", "min_b"), ("btc_daily_ladder", "hte_b"))):
            ww = w.filter((pl.col("family") == fam) & pl.col(key).is_not_null())
            for k in sorted(ww[key].unique().to_list()):
                r = ww.filter(pl.col(key) == k).sort("tau")
                ax.errorbar([TAU_POS[t] for t in r["tau"]], r["w_hat"], yerr=r["w_se"], marker="o", capsize=3, label=k)
            ax.axhline(1, color="k", lw=0.6, ls="--"); ax.axhline(0, color="k", lw=0.6)
            ax.set_xticks(range(4)); ax.set_xticklabels(["1", "5", "30", "60"])
            ax.set_xlabel("horizon τ (s)"); ax.set_ylabel("ŵ = share of model–book gap closed"); ax.set_title(f"{fam} by {key}"); ax.legend(fontsize=8)
        fig.tight_layout(); fig.savefig(out / "w_hat_by_horizon.png", dpi=130); plt.close(fig)

    # 4. calibration (15m)
    c = T.get("calibration_15m", pl.DataFrame())
    if c.height:
        fig, ax = plt.subplots(figsize=(5, 5))
        for col, name, color in (("mid", "mid", "#9aa0a6"), ("p_model", "BTC model (raw)", "#d62728"), ("p_fair", "fair", "#2ca02c")):
            r = c.filter(pl.col("col") == col)
            if r.height:
                ax.plot(r["pred"], r["obs"], marker="o", color=color, label=f"{name} (Brier {r['brier'][0]:.4f})")
        ax.plot([0, 1], [0, 1], "k--", lw=0.8); ax.set_xlabel("predicted P(Up)"); ax.set_ylabel("share resolved Up")
        ax.set_title("btc_15m calibration, held-out seconds"); ax.legend(fontsize=8)
        fig.tight_layout(); fig.savefig(out / "calibration_15m.png", dpi=130); plt.close(fig)

    # 5. oracle bars (15m, τ = 30): what perfecting one input could buy
    if head.height:
        h = head.filter((pl.col("family") == "btc_15m") & (pl.col("tau") == 30))
        order = [c for c in ("x_anch", "x_or_sigma_a", "x_or_basis_a", "x_or_zero_a", "x_or_lead1_a") if c in h["cand"].to_list()]
        if order:
            fig, ax = plt.subplots(figsize=(7, 3.6))
            v = [h.filter(pl.col("cand") == c)["skill"].item() for c in order]
            e = [h.filter(pl.col("cand") == c)["skill_se"].item() for c in order]
            ax.bar([NAMES[c] for c in order], v, yerr=e, capsize=3, color=[COL[c] for c in order])
            ax.axhline(0, color="k", lw=0.8); ax.set_ylabel("skill vs mid at τ = 30 s")
            ax.set_title("btc_15m: anchored model with one input replaced by its true value")
            fig.tight_layout(); fig.savefig(out / "oracles_15m.png", dpi=130); plt.close(fig)

    # 6. one example window: mid, model, fair, and BTC
    e = oos.filter((pl.col("family") == "btc_15m") & pl.col("x_model").is_finite())
    if e.height:
        pick = (e.group_by("condition_id").agg(pl.len().alias("n"), (pl.col("p_model") - pl.col("mid")).abs().max().alias("gap"))
                .filter(pl.col("n") >= 800).sort("gap", descending=True))
        if pick.height:
            cid = pick["condition_id"][min(2, pick.height - 1)]
            r = e.filter(pl.col("condition_id") == cid).sort("sec")
            t = (r["sec"] - r["t_start_s"]).to_numpy() / 60
            fig, ax = plt.subplots(figsize=(9, 4.2))
            ax.plot(t, r["mid"], color="#9aa0a6", label="mid", lw=1.2)
            ax.plot(t, r["p_model"], color="#d62728", label="BTC model (raw)", lw=0.8, alpha=0.6)
            ax.plot(t, r["p_anch"], color="#bcbd22", label="anchored model", lw=1)
            ax.plot(t, r["p_fair"], color="#2ca02c", label="fair (blend)", lw=1.2)
            ax.set_ylim(0, 1); ax.set_xlabel("minute of the window"); ax.set_ylabel("P(Up)")
            ax2 = ax.twinx()
            ax2.plot(t, r["S"], color="#ff7f0e", lw=0.8, alpha=0.6, label="Binance mid")
            k_b = (r["K"] * (r["basis0"]).exp())[0]
            ax2.axhline(k_b, color="#ff7f0e", ls=":", lw=0.8)
            ax2.set_ylabel("BTC (Binance), dotted = strike on Binance scale")
            ax.set_title(f"{r['event_slug'][0]} (outcome {'Up' if r['outcome'][0] == 1 else 'Down' if r['outcome'][0] == 0 else '?'})")
            h1, l1 = ax.get_legend_handles_labels(); h2, l2 = ax2.get_legend_handles_labels()
            ax.legend(h1 + h2, l1 + l2, fontsize=8, loc="upper left")
            fig.tight_layout(); fig.savefig(out / "example_window_15m.png", dpi=130); plt.close(fig)

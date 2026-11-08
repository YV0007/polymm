"""Plots for the adverse-selection report. Each answers one question; captions live in
research/adverse_selection.md. Colours: the dataviz reference palette, fixed order
(blue = fair reference / model, orange = mid reference / gap scorer, grey = no-information baseline)."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

BLUE, ORANGE, AQUA, GREY, INK = "#2a78d6", "#eb6834", "#1baf7a", "#9a9a94", "#3a3a37"
FAM_TITLE = {"btc_15m": "BTC 15-minute", "btc_daily_ladder": "BTC daily ladder", "fed_dec_2026": "Fed December"}


def _style(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#e6e5df", lw=0.8)
    ax.set_axisbelow(True)
    ax.axhline(0, color=INK, lw=0.8)


def make_plots(F: pl.DataFrame, T: dict, tstar: dict, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    out.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.size": 9, "axes.titlesize": 10})

    # 1. signature plot per family: how much of the half-spread survives τ seconds later?
    s = T["signature"]
    order = [0, 1, 2, 5, 10, 30, 60, 120, 300, 900, 3600, -1]
    lab = ["h", "1", "2", "5", "10", "30", "60", "120", "300", "900", "3600", "res"]
    fams = [f for f in FAM_TITLE if s.filter(pl.col("family") == f).height]
    fig, axes = plt.subplots(1, len(fams), figsize=(5.0 * len(fams), 3.8), squeeze=False)
    for ax, fam in zip(axes[0], fams):
        x = s.filter(pl.col("family") == fam)
        for ref, col, name in (("fair", BLUE, "fair-value reference"), ("mid", ORANGE, "mid reference")):
            # Why drop thin points: a 15m fill only reaches 900 s if it traded before its window
            # opened; a handful of clusters gives an error bar that only squashes the axis.
            r = x.filter((pl.col("ref") == ref) & pl.col("tau").is_not_null() & (pl.col("n_clusters") >= 30)).sort("tau")
            pos = [order.index(t) for t in r["tau"].to_list() if t in order]
            r = r.filter(pl.col("tau").is_in([order[i] for i in pos]))
            ax.errorbar(pos, r["mean"], yerr=1.96 * r["se"].fill_null(0), marker="o", ms=4, lw=2, capsize=2, color=col, label=name)
        res = x.filter(pl.col("col") == "M_res")
        if res.height:
            ax.errorbar([order.index(-1)], res["mean"], yerr=1.96 * res["se"].fill_null(0), marker="s", ms=5, color=GREY, capsize=2, label="held to resolution")
        reb = x.filter(pl.col("col") == "rebate_c")
        if reb.height:
            ax.axhline(-reb["mean"][0], color=AQUA, lw=1, ls="--", label=f"−rebate ({reb['mean'][0]:.2f}¢)")
        ax.axvline(order.index(tstar.get(fam, 60)), color=GREY, lw=1, ls=":")
        ax.text(order.index(tstar.get(fam, 60)) + 0.1, ax.get_ylim()[1] * 0.92, f"τ* = {tstar.get(fam, 60)} s", color=INK, fontsize=8)
        ax.set_xticks(range(len(order)), lab)
        ax.set_xlabel("seconds after the fill")
        ax.set_ylabel("maker markout, ¢ per share")
        ax.set_title(FAM_TITLE[fam])
        _style(ax)
    axes[0][0].legend(frameon=False, fontsize=8)
    fig.tight_layout()
    fig.savefig(out / "signature.png", dpi=150)
    plt.close(fig)

    # 2. btc_15m by minute of the window: where in the 15 minutes is flow toxic?
    m = T.get("minute_curve", pl.DataFrame())
    if m.height:
        m = m.filter(pl.col("minute") >= 0)
        k = tstar.get("btc_15m", 1)
        fig, ax = plt.subplots(figsize=(7.5, 3.6))
        for c, col, name in (("h_fair", GREY, "h (captured at the fill)"), (f"M{k}_fair", BLUE, f"M({k} s)"), ("M60_fair", ORANGE, "M(60 s)")):
            r = m.filter(pl.col("col") == c).sort("minute")
            ax.errorbar(r["minute"], r["mean"], yerr=1.96 * r["se"].fill_null(0), marker="o", ms=4, lw=2, capsize=2, color=col, label=name)
        ax.set_xlabel("minute of the 15-minute window")
        ax.set_ylabel("¢ per share")
        ax.set_title("BTC 15-minute: spread captured vs kept, by minute")
        ax.set_xticks(range(15))
        _style(ax)
        ax.legend(frameon=False, fontsize=8)
        fig.tight_layout()
        fig.savefig(out / "minute_15m.png", dpi=150)
        plt.close(fig)

    # 3. btc_15m: realized 60 s markout against the fair-value gap at the fill
    d = F.filter((pl.col("family") == "btc_15m") & pl.col("h_fair").is_not_null() & pl.col("M60_fair").is_not_null())
    if d.height:
        edges = np.arange(-8, 8.5, 1.0)
        d = d.with_columns(pl.col("h_fair").clip(-7.99, 7.99).cut(list(edges), labels=[str(i) for i in range(len(edges) + 1)]).alias("b"))
        g = (d.group_by("b").agg(pl.col("h_fair").mean().alias("x"), pl.col("M60_fair").mean().alias("y"), pl.col("M1_fair").mean().alias("y1"),
                                 pl.len().alias("n")).filter(pl.col("n") >= 200).sort("x"))
        fee = d["fee_c"].mean()
        fig, ax = plt.subplots(figsize=(6.2, 4.0))
        lim = [-8, 8]
        ax.plot(lim, lim, color=GREY, lw=1, ls="--", label="fair value fully right (M = h)")
        ax.plot(g["x"], g["y1"], marker="o", ms=4, lw=2, color=BLUE, label="M(1 s)")
        ax.plot(g["x"], g["y"], marker="o", ms=4, lw=2, color=ORANGE, label="M(60 s)")
        ax.axvline(-fee, color=AQUA, lw=1, ls=":")
        ax.text(-fee + 0.15, 5.5, f"−taker fee ≈ {fee:.2f}¢:\nstale enough to snipe", fontsize=8, color=INK)
        ax.set_xlim(lim)
        ax.set_xlabel("fair-value gap at the fill, h_fair = s(m_fair − P), ¢")
        ax.set_ylabel("realized markout, ¢ per share")
        ax.set_title("BTC 15-minute: does the fair-value gap come true?")
        _style(ax)
        ax.legend(frameon=False, fontsize=8, loc="upper left")
        fig.tight_layout()
        fig.savefig(out / "gap_vs_markout_15m.png", dpi=150)
        plt.close(fig)

    # 4. decision curves: drop the fills a scorer likes least, what do the kept ones earn?
    panels = [(n, T[n]) for n in ("decision", "decision_60") if n in T and T[n].height]
    if panels:
        fig, axes = plt.subplots(1, len(panels), figsize=(5.2 * len(panels), 3.8), squeeze=False)
        for ax, (name, dc) in zip(axes[0], panels):
            dc = dc.filter(pl.col("family") == "btc_15m")
            for sc, col, lab_ in (("const", GREY, "no information (random drop)"), ("gap", ORANGE, "fair-value gap only"), ("ridge", BLUE, "all ex-ante features")):
                r = dc.filter(pl.col("scorer") == sc).sort("drop")
                ax.plot(r["drop"] * 100, r["edge_c"], marker="o", ms=3, lw=2, color=col, label=lab_)
            ax.set_xlabel("% of fills dropped (worst predicted first)")
            ax.set_ylabel("edge of kept fills incl. rebate, ¢/share")
            ax.set_title("BTC 15-minute, held out: " + ("τ*" if name == "decision" else "60 s"))
            _style(ax)
        axes[0][0].legend(frameon=False, fontsize=8)
        fig.tight_layout()
        fig.savefig(out / "decision_curve_15m.png", dpi=150)
        plt.close(fig)

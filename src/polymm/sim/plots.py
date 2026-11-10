"""Plots for research/simulator.md. Each answers one question. One colour per rung, fixed order
(never re-assigned when a rung is missing); grey band = pessimistic … optimistic cancel models."""
from __future__ import annotations

import json
from pathlib import Path

import polars as pl

BLUE, ORANGE, AQUA, PURPLE, INK, GREY = "#2a78d6", "#eb6834", "#1baf7a", "#8e5bd0", "#3a3a37", "#9a9a94"
RUNG_COL = {0: GREY, 1: BLUE, 2: AQUA, 3: ORANGE, 4: PURPLE}
RUNG_LAB = {0: "0 symmetric, 2 s timer", 1: "1 event-driven (stale cancel)", 2: "2 + inventory skew", 3: "3 + jump guard", 4: "4 + selective (V̂ table)"}
TERMS = (("c1_spread", "① spread"), ("c2_adverse", "② adverse selection (60 s)"), ("c3_inventory", "③ inventory"), ("c4_rebate", "④ rebate"))
TERM_COL = {"c1_spread": BLUE, "c2_adverse": ORANGE, "c3_inventory": GREY, "c4_rebate": AQUA}


def _style(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#e6e5df", lw=0.8)
    ax.set_axisbelow(True)


def make_plots(T: dict, res: dict, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    out.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.size": 9, "axes.titlesize": 10})

    # 1. where the money goes, per rung: the four terms per filled share, and P&L per window
    r = T["rungs"].filter(pl.col("fold") == "test").sort("rung")
    b = T["band"].filter(pl.col("fold") == "test")
    if r.height:
        fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 3.8))
        rungs = r["rung"].to_list()
        wdt = 0.2
        for j, (c, lab) in enumerate(TERMS):
            vals = [100 * x / s if s else 0 for x, s in zip(r[c].to_list(), r["shares"].to_list())]
            a1.bar([k + (j - 1.5) * wdt for k in range(len(rungs))], vals, width=wdt - 0.02, color=TERM_COL[c], label=lab)
        a1.axhline(0, color=INK, lw=0.8)
        a1.set_xticks(range(len(rungs)), [str(x) for x in rungs])
        a1.set_xlabel("rung")
        a1.set_ylabel("¢ per filled share")
        a1.set_title("Where each filled share's P&L comes from (test days, matched, 75 ms)")
        a1.legend(frameon=False, fontsize=8, ncol=2)
        _style(a1)
        for k, rg in enumerate(rungs):
            x = b.filter(pl.col("rung") == rg)
            lo, hi = x.filter(pl.col("model") == "pess")["mean"], x.filter(pl.col("model") == "opt")["mean"]
            if lo.len() and hi.len():
                a2.plot([k, k], [lo[0], hi[0]], color=GREY, lw=8, alpha=0.4, solid_capstyle="butt",
                        label="pessimistic – optimistic" if k == 0 else None)
            m = r.filter(pl.col("rung") == rg)
            a2.errorbar([k], m["mean"], yerr=[1.96 * (m["se"][0] or 0)], color=RUNG_COL.get(rg, INK), marker="o", ms=6, capsize=3, lw=2)
        a2.axhline(0, color=INK, lw=0.8)
        a2.set_xticks(range(len(rungs)), [str(x) for x in rungs])
        a2.set_xlabel("rung")
        a2.set_ylabel("USD per 15-minute window")
        a2.set_title("P&L per window, walk-forward test days (95 % CI, matched)")
        a2.legend(frameon=False, fontsize=8)
        _style(a2)
        fig.tight_layout()
        fig.savefig(out / "rungs.png", dpi=150)
        plt.close(fig)

    # 2. what a millisecond is worth
    lt = T["latency"].filter(pl.col("fold") == "test")
    if lt.height:
        fig, ax = plt.subplots(figsize=(7, 3.6))
        for rg in sorted(lt["rung"].unique().to_list()):
            x = lt.filter(pl.col("rung") == rg).sort("lat_ms")
            xs = [max(v, 5) for v in x["lat_ms"].to_list()]
            ax.errorbar(xs, x["mean"], yerr=[1.96 * (s or 0) for s in x["se"].to_list()], color=RUNG_COL.get(rg, INK), marker="o", ms=4,
                        lw=2, capsize=2, label=RUNG_LAB.get(rg, str(rg)))
        ax.set_xscale("log")
        ax.set_xticks([5, 30, 75, 128, 300, 1000], ["0", "30", "75", "128", "300", "1000"])
        ax.axhline(0, color=INK, lw=0.8)
        ax.set_xlabel("order-path latency, ms (0 plotted at 5)")
        ax.set_ylabel("USD per window")
        ax.set_title("P&L per window against our latency (test days, matched)")
        ax.legend(frameon=False, fontsize=7)
        _style(ax)
        fig.tight_layout()
        fig.savefig(out / "latency.png", dpi=150)
        plt.close(fig)

    # 3. stability: is the best setting on a plateau? rung 1 (h × hysteresis) and rung 3 (h × jump threshold)
    g = T["grid_all"].filter(pl.col("date").is_in(res["dates"][:-1]))
    cf = T["configs"]
    if g.height:
        agg = (g.group_by("rung", "cfg").agg((pl.col("pnl").sum() / pl.col("windows").sum()).alias("mean"))
               .join(cf.drop("rung"), on="cfg", how="left"))
        fig, axes = plt.subplots(1, 2, figsize=(12, 3.6))
        for ax, (rg, line, xs) in zip(axes, ((1, "hyst", "h"), (3, "jump_c", "h"))):
            x = agg.filter(pl.col("rung") == rg)
            if rg == 3 and x.height:
                # hold the other axes at the best setting's values
                bst = x.sort("mean", descending=True).row(0, named=True)
                x = x.filter((pl.col("gamma") == bst["gamma"]) & (pl.col("hold_ms") == bst["hold_ms"]))
            for k, lv in enumerate(sorted(x[line].unique().to_list())):
                y = x.filter(pl.col(line) == lv).sort(xs)
                ax.plot(y[xs], y["mean"], marker="o", ms=4, lw=2, color=(BLUE, ORANGE, AQUA, PURPLE)[k % 4], label=f"{line} = {lv:g}")
            ax.axhline(0, color=INK, lw=0.8)
            ax.set_xlabel("half-spread h, ¢")
            ax.set_ylabel("USD per window (training days)")
            ax.set_title(f"Rung {rg}: P&L surface over the grid")
            ax.legend(frameon=False, fontsize=8)
            _style(ax)
        fig.tight_layout()
        fig.savefig(out / "stability.png", dpi=150)
        plt.close(fig)

    # 4. Fed rewards rung: reward against fill P&L
    if "fed" in T and T["fed"].height:
        f = T["fed"].sort("date", "h")
        fig, ax = plt.subplots(figsize=(7, 3.4))
        lab = [f"{d[5:]} h={h:g}" for d, h in zip(f["date"].to_list(), f["h"].to_list())]
        k = range(f.height)
        ax.bar([i - 0.2 for i in k], f["c4_rewards"], width=0.38, color=AQUA, label="④ liquidity rewards")
        ax.bar([i + 0.2 for i in k], f["pnl_hold"] + f["c4_rebate"], width=0.38, color=ORANGE, label="fills: ①+②+③ + rebate (marked to market)")
        ax.axhline(0, color=INK, lw=0.8)
        ax.set_xticks(list(k), lab, fontsize=7)
        ax.set_ylabel("USD per day")
        ax.set_title("Fed December, rung 5: rewards vs fill P&L (200 shares a side)")
        ax.legend(frameon=False, fontsize=8)
        _style(ax)
        fig.tight_layout()
        fig.savefig(out / "fed_rewards.png", dpi=150)
        plt.close(fig)
    (out / "plots.json").write_text(json.dumps({"made": sorted(p.name for p in out.glob("*.png"))}))

"""Plots for research/rewards.md. Palette as stages 4–5 (blue = central reading, grey band =
the competition bracket)."""
from __future__ import annotations

from pathlib import Path

import polars as pl

BLUE, ORANGE, AQUA, GREY, INK = "#2a78d6", "#eb6834", "#1baf7a", "#9a9a94", "#3a3a37"


def _style(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="x", color="#e6e5df", lw=0.8)
    ax.set_axisbelow(True)


def make_plots(T: dict, R: dict, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    out.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.size": 9, "axes.titlesize": 10})
    # 1. sector grid: median and 75th-percentile reward yield at 1k USD, and the pool
    s = T.get("screen_sector")
    if s is not None and s.height:
        s = s.sort("yield1k_med")
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharey=True, gridspec_kw={"width_ratios": [1.4, 1]})
        y = range(s.height)
        axes[0].barh(y, 100 * s["yield1k_med"], color=BLUE, label="median")
        axes[0].scatter(100 * s["yield1k_p75"], y, color=INK, s=14, zorder=3, label="75th percentile")
        axes[0].scatter(100 * s["yield1k_med_cons"], y, color=ORANGE, s=14, marker="|", zorder=3, label="median, high-competition bound")
        axes[0].set_yticks(list(y), s["sector"].to_list())
        axes[0].set_xlabel("gross reward yield at 1,000 USD, % of capital per day")
        axes[0].legend(frameon=False, fontsize=8, loc="lower right")
        axes[1].barh(y, s["pool_usd_day"] / 1000, color=GREY)
        axes[1].set_xlabel("live pools, thousand USD per day")
        for ax in axes:
            _style(ax)
        fig.suptitle("Liquidity rewards by sector (markets ≥ 1 day from resolution; before adverse selection)", fontsize=10)
        fig.tight_layout()
        fig.savefig(out / "sector_grid.png", dpi=150)
        plt.close(fig)
    # 2. our recorded pool markets: net USD/day vs capital for the two-sided best-price quote
    g = T.get("pool_grid")
    if g is not None and g.height:
        hd = R.get("headline_day")
        d = g.filter((pl.col("policy") == "passive") & (pl.col("model") == "matched") & (pl.col("sides") == 2) & pl.col("quote").str.contains("best")
                     & (pl.col("date") == hd))
        qs = d["question"].unique().to_list()
        fig, axes = plt.subplots(1, max(1, len(qs)), figsize=(5.5 * max(1, len(qs)), 3.8), squeeze=False)
        for ax, q in zip(axes[0], qs):
            x = d.filter(pl.col("question") == q).sort("capital")
            ax.fill_between(x["capital"], x["reward_highcomp"], x["reward_lowcomp"], color=GREY, alpha=0.35, lw=0, label="rewards, competition bracket")
            ax.plot(x["capital"], x["reward_central"], color=BLUE, marker="o", lw=2, label="rewards, central")
            ax.plot(x["capital"], x["net_central"], color=ORANGE, marker="o", lw=2, label="net of fills (matched, passive)")
            ax.fill_between(x["capital"], x["net_central"] - 1.96 * x["fill_pnl_day_se"], x["net_central"] + 1.96 * x["fill_pnl_day_se"],
                            color=ORANGE, alpha=0.12, lw=0, label="net, 95 % band of the fill P&L")
            ax.set_xscale("log")
            ax.set_xlabel("capital locked, USD")
            ax.set_ylabel("USD per day")
            ax.set_title(q.replace("Will the Fed increase interest rates by 25 bps", "Fed +25 bp").replace("Will there be no change in Fed interest rates", "Fed no change")[:60] + f" · {hd}")
            ax.axhline(0, color=INK, lw=0.8)
            _style(ax)
        axes[0][0].legend(frameon=False, fontsize=8)
        fig.tight_layout()
        fig.savefig(out / "pool_markets.png", dpi=150)
        plt.close(fig)
    # 3. screen: yield vs capacity, one dot per market, by sector
    p = T.get("screen_markets")
    if p is not None and p.height:
        p = p.filter(pl.col("rankable") & (pl.col("yield1k_central") > 0) & pl.col("capacity_usd").is_not_null() & (pl.col("capacity_usd") > 0))
        fig, ax = plt.subplots(figsize=(8, 5))
        secs = p.group_by("sector").agg(pl.col("rate_per_day").sum()).sort("rate_per_day", descending=True)["sector"].to_list()
        cmap = plt.get_cmap("tab10")
        for i, sec in enumerate(secs):
            x = p.filter(pl.col("sector") == sec)
            ax.scatter(x["capacity_usd"], 100 * x["yield1k_central"], s=6, alpha=0.5, color=cmap(i % 10), label=sec)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("capacity: capital at which we would hold 25 % of the pool, USD")
        ax.set_ylabel("gross yield at 1,000 USD, % per day")
        ax.set_title("Every paying market: reward yield vs how much capital it can take")
        ax.legend(frameon=False, fontsize=7, markerscale=2, ncols=2)
        ax.spines[["top", "right"]].set_visible(False)
        fig.tight_layout()
        fig.savefig(out / "screen_scatter.png", dpi=150)
        plt.close(fig)

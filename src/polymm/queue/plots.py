"""Plots for research/queue_fill.md. Each answers one question. Palette as stage 4: blue = the
stale-cancel policy (what a real quoter does), orange = the passive order (no cancel rule),
grey band = the bracket between the pessimistic and optimistic cancel models."""
from __future__ import annotations

from pathlib import Path

import polars as pl

from .analysis import X_LABELS

BLUE, ORANGE, AQUA, GREY, INK = "#2a78d6", "#eb6834", "#1baf7a", "#9a9a94", "#3a3a37"
TYPES = (("touch", "back of the touch"), ("behind", "one tick behind"), ("improve", "price-improving"))
POL = (("stale", BLUE, "cancel when stale (128 ms)"), ("passive", ORANGE, "passive, no cancel rule"))
XO = {b: i for i, b in enumerate(X_LABELS)}


def _style(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#e6e5df", lw=0.8)
    ax.set_axisbelow(True)


def _xo(t: pl.DataFrame, col: str) -> pl.DataFrame:
    return t.with_columns(pl.col(col).replace_strict(XO, default=99).alias("o")).sort("o")


def make_plots(T: dict, R: dict, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    out.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update({"font.size": 9, "axes.titlesize": 10})
    btc = pl.col("family") == "btc_15m"

    # 1. how likely is a fill, and how fast?
    f = T["fill_type"].filter(btc)
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.6), sharey=True)
    for ax, (typ, lab) in zip(axes, TYPES):
        for pol, col, name in POL:
            x = f.filter((pl.col("type") == typ) & (pl.col("policy") == pol))
            lo, hi, mid = (x.filter(pl.col("model") == m).sort("T") for m in ("pess", "opt", "matched"))
            if not mid.height:
                continue
            if pol == "stale":
                ax.fill_between(lo["T"], 100 * lo["mean"], 100 * hi["mean"], color=GREY, alpha=0.35, lw=0, label="pessimistic – optimistic")
            ax.errorbar(mid["T"], 100 * mid["mean"], yerr=196 * mid["se"], color=col, marker="o", ms=4, lw=2, capsize=2, label=name)
        ax.set_xscale("log")
        ax.set_xticks([1, 5, 10, 30, 60], ["1", "5", "10", "30", "60"])
        ax.set_title(lab)
        ax.set_xlabel("seconds after the order arrives")
        _style(ax)
    axes[0].set_ylabel("% of placements filled")
    axes[0].legend(frameon=False, fontsize=8)
    fig.suptitle("BTC 15-minute: probability that a 5-share order is filled (matched cancel model)", fontsize=10)
    fig.tight_layout()
    fig.savefig(out / "fill_curves.png", dpi=150)
    plt.close(fig)

    # 2. are fills worse at the back of the queue?
    t = T["tox_x"].filter((pl.col("model") == "matched") & (pl.col("qpos") != ">5k"))
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8), sharey=True)
    for ax, k in zip(axes, (1, 60)):
        for pol, col, name in POL:
            x = _xo(t.filter(pl.col("policy") == pol), "qpos")
            ax.errorbar(x["o"], x[f"a{k}"], yerr=1.96 * x[f"a{k}_se"], color=col, marker="o", ms=4, lw=2, capsize=2, label=name)
        ax.set_xticks(range(len(X_LABELS) - 1), X_LABELS[:-1])
        ax.set_xlabel("shares ahead on arrival (0 = price-improving, alone at a new best)")
        ax.axhline(0, color=INK, lw=0.8)
        ax.set_title(f"adverse selection a({k} s) of our fills")
        _style(ax)
    axes[0].set_ylabel("¢ per share (h − M)")
    axes[0].legend(frameon=False, fontsize=8)
    fig.suptitle("BTC 15-minute: toxicity of our fills by queue position (matched cancel model)", fontsize=10)
    fig.tight_layout()
    fig.savefig(out / "toxicity_by_queue.png", dpi=150)
    plt.close(fig)

    # 3. is a place in line worth having?
    v = T["value_x"].filter((pl.col("model") == "matched") & (pl.col("qtb") != ">5k") & (pl.col("qtb") != "0"))
    vt = T["value_type"].filter(btc & (pl.col("model") == "matched") & (pl.col("type") == "improve"))
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8), sharey=True)
    for ax, k in zip(axes, (1, 60)):
        for pol, col, name in POL:
            x = _xo(v.filter((pl.col("tau") == k) & (pl.col("policy") == pol)), "qtb")
            ax.errorbar(x["o"], x["mean"], yerr=1.96 * x["se"], color=col, marker="o", ms=4, lw=2, capsize=2, label=f"back of the touch, {name}")
            imp = vt.filter((pl.col("tau") == k) & (pl.col("policy") == pol))
            if imp.height:
                ax.axhline(imp["mean"][0], color=col, lw=1, ls=":", label=f"price-improving, {name}")
        ax.set_xticks(range(len(X_LABELS) - 1), X_LABELS[:-1])
        ax.axhline(0, color=INK, lw=0.8)
        ax.set_xlabel("touch size when we decide to join, shares")
        ax.set_title(f"markout horizon {k} s")
        _style(ax)
    axes[0].set_ylabel("value per placement incl. rebate, ¢/share")
    axes[0].legend(frameon=False, fontsize=7)
    fig.suptitle("BTC 15-minute: what joining the line is worth (matched cancel model)", fontsize=10)
    fig.tight_layout()
    fig.savefig(out / "value_by_queue.png", dpi=150)
    plt.close(fig)

    # 4. latency: cancel delay (left) and racing for a spread opening (right)
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
    c = T["cancel_latency"].filter(pl.col("model") == "matched")
    for (typ, lab), col in zip(TYPES, (BLUE, GREY, AQUA)):
        x = c.filter((pl.col("type") == typ) & (pl.col("tau") == 60)).sort("lc_ms")
        axes[0].errorbar(x["lc_ms"], x["mean"], yerr=1.96 * x["se"], marker="o", color=col, lw=2, capsize=2, label=lab)
    axes[0].set_title("Cancel delay (stale policy), 60 s markout")
    axes[0].set_ylabel("value per placement incl. rebate, ¢/share")
    axes[0].set_xlabel("cancel latency, ms")
    L = T["latency"].filter(pl.col("policy") == "stale")
    b = L.filter(pl.col("col") == "beaten").sort("lat_ms")
    r = L.filter(pl.col("col") == "rejected").sort("lat_ms")
    axes[1].plot(b["lat_ms"], 100 * b["mean"], marker="o", color=BLUE, lw=2, label="someone else got there first")
    axes[1].plot(r["lat_ms"], 100 * r["mean"], marker="o", color=ORANGE, lw=2, label="gap closed before we arrived")
    axes[1].set_ylabel("% of spread openings")
    axes[1].set_xlabel("order latency, ms")
    axes[1].set_title("Racing for a spread opening (≥ 2¢)")
    for ax in axes:
        ax.set_xscale("symlog", linthresh=50)
        ax.set_xticks([0, 50, 128, 300, 1000], ["0", "50", "128", "300", "1000"])
        ax.axhline(0, color=INK, lw=0.8) if ax is axes[0] else None
        _style(ax)
        ax.legend(frameon=False, fontsize=8)
    fig.suptitle("BTC 15-minute: what latency costs", fontsize=10)
    fig.tight_layout()
    fig.savefig(out / "latency.png", dpi=150)
    plt.close(fig)

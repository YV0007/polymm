"""`polymm rewards`: R0–R5 end to end.

    R0  today's reward configuration (every market with a live pool), sectors and fee categories
    R1–R2  on our recorded families: the book's reward score every minute and our share for a grid
        of candidate quotes (only markets with a live pool: today the two Fed December markets;
        BTC 15m and the daily ladder have none)
    R3–R4  the cost of resting there (stage-5 probes on the same markets) and the net value grid
    R5  the screen over every paying market from the REST book samples, and the sector grid

Outputs (reports/rewards/, never in git): table_*.parquet, rewards_params.json, summary.md.
Assumption, stated in the report: today's pools applied to the recorded days (only the current
configuration is published; daily snapshots start 2026-11-10).
"""
from __future__ import annotations

import gc
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from ..markouts.fills import strict_windows
from . import screen as SC
from .config import snapshot
from .score import Quote, minute_books, score_minutes
from .value import net_value, placement_cost

log = logging.getLogger(__name__)

POSITIONS = (0, 1, 2, 3)          # 0 = join the best price; else ¢ from the adjusted mid
SIZES = (None, 1_000, 5_000, 20_000)   # None = the market's minimum qualifying size


@dataclass
class RewardsConfig:
    dates: list[str]
    fair_root: Path = Path("reports/fair")
    queue_root: Path = Path("reports/queue")
    out: Path = Path("reports/rewards")
    data_root: Path = Path("data")
    plot_dir: Path = Path("research/rewards")
    plots: bool = True
    sample: int = 0
    every_s: int = 600


def candidates(min_size: float) -> list[Quote]:
    out = []
    for pos in POSITIONS:
        for size in SIZES:
            sz = float(size or min_size)
            out += [Quote(pos, pos, sz), Quote(pos, None, sz), Quote(None, pos, sz)]
    return out


def run_rewards(cfg: RewardsConfig) -> dict:
    t0 = time.perf_counter()
    cfg.out.mkdir(parents=True, exist_ok=True)
    R: dict = {"dates": cfg.dates}
    # ---- R0 ----------------------------------------------------------------------------------------
    if cfg.sample > 0:
        from .sample import run_sampler
        run_sampler(cfg.data_root, cfg.every_s, cfg.sample)  # writes today's config snapshot too
    days = sorted(p.name[3:] for p in (cfg.data_root / "reference" / "rewards").glob("dt=*"))
    if not days:
        snapshot(cfg.data_root)
        days = sorted(p.name[3:] for p in (cfg.data_root / "reference" / "rewards").glob("dt=*"))
    M = pl.read_parquet(cfg.data_root / "reference" / "rewards" / f"dt={days[-1]}" / "markets.parquet")
    R["config_day"] = days[-1]
    R["live"] = {"markets": M.height, "pool_usd_day": round(float(M["rate_per_day"].sum()), 1)}
    T: dict[str, pl.DataFrame] = {}
    T["config_sector"] = (M.group_by("sector").agg(pl.len().alias("markets"), pl.col("rate_per_day").sum().alias("pool_usd_day"),
                                                   pl.col("rate_per_day").median().alias("pool_med"), pl.col("rate_per_day").max().alias("pool_max"),
                                                   pl.col("fee_rate").first(), pl.col("rebate_rate").first())
                          .sort("pool_usd_day", descending=True))
    # our recorded markets: which have a pool today
    ref = pl.read_parquet(cfg.fair_root / "reference" / "markets_ref.parquet")
    ours = ref.select("family", "question", "condition_id", pl.col("yes_asset").alias("yes")).join(
        M.select("condition_id", "rate_per_day", "max_spread_c", "min_size", "tick"), on="condition_id", how="left")
    R["recorded_with_pool"] = (ours.group_by("family").agg(pl.len().alias("markets"), (pl.col("rate_per_day").fill_null(0) > 0).sum().alias("with_pool"),
                                                           pl.col("rate_per_day").fill_null(0).sum().alias("pool_usd_day")).to_dicts())
    paid = ours.filter(pl.col("rate_per_day").fill_null(0) > 0)
    # ---- R1–R4 on recorded books -------------------------------------------------------------------
    if paid.height:
        params = {r["yes"]: (r["max_spread_c"], r["min_size"]) for r in paid.iter_rows(named=True)}
        tick = {r["yes"]: r["tick"] for r in paid.iter_rows(named=True)}
        pools = {r["yes"]: r["rate_per_day"] for r in paid.iter_rows(named=True)}
        yes = list(params)
        shares, probes = [], []
        for d in cfg.dates:
            dd = cfg.fair_root / d
            B = minute_books(dd, yes, strict_windows(dd))
            if not B.height:
                continue
            for y in yes:
                Sy = score_minutes(B.filter(pl.col("yes") == y), {y: params[y]}, candidates(params[y][1]), tick)
                if Sy.height:
                    tm = B.filter(pl.col("yes") == y)["tob_match_share"]
                    shares.append(Sy.with_columns(pl.lit(d).alias("date"), pl.lit(float(tm[0]) if tm.len() else None).alias("tob_match_share")))
            pp = cfg.queue_root / d / "probes.parquet"
            if pp.exists():
                probes.append(pl.read_parquet(pp).filter(pl.col("yes").is_in(yes)).with_columns(pl.col(pl.Float32).cast(pl.Float64)))
            del B
            gc.collect()
        if shares:
            S = pl.concat(shares)
            T["pool_minutes"] = (S.filter(pl.col("cand") == 0).group_by("yes", "date")
                                 .agg(pl.len().alias("minutes"), pl.col("tob_match_share").first(), pl.col("mid_adj").mean(), pl.col("q_one").mean(), pl.col("q_two").mean(),
                                      pl.col("comp_low").mean(), pl.col("comp_central").mean(), pl.col("comp_high").mean()).sort("yes", "date"))
            P = pl.concat(probes, how="diagonal_relaxed").with_columns(pl.col("stop").cast(pl.String)) if probes else pl.DataFrame()
            C = placement_cost(P) if P.height else pl.DataFrame()
            T["pool_cost"] = C
            if C.height:
                names = {r["yes"]: r["question"] for r in paid.iter_rows(named=True)}
                Vd = net_value(S, C, pools, by=["date"])
                Va = net_value(S, C, pools).with_columns(pl.lit("all").alias("date"))
                V = pl.concat([Vd, Va], how="diagonal_relaxed").with_columns(pl.col("yes").replace_strict(names, default="").alias("question"))
                # Why rank by yield and show the fill P&L beside it: the Fed fill P&L is 0 ± ~1 USD/day
                # per 200 shares (≈190 fills in 3 days); ranking on reward + its point estimate picks the
                # luckiest noise and scales it to 20k shares (audit finding 3). Conservative = high-
                # competition rewards + the worst cancel model's fill P&L − 1.96 SE.
                V = V.with_columns((pl.col("reward_highcomp") + pl.col("fill_pnl_day") - 1.96 * pl.col("fill_pnl_day_se")).alias("net_lower95"),
                                   (pl.col("reward_central") / pl.col("size") / pl.col("sides") * 100).alias("reward_c_per_share_side"),
                                   (pl.col("fill_pnl_day") / pl.col("size") / pl.col("sides") * 100).alias("fill_c_per_share_side"),
                                   (pl.col("fill_pnl_day_se") / pl.col("size") / pl.col("sides") * 100).alias("fill_c_per_share_side_se"),
                                   pl.concat_str([pl.col("s_bid_c").round(1).cast(pl.String).fill_null("–"), pl.lit("¢ / "),
                                                  pl.col("s_ask_c").round(1).cast(pl.String).fill_null("–"), pl.lit("¢")]).alias("dist_from_mid"))
                V = V.with_columns((pl.col("net_lower95") / pl.col("capital")).alias("yield_lower95"))
                T["pool_grid"] = V
                last = cfg.dates[-1]
                R["headline_day"] = last
                base = V.filter((pl.col("policy") == "passive") & (pl.col("model") == "matched") & (pl.col("date") == last))
                T["pool_best"] = base.sort("gross_yield_central", "size", descending=[True, False]).group_by("yes", maintain_order=True).head(6)
                cons = (V.filter((pl.col("policy") == "passive") & (pl.col("date") == last)).sort("net_lower95")
                        .group_by("yes", "cand", maintain_order=True).first())
                T["pool_best_cons"] = cons.sort("yield_lower95", descending=True).group_by("yes", maintain_order=True).head(4)
                T["pool_per_share"] = (V.filter(pl.col("quote").str.contains("best") & (pl.col("sides") == 2) & (pl.col("size") == 1000))
                                       .select("question", "date", "policy", "model", "minutes", "comp_central", "reward_c_per_share_side",
                                               "fill_c_per_share_side", "fill_c_per_share_side_se")
                                       .sort("question", "date", "policy", "model"))
            del S
    # ---- R5 screen -------------------------------------------------------------------------------
    per, sector, info = SC.screen(cfg.data_root)
    R["screen"] = info
    if per.height:
        T["screen_markets"] = per
        T["screen_sector"] = sector
        T["screen_top"] = (per.filter(pl.col("rankable")).sort("yield1k_high", descending=True).head(50)
                           .select("sector", "question", "condition_id", "rate_per_day", "mid", "spread_c", "comp_central", "yield1k_central",
                                   "yield1k_high", "yield10k_central", "capacity_usd", "jump_c_10min", "days_to_end", "two_sided_only", "snaps"))
    for name, t in T.items():
        if t is not None and t.height:
            t.write_parquet(cfg.out / f"table_{name}.parquet")
    R["seconds"] = round(time.perf_counter() - t0, 1)
    (cfg.out / "rewards_params.json").write_text(json.dumps(params_out(T, R), indent=1, default=str))
    (cfg.out / "rewards.json").write_text(json.dumps(R, indent=1, default=str))
    if cfg.plots:
        from .plots import make_plots
        try:
            make_plots(T, R, cfg.plot_dir)
        except Exception:
            log.exception("plots failed")
    write_summary(T, R, cfg)
    return R


def params_out(T: dict, R: dict) -> dict:
    rows = lambda n, k=50: T[n].head(k).to_dicts() if n in T and T[n].height else []
    return {"config_day": R.get("config_day"), "live": R.get("live"), "recorded_with_pool": R.get("recorded_with_pool"),
            "pool_best": rows("pool_best"), "pool_best_cons": rows("pool_best_cons"), "screen": R.get("screen"),
            "screen_sector": rows("screen_sector"), "screen_top": rows("screen_top", 50)}


def _fmt(t: pl.DataFrame | None, cols: list[str], nd: int = 3) -> str:
    if t is None or not t.height:
        return "_(empty)_"
    cols = [c for c in cols if c in t.columns]
    def f(v):
        if v is None:
            return "–"
        if isinstance(v, float):
            return f"{v:,.{nd}f}"
        if isinstance(v, int):
            return f"{v:,}"
        s = str(v)
        return s[:70]
    L = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    return "\n".join(L + ["| " + " | ".join(f(r[c]) for c in cols) + " |" for r in t.select(cols).iter_rows(named=True)])


def write_summary(T: dict, R: dict, cfg: RewardsConfig) -> None:
    g = lambda n: T.get(n)
    S = [f"# Rewards and fees v1 (stage 6) — recorded days {', '.join(cfg.dates)}, configuration {R.get('config_day')}", "",
         f"Live pools: `{R.get('live')}` · our recorded families: `{R.get('recorded_with_pool')}` · screen: `{R.get('screen')}` · run {R.get('seconds')} s",
         "", "Units: USD per day; yields are per USD of capital per day (0.001 = 0.1 % a day).", "",
         "## R0 pools by sector (today)", "", _fmt(g("config_sector"), ["sector", "markets", "pool_usd_day", "pool_med", "pool_max", "fee_rate", "rebate_rate"], 2),
         "", "## R1 competition in our recorded pool markets (score-shares, minute means)", "",
         _fmt(g("pool_minutes"), ["yes", "date", "minutes", "tob_match_share", "mid_adj", "q_one", "q_two", "comp_low", "comp_central", "comp_high"], 1),
         "", "## R3 fill P&L of resting (USD/day per 200 shares, incl. rebate; market × hour clusters)", "",
         _fmt(g("pool_cost"), ["yes", "type", "policy", "model", "usd_day_per200", "usd_day_per200_se", "shares_filled_day_per200", "placements", "clusters"], 4),
         "", f"## R4 quotes ranked by gross reward yield ({R.get('headline_day')}; passive, matched cancel model; central competition)", "",
         _fmt(g("pool_best"), ["question", "quote", "dist_from_mid", "capital", "reward_central", "gross_yield_central", "reward_highcomp", "fill_pnl_day",
                               "fill_pnl_day_se", "net_central", "net_lower95"], 4),
         "", "### Per share and side at the best price (1,000 shares, two-sided): reward vs fill P&L, ¢ per day, per day and pooled", "",
         _fmt(g("pool_per_share"), ["question", "date", "policy", "model", "minutes", "comp_central", "reward_c_per_share_side", "fill_c_per_share_side",
                                    "fill_c_per_share_side_se"], 4),
         "", "### Conservative (high competition, worst cancel model, fill P&L at its 95 % lower bound)", "",
         _fmt(g("pool_best_cons"), ["question", "quote", "dist_from_mid", "model", "capital", "reward_highcomp", "fill_pnl_day", "fill_pnl_day_se", "net_lower95",
                                    "yield_lower95"], 4),
         "", "## R5 screen: sector grid (markets ≥ 1 day from resolution; standard quote; gross of adverse selection)", "",
         _fmt(g("screen_sector"), ["sector", "markets", "pool_usd_day", "yield1k_med", "yield1k_p75", "yield1k_med_cons", "yield10k_med",
                                   "markets_cons_over_0_2pct", "capacity_med_usd", "capacity_sum_usd", "jump_med_c", "days_to_end_med", "two_sided_only_share"], 4),
         "", "### Top 50 markets by conservative 1k yield", "",
         _fmt(g("screen_top"), ["sector", "question", "rate_per_day", "mid", "spread_c", "comp_central", "yield1k_central", "yield1k_high", "yield10k_central",
                                "capacity_usd", "jump_c_10min", "days_to_end", "snaps"], 4)]
    (cfg.out / "summary.md").write_text("\n".join(S) + "\n")

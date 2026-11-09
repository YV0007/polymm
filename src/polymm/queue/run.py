"""`polymm queue`: Q0–Q4 end to end. Per day: level streams + checks (Q0), probes (Q1), markouts of
probe fills; then pooled tables (Q2–Q4), `queue_params.json` for the simulator, `summary.md`.

Outputs (reports/queue/, never in git):
    <date>/probes.parquet    one row per probe, lean columns, all four cancel models
    <date>/checks.json       Q0 checks
    table_*.parquet          every table in the summary
    queue_params.json        what the simulator (stage 7) reads
    summary.md               machine-written tables; the write-up is research/queue_fill.md
"""
from __future__ import annotations

import gc
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from ..markouts.fills import data_root, fee_table, market_index
from . import analysis as A
from .engine import MODELS, make_probes, simulate
from .events import load_day

log = logging.getLogger(__name__)

LEAN_MODEL = ["t_first", "t_pre", "through", "filled5", "filled50", "filled200"]
LEAN_MK = ["h_fair", "M1_fair", "M5_fair", "M10_fair", "M30_fair", "M60_fair", "M_res"]


@dataclass
class QueueConfig:
    dates: list[str]
    fair_root: Path = Path("reports/fair")
    markouts_root: Path = Path("reports/markouts")
    out: Path = Path("reports/queue")
    data_dirs: list[Path] = field(default_factory=lambda: [Path("data_vps"), Path("data")])
    plot_dir: Path = Path("research/queue_fill")
    plots: bool = True
    reuse: bool = False


def build_day(date: str, cfg: QueueConfig, idx: pl.DataFrame, fees: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    t0 = time.perf_counter()
    dd = cfg.fair_root / date
    root = data_root(dd, cfg.data_dirs, date)
    D = load_day(date, dd, root, idx)
    P = make_probes(D["tob"], D["windows"], idx)
    fp = cfg.markouts_root / date / "fair_oos.parquet"
    fair = pl.read_parquet(fp) if fp.exists() else None
    if fair is None:
        log.warning("%s: no walk-forward fair series at %s (run polymm markouts first)", date, fp)
    R = simulate(P, D["levels"], D["trades"], D["tob"], fair)
    del P
    chk = D["checks"] | {"data_root": str(root), "probes": R.height}
    R = A.add_features(R.with_columns(pl.lit(date).alias("date")), fair)
    R = A.probe_markouts(R, D["tob"], fair, fees)
    del D, fair
    gc.collect()
    keep = ["pid", "date", "family", "condition_id", "yes", "t_start_s", "t_end_s", "design", "type", "s", "pk", "P", "lat_ms",
            "t_dec", "t_arr", "t_hi", "t_stop", "stop", "t_alone", "t_stale", "pos_arr", "x0", "spread_c", "imb_adv", "q_touch", "h_dec",
            "x0b", "qtb", "qpos", "spread_b", "imb_b", "minute",
            "rebate_c", *[f"{c}_{m}" for m in MODELS for c in LEAN_MODEL + LEAN_MK]]
    R = R.select([c for c in keep if c in R.columns]).with_columns(pl.col(pl.Float64).cast(pl.Float32), pl.col("stop").cast(pl.Categorical))
    # Why keep the timestamps Int64: Float32 would round nanoseconds to ~minutes
    chk["seconds"] = round(time.perf_counter() - t0, 1)
    return R, chk


def _inputs_sha1(date: str, cfg: QueueConfig) -> str:
    """Fingerprint of a day's inputs. Why content, not mtime: the morning refresh rewrites every
    day's fair_oos.parquet even when nothing changed, and a day first run on partial data is redone
    in full later; --reuse must rebuild exactly the days whose inputs differ."""
    import hashlib
    h = hashlib.sha1()
    for f in (cfg.fair_root / date / "clean_windows_strict.parquet", cfg.fair_root / date / "quality.md",
              cfg.markouts_root / date / "fair_oos.parquet"):
        h.update(f.read_bytes() if f.exists() else b"missing")
    return h.hexdigest()


def run_queue(cfg: QueueConfig) -> dict:
    t0 = time.perf_counter()
    cfg.out.mkdir(parents=True, exist_ok=True)
    idx = market_index(pl.read_parquet(cfg.fair_root / "reference" / "markets_ref.parquet"))
    fees = fee_table(cfg.data_dirs)
    R: dict = {"dates": cfg.dates, "checks": {}}
    days = []
    for d in cfg.dates:
        pp, cp = cfg.out / d / "probes.parquet", cfg.out / d / "checks.json"
        sha = _inputs_sha1(d, cfg)
        if cfg.reuse and pp.exists() and cp.exists() and json.loads(cp.read_text()).get("inputs_sha1") == sha:
            chk = json.loads(cp.read_text())
        else:
            X, chk = build_day(d, cfg, idx, fees)
            chk["inputs_sha1"] = sha
            (cfg.out / d).mkdir(parents=True, exist_ok=True)
            X.write_parquet(pp)
            cp.write_text(json.dumps(chk, indent=1, default=str))
            del X
            gc.collect()
        R["checks"][d] = chk
        log.info("queue %s: %s", d, chk)
        days.append(pp)
    P = pl.concat([pl.read_parquet(p) for p in days], how="diagonal_relaxed").with_columns(
        pl.col("stop").cast(pl.String), pl.col(pl.Float32).cast(pl.Float64))
    T: dict[str, pl.DataFrame] = {}
    T["stop_mix"] = A.stop_mix(P)
    T["fill_model"], R["fill_model_coefs"] = A.fill_model(P, cfg.dates)
    # Why three policies: the passive probe (no cancel rule) is the textbook sitting duck; the
    # 11-09 first run showed 58 % of its fills come from the price trading THROUGH it. A real quoter
    # cancels when it is left alone or the fair value crosses its price; those two policies were
    # added after seeing the passive result (stated in the write-up), with a 128 ms cancel latency.
    acc: dict[str, list] = {}
    def add(name, t, pol):
        if t is not None and t.height:
            acc.setdefault(name, []).append(t.with_columns(pl.lit(pol).alias("policy")))
    R["x_star"], R["gate_ours"], R["intensity_fit"] = {}, {}, {}
    for pol in A.POLICIES:
        Pp = A.apply_policy(P, pol)
        grid = Pp.filter(pl.col("design") == "grid")
        btc = grid.filter(pl.col("family") == "btc_15m")
        # ---- Q2 fill probability ------------------------------------------------------------------
        add("fill_type", A.fill_prob(grid, ["family", "type"]), pol)
        add("fill_day", A.fill_prob(grid, ["family", "type", "date"], models=("pess", "matched"), Ts=(10, 60)), pol)
        add("fill_x", A.fill_prob(btc.filter(pl.col("type") == "touch"), ["qtb"], Ts=(1, 10, 60)), pol)
        add("fill_spread", A.fill_prob(btc, ["type", "spread_b"], models=("pess", "matched"), Ts=(10, 60)), pol)
        if pol == "passive":
            add("fill_imb", A.fill_prob(btc, ["type", "imb_b"], models=("matched",), Ts=(10, 60)), pol)
            add("fill_minute", A.fill_prob(btc.filter(pl.col("type") == "touch"), ["minute"], models=("matched",), Ts=(10, 60)), pol)
            R["intensity_fit"] = {}
            for m in ("pess", "matched"):
                g, fit = A.intensity(grid, m)
                add("intensity", g, pol)
                R["intensity_fit"][m] = fit
        # ---- Q3 toxicity of our fills ---------------------------------------------------------------
        F = A.fills_long(grid)
        add("tox_type", A.toxicity(F, ["family", "model", "type"]), pol)
        tx = A.toxicity(F.filter((pl.col("family") == "btc_15m") & pl.col("qpos").is_not_null()), ["model", "qpos"])
        add("tox_x", A.back_of_queue_penalty(tx, ["model"]), pol)
        add("tox_through", A.toxicity(F.filter(pl.col("family") == "btc_15m").with_columns(
            pl.when(pl.col("through")).then(pl.lit("through")).otherwise(pl.lit("at our level")).alias("via")), ["model", "type", "via"]), pol)
        del F
        # ---- Q4 value of a placement --------------------------------------------------------------
        add("value_type", A.value(grid, ["family", "type"]), pol)
        add("value_type_norebate", A.value(grid, ["family", "type"], rebate=False), pol)
        vx = A.value(btc.filter(pl.col("type") == "touch"), ["family", "qtb"])
        add("value_x", vx, pol)
        R["x_star"][pol] = A.x_star(vx)
        add("value_spread", A.value(btc, ["type", "spread_b"], models=("pess", "matched")), pol)
        add("value_imb", A.value(btc, ["type", "imb_b"], models=("matched",)), pol)
        add("value_day", A.value(grid, ["family", "type", "date"], models=("pess", "matched")), pol)
        add("latency", A.latency(Pp.filter(pl.col("family") == "btc_15m")), pol)
        g, R["gate_ours"][pol] = A.gate_ours(Pp, cfg.dates[1:])
        add("gate_ours", g, pol)
        if pol == "passive":  # the fill amount is the passive path's (audit finding 4)
            add("capacity", A.capacity(Pp), pol)
        del Pp, grid, btc
        gc.collect()
    # cancel latency: what each millisecond of cancel delay costs (stale policy, BTC 15m grid)
    for lc in A.CANCEL_LAT_MS:
        Pc = A.apply_policy(P.filter((pl.col("family") == "btc_15m") & (pl.col("design") == "grid")), "stale", lc)
        add("cancel_latency", A.value(Pc, ["type"], models=("pess", "matched")).with_columns(pl.lit(lc).alias("lc_ms")), "stale")
    for k, v in acc.items():
        T[k] = pl.concat(v, how="diagonal_relaxed")
    # ---- calibration against stage 4: our touch fills vs the average maker fill -------------------
    sig = cfg.markouts_root / "table_signature.parquet"
    if sig.exists():
        s4 = pl.read_parquet(sig).filter((pl.col("family") == "btc_15m") & (pl.col("ref") == "fair") & (pl.col("sample") == "balanced") & pl.col("tau").is_in([0, 1, 60]))
        R["stage4_avg_maker"] = {int(r["tau"]): round(r["mean"], 3) for r in s4.iter_rows(named=True)}
    for name, t in T.items():
        if t is not None and t.height:
            t.write_parquet(cfg.out / f"table_{name}.parquet")
    R["probes"] = P.height
    R["seconds"] = round(time.perf_counter() - t0, 1)
    R["params_path"] = str(cfg.out / "queue_params.json")
    (cfg.out / "queue_params.json").write_text(json.dumps(params(T, R), indent=1, default=str))
    (cfg.out / "gate_ours.json").write_text(json.dumps(R, indent=1, default=str))
    if cfg.plots:
        from .plots import make_plots
        try:
            make_plots(T, R, cfg.plot_dir)
        except Exception:
            log.exception("plots failed")
    write_summary(R, T, cfg)
    return R


def params(T: dict, R: dict) -> dict:
    """What stage 7 reads: fill probabilities by type and queue bucket, A and k, a(x), x*, latency."""
    def rows(t, flt):
        return t.filter(flt).to_dicts() if t is not None and t.height else []
    return {
        "dates": R["dates"],
        "models": list(MODELS),
        "fill_prob_btc15m_by_type": rows(T.get("fill_type"), pl.col("family") == "btc_15m"),
        "fill_prob_btc15m_touch_by_touch_size": rows(T.get("fill_x"), pl.lit(True)),
        "intensity_fit": R.get("intensity_fit"),
        "toxicity_btc15m_by_queue_position": rows(T.get("tox_x"), pl.lit(True)),
        "fill_model_coefs": R.get("fill_model_coefs"),
        "x_star": R.get("x_star"),
        "gate_ours": R.get("gate_ours"),
    }


def _fmt(t: pl.DataFrame | None, cols: list[str], nd: int = 2) -> str:
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
        return str(v)
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(f(r[c]) for c in cols) + " |" for r in t.select(cols).iter_rows(named=True)]
    return "\n".join(lines)


def write_summary(R: dict, T: dict, cfg: QueueConfig) -> None:
    btc = pl.col("family") == "btc_15m"
    def t(name, flt=None, sort=None):
        x = T.get(name)
        if x is None or not x.height:
            return None
        x = x.filter(flt) if flt is not None else x
        return x.sort(sort) if sort else x
    S = [f"# Queue and fill model v1 (stage 5) — {', '.join(cfg.dates)}",
         "",
         f"Probes {R['probes']:,} · run {R['seconds']} s · units: cents per share, maker's view · one probe = one 5-share placement. "
         "Cancel models: pess (no cancel ahead of us), prop (uniform), matched (pseudo-L3 + prop), opt (all ahead). "
         "Policies: passive (no cancel rule), alone (cancel when our side moved through our price), stale (alone or fair value crossed our price); cancel latency 128 ms.",
         "", "## Q0 checks", ""]
    for d, c in R["checks"].items():
        S.append(f"- {d}: `{json.dumps(c, default=str)}`")
    S += ["", "## Gate A slice for our orders (step into a > 5¢ spread, 128 ms)", "",
          f"Verdict per policy `{R['gate_ours']}`", "",
          _fmt(t("gate_ours", sort=["policy", "rebate", "model", "tau", "date"]), ["policy", "rebate", "model", "tau", "date", "n", "n_clusters", "mean", "se"]),
          "", "## Q2 fill probability (grid probes, BTC 15m)", "",
          _fmt(t("fill_type", btc & pl.col("T").is_in([10, 60]), ["policy", "type", "T", "model"]), ["policy", "type", "T", "model", "n", "mean", "se"], 3),
          "", "### Ladder and Fed (descriptive, passive)", "",
          _fmt(t("fill_type", ~btc & (pl.col("policy") == "passive") & pl.col("T").is_in([60]) & pl.col("model").is_in(["pess", "matched"]), ["family", "type", "model"]),
               ["family", "type", "T", "model", "n", "mean", "se"], 3),
          "", "### BTC 15m touch, by touch size at the decision (q_touch, shares)", "",
          _fmt(t("fill_x", pl.col("model").is_in(["pess", "matched", "opt"]), ["policy", "T", "qtb", "model"]), ["policy", "T", "qtb", "model", "n", "mean", "se"], 3),
          "", "### Stop reasons", "", _fmt(T["stop_mix"], ["family", "design", "type", "stop", "len", "share"], 3),
          "", "### Fill intensity λ(δ) (fills per resting minute, passive) and the A–S fit", "", f"`{R['intensity_fit']}`", "",
          _fmt(t("intensity", btc), ["model", "delta", "fills", "minutes", "lam_per_min"], 3),
          "", "### Walk-forward fill model, P(fill ≤ 10 s), BTC 15m (passive, matched)", "",
          _fmt(T.get("fill_model"), ["test_day", "n", "rate", "auc", "brier", "brier_base"], 4),
          "", "## Q3 toxicity of our fills (fair reference)", "",
          _fmt(t("tox_type", btc, ["policy", "type", "model"]), ["policy", "type", "model", "n", "h", "M1", "M60", "a1", "a60", "through", "wait_s"]),
          "", "### BTC 15m by queue position taken (0 = improving order alone at a new best; else touch x0 on arrival); D = a(x) − a(0)", "",
          _fmt(t("tox_x", pl.col("model").is_in(["pess", "matched"]), ["policy", "model", "qpos"]), ["policy", "model", "qpos", "n", "h", "M1", "M60", "a1", "a60", "D1", "D60"]),
          "", "### BTC 15m: fills at our level vs the price trading through us", "",
          _fmt(t("tox_through", pl.col("model") == "matched", ["policy", "type", "via"]), ["policy", "type", "via", "n", "h", "M1", "M60", "a1", "a60"]),
          "", f"Stage-4 average maker fill (fair, balanced): `{R.get('stage4_avg_maker')}` (τ 0 = h)",
          "", "## Q4 value of a placement V (incl. rebate)", "",
          _fmt(t("value_type", btc, ["policy", "type", "tau", "model"]), ["policy", "type", "tau", "model", "n", "mean", "se"], 3),
          "", "### Without the rebate", "",
          _fmt(t("value_type_norebate", btc & pl.col("model").is_in(["pess", "matched"]), ["policy", "type", "tau", "model"]), ["policy", "type", "tau", "model", "n", "mean", "se"], 3),
          "", "### Ladder and Fed (descriptive)", "",
          _fmt(t("value_type", ~btc & (pl.col("model") == "matched"), ["family", "policy", "type", "tau"]), ["family", "policy", "type", "tau", "n", "mean", "se"], 3),
          "", "### BTC 15m touch by touch size at the decision", "", f"x* (first bucket with V < 0): `{R['x_star']}`", "",
          _fmt(t("value_x", pl.col("model").is_in(["pess", "matched"]), ["policy", "tau", "qtb", "model"]), ["policy", "tau", "qtb", "model", "n", "mean", "se"], 3),
          "", "### BTC 15m by spread", "", _fmt(t("value_spread", None, ["policy", "type", "spread_b", "tau", "model"]), ["policy", "type", "spread_b", "tau", "model", "n", "mean", "se"], 3),
          "", "### BTC 15m by adverse imbalance (matched)", "", _fmt(t("value_imb", None, ["policy", "type", "imb_b", "tau"]), ["policy", "type", "imb_b", "tau", "n", "mean", "se"], 3),
          "", "### Per day", "", _fmt(t("value_day", btc, ["policy", "type", "tau", "model", "date"]), ["policy", "type", "tau", "model", "date", "n", "mean", "se"], 3),
          "", "## Cancel latency (stale policy, BTC 15m grid): value per placement incl. rebate", "",
          _fmt(t("cancel_latency", None, ["type", "tau", "model", "lc_ms"]), ["type", "tau", "model", "lc_ms", "n", "mean", "se"], 3),
          "", "## Order latency: stepping into a spread opening (BTC 15m)", "",
          _fmt(t("latency", None, ["policy", "col", "lat_ms", "model", "T", "tau"]), ["policy", "col", "model", "T", "tau", "lat_ms", "n", "mean", "se"], 3),
          "", "## Capacity: a 50-share order stepping into every opening (matched, 60 s markout incl. rebate)", "",
          _fmt(t("capacity", None, ["policy", "family", "date", "spread_b"]), ["policy", "family", "date", "spread_b", "placements", "fills", "shares", "usd"], 2),
          "", "### Pseudo-L3 and x0 caveat", "",
          "x0 is read on arrival (after the latency), so it is descriptive only; decisions use the touch size at the decision (qtb)."]
    (cfg.out / "summary.md").write_text("\n".join(S) + "\n")

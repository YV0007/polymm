"""`polymm dashboard`: gather every number the research dashboard shows into ONE JSON file.

Why one file: the dashboard (a claude.ai artifact) reads attached files, and one upload per
refresh keeps the daily update a single step; each section of the page is a dataset that
picks its own key out of this file.

Sections (each a list of row objects, so every cell can be traced on the page):
    kpis            one row: the headline figures
    collection      per UTC day: rows recorded per table, GB on disk, source (laptop/server)
    quality         per day and family: strict clean share; per day: FAIL/WARN/PASS counts
    fair_scores     held-out fair-value scores per family, candidate and horizon (D-023)
    fair_minute     btc_15m skill by minute of the window
    fair_w          ŵ(τ): share of the model–book gap the mid closes
    fair_calib      btc_15m calibration bins
    markout_curve   average maker markout per family, reference (mid/fair) and horizon (D-024)
    markout_days    per-day markout at τ* and 60 s, with and without the rebate
    model_metrics   expectation model vs baselines (R², correlation)
    decision        drop the worst x % of fills by the model's score: edge of what is kept
    gate            Gate A verdict per family + the reasons
    checks          pass/fail of every automated check we run
    history         the headline figures at every past export (for trends)
"""
from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

TABLES = {"book_events": "events", "trades": "events", "tob": "events", "spot_quotes": "spot", "spot_trades": "spot"}


def _rows(path: Path) -> int:
    n = 0
    for f in path.rglob("*.parquet"):
        try:
            n += pq.ParquetFile(f).metadata.num_rows
        except Exception:  # a file being written (.tmp is excluded by the glob) or truncated
            pass
    return n


def _gb(path: Path) -> float:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1e9


def collection(data_dirs: dict[str, Path]) -> list[dict]:
    """Rows per day and table. Why both roots: days before 2026-11-07 09:00 UTC exist only in
    the laptop recording (`data`), later ones on the server copy (`data_vps`); for a day in
    both, the server copy wins."""
    days: dict[str, dict] = {}
    for src, root in data_dirs.items():
        be = root / "events" / "book_events"
        if not be.exists():
            continue
        for d in sorted(p.name[3:] for p in be.glob("dt=*")):
            if d in days and days[d]["source"] == "server":
                continue
            row = {"date": d, "source": src}
            for t, layer in TABLES.items():
                row[t] = _rows(root / layer / t / f"dt={d}")
            row["hours"] = len(list((be / f"dt={d}").glob("hour=*")))
            row["gb"] = round(sum(_gb(root / layer / t / f"dt={d}") for t, layer in TABLES.items())
                              + _gb(root / "raw" / "ws_market" / f"dt={d}") + _gb(root / "raw" / "binance" / f"dt={d}"), 3)
            days[d] = row
    return [days[d] for d in sorted(days)]


def quality(day_dirs: list[Path]) -> tuple[list[dict], list[dict]]:
    """Parse each day's quality.md: the FAIL/WARN/PASS line and the strict clean share rows."""
    status, clean = [], []
    for dd in day_dirs:
        q = dd / "quality.md"
        if not q.exists():
            continue
        txt = q.read_text()
        m = re.search(r"\*\*(\d+) FAIL · (\d+) WARN · (\d+) PASS · (\d+) SKIP\*\*", txt)
        if m:
            status.append({"date": dd.name, "fail": int(m[1]), "warn": int(m[2]), "pass": int(m[3]), "skip": int(m[4])})
        for fam, val in re.findall(r"\| coverage \| clean share \(strict\) \| (\w+) \| ([\d.]+) % \|", txt):
            clean.append({"date": dd.name, "family": fam, "strict_clean_pct": float(val)})
    return status, clean


def _records(p: Path, cols: list[str] | None = None, where: pl.Expr | None = None) -> list[dict]:
    if not p.exists():
        return []
    d = pl.read_parquet(p)
    if where is not None:
        d = d.filter(where)
    if cols:
        d = d.select([c for c in cols if c in d.columns])
    d = d.with_columns([pl.col(c).cast(pl.String) for c, t in d.schema.items() if t == pl.Categorical])
    out = []
    for r in d.iter_rows(named=True):
        out.append({k: (None if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))) else
                        round(v, 6) if isinstance(v, float) else v) for k, v in r.items()})
    return out


def markout_curve(p: Path) -> list[dict]:
    """table_signature has one row per (family, column, sample); columns are h_mid/h_fair (the
    spread captured at the fill, τ = 0), M<τ>_<ref> and M_res (to resolution). Long form for charts."""
    if not p.exists():
        return []
    s = pl.read_parquet(p).filter(pl.col("sample") == "balanced")
    rows = []
    for r in s.iter_rows(named=True):
        col = r["col"]
        m = re.fullmatch(r"(h|M(\d+))_(mid|fair)", col)
        if not m:
            continue
        tau = 0 if m[1] == "h" else int(m[2])
        rows.append({"family": r["family"], "ref": m[3], "tau": tau, "mean_c": round(r["mean"], 4),
                     "se_c": round(r["se"], 4), "n": r["n"], "clusters": r["n_clusters"]})
    return sorted(rows, key=lambda x: (x["family"], x["ref"], x["tau"]))


def run_tests(repo: Path) -> dict:
    r = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"], cwd=repo, capture_output=True, text=True)
    last = (r.stdout.strip().splitlines() or [""])[-1]
    passed = int(m[1]) if (m := re.search(r"(\d+) passed", last)) else 0
    failed = int(m[1]) if (m := re.search(r"(\d+) failed", last)) else 0
    return {"passed": passed, "failed": failed, "summary": last}


def build(repo: Path, out: Path, fair_root: Path, mk_root: Path, data_dirs: dict[str, Path], tests: bool = True) -> dict:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=repo, capture_output=True, text=True).stdout.strip()
    fair_params = json.loads((fair_root / "fair_params.json").read_text()) if (fair_root / "fair_params.json").exists() else {}
    gate = json.loads((mk_root / "gate.json").read_text()) if (mk_root / "gate.json").exists() else {}
    coll = collection(data_dirs)
    day_dirs = sorted(p for p in fair_root.glob("20*-*-*") if p.is_dir())
    q_status, q_clean = quality(day_dirs)
    fair_scores = _records(fair_root / "table_headline.parquet",
                           ["family", "cand", "tau", "n", "n_clusters", "skill", "skill_se", "b", "b_se", "hit", "brier", "brier_mid"])
    t = run_tests(repo) if tests else {"passed": None, "failed": None, "summary": "not run"}

    def pick(rows, **kv):
        for r in rows:
            if all(r.get(k) == v for k, v in kv.items()):
                return r
        return {}

    f30 = pick(fair_scores, family="btc_15m", cand="x_fair", tau=30)
    s30 = pick(fair_scores, family="btc_15m", cand="x_stoikov", tau=30)
    curve = markout_curve(mk_root / "table_signature.parquet")
    h0 = pick(curve, family="btc_15m", ref="fair", tau=0)
    m1 = pick(curve, family="btc_15m", ref="fair", tau=1)
    m60 = pick(curve, family="btc_15m", ref="fair", tau=60)
    dec = _records(mk_root / "table_decision.parquet")
    keep_half = pick(dec, family="btc_15m", scorer="ridge", drop=0.5)
    all_fills = pick(dec, family="btc_15m", scorer="const", drop=0.0)
    checks = gate.get("checks", {})
    kpis = {
        "generated_at": now, "commit": commit,
        "data_through": max((r["date"] for r in coll), default=None),
        "days_recorded": len(coll), "gb_recorded": round(sum(r["gb"] for r in coll), 2),
        "book_rows_m": round(sum(r["book_events"] for r in coll) / 1e6, 1),
        "fills_analyzed": gate.get("fills_used"),
        "fair_test_days": ", ".join(fair_params.get("dates", [])[1:]),
        "fair_skill30_15m": f30.get("skill"), "fair_skill30_15m_se": f30.get("skill_se"),
        "stoikov_skill30_15m": s30.get("skill"),
        "fair_brier_15m": f30.get("brier"), "mid_brier_15m": f30.get("brier_mid"),
        "h_fair_15m_c": h0.get("mean_c"), "m1_fair_15m_c": m1.get("mean_c"), "m60_fair_15m_c": m60.get("mean_c"),
        "edge_all_15m_c": all_fills.get("edge_c"), "edge_keep_half_15m_c": keep_half.get("edge_c"),
        "gate_verdict": gate.get("verdict"), "gate_15m": gate.get("verdict_family", {}).get("btc_15m"),
        "tau_star_15m_s": gate.get("tau_star", {}).get("btc_15m"),
        "tests_passed": t["passed"], "tests_failed": t["failed"],
    }
    # checks: everything automated that can pass or fail, one row each
    chk = []
    for r in q_status:
        chk.append({"area": "data quality", "check": f"quality report {r['date']}", "result": f"{r['fail']} FAIL · {r['warn']} WARN · {r['pass']} PASS",
                    "ok": r["fail"] == 0})
    fs = fair_root / "summary.md"
    if fs.exists():
        txt = fs.read_text()
        if (m := re.search(r"reference: `(\{.*?\})`", txt)):
            ref = ast.literal_eval(m[1])  # a dict literal in our own summary file
            chk += [
                {"area": "fair value", "check": "15m strike chain: finalPrice(k) = priceToBeat(k+1)", "result": f"{ref['chain_pairs'] - ref['chain_mismatch']} of {ref['chain_pairs']}", "ok": ref["chain_mismatch"] == 0},
                {"area": "fair value", "check": "Up iff final ≥ strike", "result": f"{ref['rule_checked'] - ref['rule_mismatch']} of {ref['rule_checked']}", "ok": ref["rule_mismatch"] == 0},
                {"area": "fair value", "check": "Gamma outcome = our recorded resolution", "result": f"{ref['ws_resolutions_compared'] - ref['ws_resolution_mismatch']} of {ref['ws_resolutions_compared']}", "ok": ref["ws_resolution_mismatch"] == 0},
                {"area": "fair value", "check": "ladder strikes parsed", "result": f"{ref['ladder_markets'] - ref['ladder_strike_missing']} of {ref['ladder_markets']}", "ok": ref["ladder_strike_missing"] == 0},
            ]
        chk.append({"area": "fair value", "check": "no input received after t (lookahead)", "result": "asserted on every row", "ok": "inputs " in txt})
    for d, c in sorted(checks.items()):
        chk.append({"area": "adverse selection", "check": f"fills {d}: trade side consistent with the book", "result": f"{c.get('sign_ok', 0) * 100:.1f} %", "ok": c.get("sign_ok", 0) >= 0.95})
    chk.append({"area": "code", "check": "test suite", "result": t["summary"], "ok": bool(t["passed"]) and not t["failed"]})
    # history: one snapshot per data date (a rerun on the same data replaces it)
    hist_path = out.parent / "history.json"
    hist = json.loads(hist_path.read_text()) if hist_path.exists() else []
    snap = {k: kpis[k] for k in ("data_through", "generated_at", "days_recorded", "fills_analyzed", "fair_skill30_15m",
                                 "fair_brier_15m", "m1_fair_15m_c", "edge_keep_half_15m_c", "tests_passed")}
    hist = [h for h in hist if h.get("data_through") != snap["data_through"]] + [snap]
    hist.sort(key=lambda h: h.get("data_through") or "")
    data = {
        "kpis": [kpis],
        "collection": coll,
        "quality": q_status,
        "clean": q_clean,
        "fair_scores": fair_scores,
        "fair_minute": _records(fair_root / "table_by_minute_15m.parquet", ["cand", "min_b", "tau", "n", "skill", "skill_se", "hit"]),
        "fair_w": _records(fair_root / "table_w_hat.parquet", ["family", "min_b", "absd_b", "hte_b", "tau", "w_hat", "w_se", "n"]),
        "fair_calib": _records(fair_root / "table_calibration_15m.parquet", ["col", "bin", "pred", "obs", "n", "brier"]),
        "markout_curve": curve,
        "markout_days": _records(mk_root / "table_gate_unconditional.parquet",
                                 ["family", "scope", "tau", "n", "h_fair", "M", "M_se", "M_reb", "M_reb_se", "M60", "M60_se", "M60_reb", "M60_reb_se"]),
        "model_metrics": _records(mk_root / "table_model_metrics.parquet"),
        "decision": [r for r in dec if r["family"] == "btc_15m"],
        "gate": [{"family": f, "verdict": v, "tau_star_s": gate.get("tau_star", {}).get(f),
                  "reasons": " | ".join(x for x in gate.get("verdict_why", []) if f in x)} for f, v in gate.get("verdict_family", {}).items()],
        "checks": chk,
        "history": hist,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, allow_nan=False, default=str))
    tmp.replace(out)
    hist_path.write_text(json.dumps(hist, indent=1))
    return data

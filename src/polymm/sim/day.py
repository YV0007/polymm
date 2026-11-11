"""Glue between one day's inputs (inputs.py) and the compiled engine (engine.py).

prepare()  slices the day's streams per YES token into numpy arrays ONCE; every strategy run
           of the day reuses them (a grid of hundreds of runs must not re-slice 50 M rows).
run_day()  one strategy setting over every segment of the day; returns fills and orders as
           tables with the market facts attached. Inventory carries across a token's segments
           (a data gap cancels our orders but not our position).
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl

from . import engine as E
from .inputs import NS, TICK

JITTER = 0.126  # D-031: VPS → CLOB round trip p50 52 ms, p95 64 ms → σ of log latency ≈ ln(64/52)/1.645


@dataclass
class Prepared:
    date: str
    tok: dict            # yes → dict of arrays
    seg: pl.DataFrame    # segments with family, outcome, …
    lag_sec0: int
    lag_extra: np.ndarray


def _split(df: pl.DataFrame, cols: dict[str, type]) -> dict[str, dict[str, np.ndarray]]:
    out = {}
    for (yes,), g in df.partition_by("yes", as_dict=True, maintain_order=True).items():
        out[yes] = {k: g[k].to_numpy().astype(dt) for k, dt in cols.items()}
    return out


def prepare(S: dict, date: str) -> Prepared:
    seg = S["segments"]
    yes = seg["yes"].unique().to_list()
    # Why a stable sort by (yes, t): the level stream is stored by (level, t); the engine needs one
    # time-ordered stream per token, and updates sharing a timestamp keep their original order
    el = S["levels"].filter(pl.col("yes").is_in(yes)).sort("yes", "t", maintain_order=True)
    tr = S["trades"].filter(pl.col("yes").is_in(yes)).sort("yes", "t", maintain_order=True)
    bk = S["tob"].filter(pl.col("yes").is_in(yes)).sort("yes", "t", maintain_order=True)
    fr = S["fair"].filter(pl.col("yes").is_in(yes)).with_columns(pl.col("v2").fill_null(0.0)).sort("yes", "t", maintain_order=True)
    ELc = {"t": np.int64, "s": np.int64, "pk": np.int64, "cons": np.float64, "res": np.float64, "L": np.float64,
           "addt": np.int64, "tord": np.int64}
    parts = {"el": _split(el.select(["yes", *ELc]), ELc),
             "tr": _split(tr.select("yes", "t", "s", "pk", "v"), {"t": np.int64, "s": np.int64, "pk": np.int64, "v": np.float64}),
             "bk": _split(bk.select("yes", "t", "bid_pk", "ask_pk"), {"t": np.int64, "bid_pk": np.int64, "ask_pk": np.int64}),
             "fr": _split(fr.select("yes", "t", "p", "v2"), {"t": np.int64, "p": np.float64, "v2": np.float64})}
    empty = {"el": {k: np.zeros(0, dt) for k, dt in ELc.items()},
             "tr": {"t": np.zeros(0, np.int64), "s": np.zeros(0, np.int64), "pk": np.zeros(0, np.int64), "v": np.zeros(0)},
             "bk": {"t": np.zeros(0, np.int64), "bid_pk": np.zeros(0, np.int64), "ask_pk": np.zeros(0, np.int64)},
             "fr": {"t": np.zeros(0, np.int64), "p": np.zeros(0), "v2": np.zeros(0)}}
    tok = {y: {k: parts[k].get(y, empty[k]) for k in parts} for y in yes}
    lag = S["lag"]
    if lag.height:
        s0 = int(lag["sec"].min())
        ext = np.zeros(int(lag["sec"].max()) - s0 + 1)
        ext[(lag["sec"].to_numpy() - s0)] = lag["extra_ms"].fill_null(0.0).to_numpy()
    else:
        s0, ext = 0, np.zeros(1)
    return Prepared(date, tok, seg, s0, ext)


_NOSC = (np.zeros(0, np.int64), np.zeros(0, np.int64), np.zeros(0, np.int64), np.zeros(0), np.zeros(0, np.int64))


def run_day(P: Prepared, rung: int, prm: np.ndarray, model: str = "matched", lat_ms: float = 75.0, seed: int = 0,
            vtab: np.ndarray | None = None, vtab_n: np.ndarray | None = None, vmin_n: int = 200,
            family: str = "btc_15m", script: dict | None = None, maxo: int = 16, maxf: int = 20000,
            burst: bool = True, fair_noise_c: float = 0.0, fair_lead_ms: float = 0.0, queue_front: bool = False,
            cx_lat_ms: float = -1.0) -> dict:
    """Run one setting over the day's segments of `family`. Returns fills, orders, per-segment
    summary (inventory at the end, peak inventory, decisions, rejections).
    fair_noise_c > 0 replaces the fair value by fair + N(0, noise) per tick: the random-signal
    check (a signal with no information must not beat the true fair value).
    Oracle switches (diagnosis, D-034; each removes one handicap, so the P&L gain bounds that lever):
    fair_lead_ms   the strategy sees the fair value this many ms EARLIER than it happened (a better
                   or faster BTC signal); attribution still uses the true fair value
    queue_front    our order is first in line at its price on arrival
    cx_lat_ms      cancel latency, separately from the order latency (-1 = the same)"""
    rng = np.random.default_rng(seed + 991)
    vt = vtab if vtab is not None else np.zeros((3, 4, 4))
    vn = vtab_n if vtab_n is not None else np.zeros((3, 4, 4))
    lag_extra = P.lag_extra if burst else np.zeros_like(P.lag_extra)
    seg = P.seg.filter(pl.col("family") == family)
    F, O, Sx = [], [], []
    q_by_yes: dict[str, float] = {}
    for k, r in enumerate(seg.iter_rows(named=True)):
        a = P.tok[r["yes"]]
        sc = _NOSC
        if script is not None:
            s = script.get(r["yes"])
            if s is None:
                continue
            sc = (s["t"], s["s"], s["pk"], s["sz"], s["tc"])
        q0 = q_by_yes.get(r["yes"], 0.0)
        fp = a["fr"]["p"]
        ft = a["fr"]["t"] - np.int64(fair_lead_ms * 1e6) if fair_lead_ms else a["fr"]["t"]
        if fair_noise_c > 0 and len(fp):
            fp = np.clip(fp + rng.normal(0.0, fair_noise_c / 100.0, len(fp)), 0.001, 0.999)
        out = E.run_segment(a["el"]["t"], a["el"]["s"], a["el"]["pk"], a["el"]["cons"], a["el"]["res"], a["el"]["L"], a["el"]["addt"],
                            a["el"]["tord"], a["tr"]["t"], a["tr"]["s"], a["tr"]["pk"], a["tr"]["v"],
                            a["bk"]["t"], a["bk"]["bid_pk"], a["bk"]["ask_pk"], ft, fp, a["fr"]["v2"],
                            np.int64(P.lag_sec0), lag_extra, np.int64(r["t0"]), np.int64(r["t1"]), np.int64(r["t_end_s"] * NS),
                            np.int64(r["t_start_s"] or 0), float(q0), np.int64(rung), prm, np.int64(E.MODEL_ID[model]), float(lat_ms),
                            float(JITTER), np.int64(seed * 100003 + k), np.int64(TICK),
                            vt, np.float64(vmin_n), vn, *sc, np.int64(max(maxo, len(sc[0]) + 4) if script else maxo), np.int64(maxf),
                            bool(queue_front), float(cx_lat_ms))
        (f_t, f_tpre, f_s, f_pk, f_q, f_thr, f_xarr, f_typ, f_oid, f_inv,
         g_id, g_tsend, g_tarr, g_tend, g_s, g_pk, g_sz, g_fill, g_end, g_typ, q, q_peak, n_dec, n_rej, n_ord, ovf,
         f_cxp, f_tarr, f_tsend) = out
        q_by_yes[r["yes"]] = q
        base = {"yes": r["yes"], "condition_id": r["condition_id"], "seg": k}
        if len(f_t):
            F.append(pl.DataFrame({"t": f_t, "t_pre": f_tpre, "s": f_s, "pk": f_pk, "qty": f_q, "through": f_thr, "x_arr": f_xarr,
                                   "typ": f_typ, "oid": f_oid, "inv": f_inv,
                                   "cx_pending": f_cxp, "t_arr_o": f_tarr, "t_send_o": f_tsend}).with_columns(**{c: pl.lit(v) for c, v in base.items()}))
        if len(g_id):
            O.append(pl.DataFrame({"oid": g_id, "t_send": g_tsend, "t_arr": g_tarr, "t_end": g_tend, "s": g_s, "pk": g_pk, "size": g_sz,
                                   "filled": g_fill, "end": g_end, "typ": g_typ}).with_columns(**{c: pl.lit(v) for c, v in base.items()}))
        Sx.append(base | {"t0": r["t0"], "t1": r["t1"], "t_start_s": r["t_start_s"], "t_end_s": r["t_end_s"], "outcome": r["outcome"],
                          "q0": q0, "q_end": q, "q_peak": q_peak, "decisions": n_dec, "rejected": n_rej, "orders": n_ord,
                          # Why count missing order-log rows too: every strategy order ends exactly once
                          # (filled / cancelled / rejected / segment end); fewer log rows = truncation
                          "overflow": ovf + (max(0, n_ord - len(g_id)) if rung >= 0 else 0)})
    fills = pl.concat(F) if F else pl.DataFrame()
    orders = pl.concat(O) if O else pl.DataFrame()
    return {"fills": fills, "orders": orders, "segments": pl.DataFrame(Sx) if Sx else pl.DataFrame()}

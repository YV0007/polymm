"""Q1: virtual orders ("probes") replayed through the recorded book.

A probe is an order that never existed: placed at decision time t on side s (+1 bid = buy YES,
-1 ask = sell YES) at price p, it reaches the book at t + latency and joins the BACK of the
queue there. It changes nothing in the book (small-order assumption). From then on, at its
level, per update (see events.py):

    cons v (a taker ate v from the front)   x ← x − v; beyond x it eats us: E += v − x
    res > 0 (add)                           nothing: new orders queue behind us
    res < 0 (cancel c)                      x ← x − c_ahead, by cancel model m (below)
    after every update                      x ← min(x, L): there cannot be more ahead than
                                            the whole level holds
    a trade at a price WORSE than p          the taker went through our price: E += v
    (bid: maker-bid trade at pk < p)

E is the volume that reached us; a size-q order has filled min(q, E) shares.

Cancel models (the share of a cancel c that came from AHEAD of us), run side by side:
    0 pessimistic   none                    a lower bound on fills
    1 proportional  c · x / L_before        cancels hit the line uniformly
    2 matched       pseudo-L3: a matched cancel whose order joined before us is ahead
                    (all of it), after us is behind (none); unmatched → proportional
    3 optimistic    min(c, x)               an upper bound on fills

A probe stops at the earliest of: its horizon (60 s), the end of its clean window or of the
market, being left behind (best price on our side 2 ticks better than p: a real quoter would
re-price; a competing risk, not censoring), or being rejected on arrival (post-only: p would
cross the opposite best).
"""
from __future__ import annotations

import numpy as np
import polars as pl
from numba import njit

from ..markouts.fills import NS
from .events import TICK

MODELS = ("pess", "prop", "matched", "opt")
QS = np.array([5.0, 50.0, 200.0])
HORIZON_NS = 60 * NS
LEFT_BEHIND_TICKS = 2
LB = LEFT_BEHIND_TICKS * TICK
STOP = {0: "horizon", 1: "window_end", 2: "left_behind", 3: "rejected", 4: "filled"}


@njit(cache=True)
def _first_gt(a, lo, hi, v):
    """First index i in [lo, hi) with a[i] > v (a sorted): everything before it happened at or
    before v, which is the "last update at or before t" convention used throughout."""
    while lo < hi:
        mid = (lo + hi) // 2
        if a[mid] <= v:
            lo = mid + 1
        else:
            hi = mid
    return lo


@njit(cache=True)
def run_probes(p_lvl, p_grp, p_book, p_s, p_pk, p_tarr, p_tmax,
               ev_t, ev_cons, ev_res, ev_L, ev_addt, ev_tord, lvl_lo, lvl_hi,
               tr_t, tr_pk, tr_v, grp_lo, grp_hi,
               bk_t, bk_bid, bk_ask, book_lo, book_hi, qs,
               fr_sec, fr_pk, fair_lo, fair_hi):
    n = len(p_s)
    M = 4
    nq = len(qs)
    x0 = np.zeros(n)
    t_stop = np.zeros(n, np.int64)
    reason = np.zeros(n, np.int8)
    t_front = np.full((n, M), -1, np.int64)
    t_first = np.full((n, M), -1, np.int64)
    first_through = np.zeros((n, M), np.bool_)
    t_full = np.full((n, M, nq), -1, np.int64)
    filled = np.zeros((n, M, nq))
    x_end = np.zeros((n, M))
    t_alone = np.full(n, -1, np.int64)
    t_stale = np.full(n, -1, np.int64)
    t_pre = np.full((n, M), -1, np.int64)
    pos_arr = np.zeros(n, np.int64)
    for i in range(n):
        s = p_s[i]
        p = p_pk[i]
        ta = p_tarr[i]
        tmax = p_tmax[i]
        # ---- book on arrival: post-only rejection, then the left-behind time ------------------
        b0, b1 = book_lo[p_book[i]], book_hi[p_book[i]]
        j = _first_gt(bk_t, b0, b1, ta) - 1
        if j < b0:
            t_stop[i] = ta
            reason[i] = 3
            continue
        if (s == 1 and bk_ask[j] <= p) or (s == -1 and bk_bid[j] >= p):
            t_stop[i] = ta
            reason[i] = 3
            continue
        tend = tmax
        why = 0 if tmax - ta >= HORIZON_NS else 1
        # where our price sits against the best on arrival, in ticks: 0 at the touch, > 0 behind
        # it, < 0 better than it (an improving order, or a touch order whose level was swept
        # while it was in flight)
        pos_arr[i] = ((bk_bid[j] - p) if s == 1 else (p - bk_ask[j])) // TICK
        # "alone": the rest of our side moved THROUGH our price (for a bid: the real best bid fell
        # below both p and where it stood on arrival) – our order is the stale one left behind.
        ref = bk_bid[j] if s == 1 else bk_ask[j]
        lim = (p if p < ref else ref) if s == 1 else (p if p > ref else ref)
        k = j
        while k < b1 and bk_t[k] <= tend:
            if k > j and t_alone[i] < 0 and ((s == 1 and bk_bid[k] < lim) or (s == -1 and bk_ask[k] > lim)):
                t_alone[i] = bk_t[k]
            if (s == 1 and bk_bid[k] >= p + LB) or (s == -1 and bk_ask[k] <= p - LB):
                tend = bk_t[k] if bk_t[k] > ta else ta
                why = 2
                break
            k += 1
        # "stale": the fair value (1 s grid, state at each whole second) crossed our price
        f0, f1 = fair_lo[p_book[i]], fair_hi[p_book[i]]
        if f1 > f0:
            sec0 = (ta + 999_999_999) // 1_000_000_000
            f = _first_gt(fr_sec, f0, f1, sec0 - 1)
            while f < f1 and fr_sec[f] * 1_000_000_000 <= tend:
                if (s == 1 and fr_pk[f] < p) or (s == -1 and fr_pk[f] > p):
                    t_stale[i] = fr_sec[f] * 1_000_000_000
                    break
                f += 1
        # ---- queue on arrival ------------------------------------------------------------------
        lv = p_lvl[i]
        if lv >= 0:
            e0, e1 = lvl_lo[lv], lvl_hi[lv]
            e = _first_gt(ev_t, e0, e1, ta)
            L = ev_L[e - 1] if e > e0 else 0.0
        else:
            e, e1 = 0, 0
            L = 0.0
        x0[i] = L
        x = np.full(M, L)
        E = np.zeros(M)
        # trades through our price (worse for the taker than p), same token, our maker side
        g0, g1 = grp_lo[p_grp[i]], grp_hi[p_grp[i]]
        g = _first_gt(tr_t, g0, g1, ta)
        # ---- merge level updates and through-trades in time order ------------------------------
        while True:
            te = ev_t[e] if e < e1 else np.int64(9223372036854775807)
            # next through-trade
            while g < g1 and not ((s == 1 and tr_pk[g] < p) or (s == -1 and tr_pk[g] > p)):
                g += 1
            tg = tr_t[g] if g < g1 else np.int64(9223372036854775807)
            tn = te if te <= tg else tg
            if tn > tend:
                break
            if te <= tg:
                v = ev_cons[e]
                r = ev_res[e]
                Lb = L
                L = ev_L[e]
                for m in range(M):
                    if v > 0:
                        if x[m] >= v:
                            x[m] -= v
                        else:
                            E[m] += v - x[m]
                            x[m] = 0.0
                    if r < 0 and x[m] > 0:
                        c = -r
                        if m == 1 or (m == 2 and ev_addt[e] < 0):
                            base = Lb - v if Lb - v > 0 else 0.0
                            ahead = c * x[m] / base if base > 0 else c
                        elif m == 2:
                            ahead = c if ev_addt[e] <= ta else 0.0
                        elif m == 3:
                            ahead = c
                        else:
                            ahead = 0.0
                        x[m] = x[m] - ahead if x[m] > ahead else 0.0
                    if x[m] > L:
                        x[m] = L
                src_t = ev_tord[e]
                e += 1
                through = False
            else:
                for m in range(M):
                    x[m] = 0.0
                    E[m] += tr_v[g]
                src_t = tr_t[g]
                g += 1
                through = True
            for m in range(M):
                if x[m] <= 0 and t_front[i, m] < 0:
                    t_front[i, m] = tn
                if E[m] > 0 and t_first[i, m] < 0:
                    t_first[i, m] = tn
                    t_pre[i, m] = src_t
                    first_through[i, m] = through
                for q in range(nq):
                    if E[m] >= qs[q] and t_full[i, m, q] < 0:
                        t_full[i, m, q] = tn
            done = True
            for m in range(M):
                if E[m] < qs[nq - 1]:
                    done = False
            if done:
                tend = tn
                why = 4
                break
        t_stop[i] = tend
        reason[i] = why
        for m in range(M):
            x_end[i, m] = x[m]
            for q in range(nq):
                filled[i, m, q] = E[m] if E[m] < qs[q] else qs[q]
    return x0, t_stop, reason, t_front, t_first, first_through, t_full, filled, x_end, t_alone, t_stale, t_pre, pos_arr


# ------------------------------------------------------------------------------------------------
# probe placement
# ------------------------------------------------------------------------------------------------
GRID_S = 2           # base grid: every 2 s, our latency
SWEEP_S = 10         # latency sweep: every 10 s
# Why 128 ms: a pessimistic stand-in for the ORDER PATH (decision → our order resting in the book).
# It is the Binance feed's median lag, not Polymarket's (that is ~11 ms) — D-031. The replay runs
# on our receive clock, which already contains our feed lag, so only the order path is added
# here; measured VPS → CLOB round trip is ~46–52 ms (p50), ~25 ms one way + unknown venue time.
BASE_LAT_MS = 128
SWEEP_LAT_MS = (0, 50, 300, 1000)


def _decisions(tob: pl.DataFrame, windows: pl.DataFrame, idx: pl.DataFrame, every_s: int) -> pl.DataFrame:
    """Decision instants on a regular grid inside each strict clean window (and before the market
    closes), with the book at that instant."""
    mk = idx.filter(pl.col("oi") == 0).select("yes", "family", "condition_id", "t_start_s", "t_end_s", "outcome").unique("yes")
    w = windows.join(mk, on="yes", how="inner").with_columns(
        pl.min_horizontal("t_last", pl.col("t_end_s") * NS).alias("t_hi"))
    w = w.filter(pl.col("t_hi") - pl.col("t_start") > 5 * NS)
    g = (w.with_columns(pl.int_ranges((pl.col("t_start") // (every_s * NS)) + 1, pl.col("t_hi") // (every_s * NS)).alias("k"))
         .explode("k").drop_nulls("k").with_columns((pl.col("k") * every_s * NS).alias("t_dec")).drop("k"))
    g = g.sort("t_dec").join_asof(tob.select("t", "yes", "bid_pk", "ask_pk", "bid_sz", "ask_sz", "mid").sort("t"),
                                  left_on="t_dec", right_on="t", by="yes", strategy="backward")
    return g.drop("t").drop_nulls("bid_pk")


def _openings(tob: pl.DataFrame, windows: pl.DataFrame, idx: pl.DataFrame) -> pl.DataFrame:
    """Triggered decisions: the instants the spread opens to ≥ 2 ticks (a gap to step into)."""
    mk = idx.filter(pl.col("oi") == 0).select("yes", "family", "condition_id", "t_start_s", "t_end_s", "outcome").unique("yes")
    o = (tob.select("t", "yes", "bid_pk", "ask_pk", "bid_sz", "ask_sz", "mid")
         .with_columns((pl.col("ask_pk") - pl.col("bid_pk")).alias("spr"))
         .with_columns(pl.col("spr").shift(1).over("yes").alias("spr_prev"))
         .filter((pl.col("spr") >= 2 * TICK) & (pl.col("spr_prev") < 2 * TICK)))
    o = o.sort("t").join_asof(windows.sort("t_start"), left_on="t", right_on="t_start", by="yes", strategy="backward")
    o = o.filter(pl.col("t_start").is_not_null() & (pl.col("t") < pl.col("t_last"))).join(mk, on="yes", how="inner")
    o = o.with_columns(pl.min_horizontal("t_last", pl.col("t_end_s") * NS).alias("t_hi")).filter(pl.col("t") < pl.col("t_hi") - NS)
    return o.rename({"t": "t_dec"}).drop("spr", "spr_prev")


def make_probes(tob: pl.DataFrame, windows: pl.DataFrame, idx: pl.DataFrame, full: tuple[str, ...] = ("btc_15m",)) -> pl.DataFrame:
    """Every probe of one day. Families in `full`: grid (2 s, 128 ms), latency sweep (10 s), spread
    openings. Others (descriptive only): grid every 10 s at 128 ms.
    Why: the ladder and Fed were 70 % of all probes on 11-08 but cannot support a verdict
    (stage 4: too few fills); a coarser grid keeps three days inside 8 GB of memory."""
    fam = pl.col("family").is_in(list(full))
    # Why live window only for BTC 15m: its markets are listed up to ~30 min before their window
    # and sit flat at 0.50 with no fair value (no strike yet); 70 % of the first run's grid fell
    # there and diluted every number (audit 2026-11-09, finding 1).
    live = ~(pl.col("family") == "btc_15m") | (pl.col("t_dec") >= pl.col("t_start_s") * NS)
    parts = []
    for name, dec, lats in (("grid", pl.concat([_decisions(tob, windows, idx, GRID_S).filter(fam),
                                                _decisions(tob, windows, idx, SWEEP_S).filter(~fam)]), (BASE_LAT_MS,)),
                            ("sweep", _decisions(tob, windows, idx, SWEEP_S).filter(fam), SWEEP_LAT_MS),
                            ("opening", _openings(tob, windows, idx).filter(fam), (BASE_LAT_MS, *SWEEP_LAT_MS))):
        dec = dec.filter(live & pl.col("bid_pk").is_between(50, 950) & pl.col("ask_pk").is_between(50, 950) & (pl.col("ask_pk") > pl.col("bid_pk")))
        for s in (1, -1):
            best = pl.col("bid_pk") if s == 1 else pl.col("ask_pk")
            spr = pl.col("ask_pk") - pl.col("bid_pk")
            types = [("improve", best + s * TICK, spr >= 2 * TICK)]
            if name != "opening":
                types = [("touch", best, pl.lit(True)), ("behind", best - s * TICK, pl.lit(True))] + types
            for typ, price, ok in types:
                for lat in lats:
                    parts.append(dec.filter(ok).with_columns(
                        pl.lit(name).alias("design"), pl.lit(typ).alias("type"), pl.lit(s, pl.Int8).alias("s"),
                        price.alias("pk"), pl.lit(lat).alias("lat_ms")))
    P = pl.concat(parts, how="diagonal_relaxed")
    P = P.with_columns(
        (pl.col("t_dec") + pl.col("lat_ms") * 1_000_000).alias("t_arr"),
        ((pl.col("ask_pk") - pl.col("bid_pk")) / TICK).alias("spread_c"),
        pl.when(pl.col("s") == -1).then(pl.col("bid_sz") / (pl.col("bid_sz") + pl.col("ask_sz")))
          .otherwise(pl.col("ask_sz") / (pl.col("bid_sz") + pl.col("ask_sz"))).alias("imb_adv"),
        pl.when(pl.col("s") == 1).then(pl.col("bid_sz")).otherwise(pl.col("ask_sz")).alias("q_touch"),
    )
    P = P.with_columns(pl.min_horizontal(pl.col("t_hi"), pl.col("t_arr") + HORIZON_NS).alias("t_max")).filter(pl.col("t_arr") < pl.col("t_max"))
    return P.with_row_index("pid")


def _offsets(keys: np.ndarray, n: int) -> tuple[np.ndarray, np.ndarray]:
    lo = np.searchsorted(keys, np.arange(n), "left")
    hi = np.searchsorted(keys, np.arange(n), "right")
    return lo.astype(np.int64), hi.astype(np.int64)


def simulate(P: pl.DataFrame, el: pl.DataFrame, tr: pl.DataFrame, tob: pl.DataFrame, fair: pl.DataFrame | None = None) -> pl.DataFrame:
    """Run every probe; returns P plus, per cancel model m, its fill path columns, and the two
    cancel triggers t_alone / t_stale (-1 = never) that the policies in analysis.py act on."""
    # level ids (sorted by level, t already); trade groups by (yes, maker side); book by yes
    lvl = el.select("yes", "s", "pk", "level").unique("level").sort("level")
    nl = lvl.height
    lvl_lo, lvl_hi = _offsets(el["level"].to_numpy(), nl)
    grp = tr.select("yes", "s").unique(maintain_order=True).with_row_index("grp")
    trg = tr.join(grp, on=["yes", "s"], how="left").sort("grp", "t")
    grp_lo, grp_hi = _offsets(trg["grp"].to_numpy(), grp.height)
    bk_ids = tob.select("yes").unique(maintain_order=True).with_row_index("book")
    tb = tob.join(bk_ids, on="yes", how="left").sort("book", "t")
    book_lo, book_hi = _offsets(tb["book"].to_numpy(), bk_ids.height)
    if fair is not None and fair.height:
        fq = (fair.select(pl.col("asset_id").alias("yes"), "sec", (pl.col("p_fair") * 1000).alias("fpk"))
              .join(bk_ids, on="yes", how="inner").drop_nulls("fpk").sort("book", "sec"))
    else:
        fq = pl.DataFrame({"book": [], "sec": [], "fpk": []}, schema={"book": pl.UInt32, "sec": pl.Int64, "fpk": pl.Float64})
    fair_lo, fair_hi = _offsets(fq["book"].to_numpy(), bk_ids.height)
    Q = (P.join(lvl, on=["yes", "s", "pk"], how="left").join(grp, on=["yes", "s"], how="left").join(bk_ids, on="yes", how="left")
         .with_columns(pl.col("level").fill_null(-1), pl.col("grp").fill_null(-1), pl.col("book").fill_null(-1))
         .filter(pl.col("book") >= 0).sort("pid"))
    # a probe with no trade group still runs: point it at an empty slice
    empty = grp.height
    grp_lo = np.append(grp_lo, 0)
    grp_hi = np.append(grp_hi, 0)
    g = Q["grp"].to_numpy().astype(np.int64)
    g[g < 0] = empty
    out = run_probes(Q["level"].to_numpy().astype(np.int64), g, Q["book"].to_numpy().astype(np.int64),
                     Q["s"].to_numpy().astype(np.int64), Q["pk"].to_numpy().astype(np.int64),
                     Q["t_arr"].to_numpy().astype(np.int64), Q["t_max"].to_numpy().astype(np.int64),
                     el["t"].to_numpy(), el["cons"].to_numpy(), el["res"].to_numpy(), el["L"].to_numpy(), el["addt"].to_numpy(),
                     el["tord"].to_numpy() if "tord" in el.columns else el["t"].to_numpy(),
                     lvl_lo, lvl_hi,
                     trg["t"].to_numpy(), trg["pk"].to_numpy().astype(np.int64), trg["v"].to_numpy(), grp_lo, grp_hi,
                     tb["t"].to_numpy(), tb["bid_pk"].to_numpy().astype(np.int64), tb["ask_pk"].to_numpy().astype(np.int64),
                     book_lo, book_hi, QS,
                     fq["sec"].to_numpy().astype(np.int64), fq["fpk"].to_numpy().astype(np.float64), fair_lo, fair_hi)
    x0, t_stop, reason, t_front, t_first, thr, t_full, filled, x_end, t_alone, t_stale, t_pre, pos_arr = out
    cols = {"x0": x0, "t_stop": t_stop, "stop": reason, "t_alone": t_alone, "t_stale": t_stale, "pos_arr": pos_arr}
    for mi, m in enumerate(MODELS):
        cols[f"t_front_{m}"] = t_front[:, mi]
        cols[f"t_first_{m}"] = t_first[:, mi]
        cols[f"t_pre_{m}"] = t_pre[:, mi]
        cols[f"through_{m}"] = thr[:, mi]
        cols[f"x_end_{m}"] = x_end[:, mi]
        for qi, q in enumerate(QS):
            cols[f"t_full{int(q)}_{m}"] = t_full[:, mi, qi]
            cols[f"filled{int(q)}_{m}"] = filled[:, mi, qi]
    res = Q.drop("level", "grp", "book").with_columns(**{k: pl.Series(k, v) for k, v in cols.items()})
    return res.with_columns(pl.col("stop").replace_strict(STOP, return_dtype=pl.String).alias("stop"))

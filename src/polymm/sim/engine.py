"""S1–S2: the event engine and the strategy ladder, compiled (numba).

One run = one market segment (a strict clean window inside the live window), one strategy, one
cancel model, one latency setting. Events are processed in receive-clock order:

    market   top of book · level update (queue) · trade through a price · fair-value tick
    ours     order arrives at the book · cancel takes effect · strategy timer

Ties: market events at a time t are processed before our own events at t (our order that lands
at t sees the book after everything stamped t; a cancel landing at t is too late for a fill at t).
This is stage 5's convention (`_first_gt`: "at or before t" happened before us).

Queue position of a resting order (stage 5, D-026), per update at its level:
    a taker consumed v from the front      x ← x − v; beyond x it fills us
    someone cancelled c                    x ← x − (share of c ahead of us, by cancel model)
    someone joined                         nothing (time priority: behind us)
    after every update                     x ← min(x, level size)
    a trade at a price worse than ours     the taker went through our price: we fill
Our order is not in the recorded level sizes (small-order assumption) and never changes the book.

Latency (D-031): the replay clock already contains our feed lag, so only the order path is
added: lat = lat_ms · exp(jitter · Z) + burst, burst = the Polymarket feed lag above normal in
the previous second (the venue/feed is slow in bursts, and bursts are when prices jump).
A cancel is sent after the order it cancels on the same connection: it never lands before it.

Strategy ladder (rung r includes every rung below it):
    0  symmetric      quote fair ± h, re-quote only on a 2 s timer (the blind quoter of stage 5)
    1  event-driven   re-quote on every book / fair-value change (hysteresis: keep an order within
                      `hyst` ticks of the target unless the fair value crossed it): stale cancel
    2  inventory      reservation price r = m − q γ σ_p² (T − t); last `end_s` seconds only the
                      side that reduces inventory
    3  guards         pull the side a jump will hit: fair moved ≥ jump_c in jump_ms → pull for hold_ms
    4  selective      choose improve / touch / behind per side by the predicted value table V̂
                      (stage-5 probes of earlier days); quote only if V̂ > v_thr, never beyond r
    5  rewards        (a reward market) join the best price while it is within h of the reservation
                      price, else rest at r ∓ h; rungs 1–3 apply; rewards scored after the run
   -1  scripted       fixed orders (send time, side, price, size, cancel time): the stage-5 check
Every rung keeps the inventory limit |q| ≤ qmax (a risk limit, not a strategy).

One decision per instant (D-038): market events that share a receive timestamp (a book update and
the fair-value tick it causes, the several level updates of one message) are all applied before
the strategy runs once on the final state. Why: deciding after each of them sent an order on a
half-updated state and the 20 ms rate limiter then blocked the correction.
"""
from __future__ import annotations

import math

import numpy as np
from numba import njit

BIG = np.int64(9223372036854775807)
NSL = 1_000_000_000
MS = 1_000_000

# parameter vector layout (float64): one array so the compiled kernel takes any rung
P_H, P_SIZE, P_REQ_MS, P_GAMMA, P_QMAX, P_END_S, P_JUMP_C, P_JUMP_MS, P_HOLD_MS, P_VTHR, P_GAP_MS, P_HYST = range(12)
NPARAM = 12
DEFAULTS = {"h": 1.0, "size": 10.0, "req_ms": 2000.0, "gamma": 0.0, "qmax": 100.0, "end_s": 0.0, "jump_c": 1e9,
            "jump_ms": 500.0, "hold_ms": 1000.0, "v_thr": 0.0, "gap_ms": 20.0, "hyst": 0.0}
NAMES = ["h", "size", "req_ms", "gamma", "qmax", "end_s", "jump_c", "jump_ms", "hold_ms", "v_thr", "gap_ms", "hyst"]

ENGINE_VERSION = 2  # bump when a change alters results on the same inputs (reuse of stored runs checks it)
MODEL_ID = {"pess": 0, "prop": 1, "matched": 2, "opt": 3}
SKEW_CAP = 0.05  # the inventory lean |r − m| never exceeds 5 ¢
# order end codes
END = {1: "filled", 2: "cancelled", 3: "rejected", 4: "segment_end"}
# placement types for rung 4 (and labels for every fill)
TYPES = ("improve", "touch", "behind", "other")
SPREAD_B = ("1c", "2c", "3-5c", ">5c")
MIN_B = ("00-04", "05-09", "10-12", "13-14")


def params(**kw) -> np.ndarray:
    p = np.array([DEFAULTS[n] for n in NAMES], dtype=np.float64)
    for k, v in kw.items():
        p[NAMES.index(k)] = float(v)
    return p


@njit(cache=True)
def _spread_b(spr_ticks):
    if spr_ticks <= 1:
        return 0
    if spr_ticks == 2:
        return 1
    if spr_ticks <= 5:
        return 2
    return 3


@njit(cache=True)
def _min_b(minute):
    if minute <= 4:
        return 0
    if minute <= 9:
        return 1
    if minute <= 12:
        return 2
    return 3


@njit(cache=True)
def _latency(lat_ms, jitter, burst_ms):
    if lat_ms <= 0.0:
        return np.int64(0)
    return np.int64((lat_ms * math.exp(jitter * np.random.normal()) + burst_ms) * MS)


@njit(cache=True)
def _type_of(s, pk, bid, ask, tick):
    """improve = strictly better than the best on our side, touch = at it, behind = 1 tick
    worse, other = further."""
    if s == 1:
        d = pk - bid
    else:
        d = ask - pk
    if d > 0:
        return 0
    if d == 0:
        return 1
    if d == -tick:
        return 2
    return 3


@njit(cache=True)
def run_segment(el_t, el_s, el_pk, el_cons, el_res, el_L, el_addt, el_tord,
                tr_t, tr_s, tr_pk, tr_v,
                bk_t, bk_bid, bk_ask,
                fr_t, fr_p, fr_v2,
                lag_sec0, lag_extra,
                t0, t1, t_end, t_wstart, q0,
                rung, prm, model, lat_ms, jitter, seed, tick,
                vtab, vmin_n, vtab_n,
                sc_t, sc_s, sc_pk, sc_sz, sc_tc,
                maxo, maxf, queue_front, cx_lat_ms):
    np.random.seed(seed)
    # ---------------- state ----------------
    size_b = np.zeros(1001)       # others' size per price, bid side (pk 0..1000)
    size_a = np.zeros(1001)
    live_b = np.zeros(1001, np.int64)  # how many of our resting orders sit at each price
    live_a = np.zeros(1001, np.int64)
    o_st = np.zeros(maxo, np.int64)       # 0 free, 1 in flight, 2 resting
    o_s = np.zeros(maxo, np.int64)
    o_pk = np.zeros(maxo, np.int64)
    o_sz = np.zeros(maxo)
    o_fill = np.zeros(maxo)
    o_x = np.zeros(maxo)
    o_E = np.zeros(maxo)
    o_tsend = np.zeros(maxo, np.int64)
    o_tarr = np.zeros(maxo, np.int64)
    o_tcx = np.full(maxo, BIG)            # cancel takes effect (BIG = none)
    o_xarr = np.zeros(maxo)
    o_typ = np.zeros(maxo, np.int64)
    o_id = np.zeros(maxo, np.int64)
    # outputs: fills
    f_t = np.zeros(maxf, np.int64)
    f_tpre = np.zeros(maxf, np.int64)
    f_s = np.zeros(maxf, np.int64)
    f_pk = np.zeros(maxf, np.int64)
    f_q = np.zeros(maxf)
    f_thr = np.zeros(maxf, np.bool_)
    f_xarr = np.zeros(maxf)
    f_typ = np.zeros(maxf, np.int64)
    f_oid = np.zeros(maxf, np.int64)
    f_inv = np.zeros(maxf)               # inventory after the fill
    f_cxp = np.zeros(maxf, np.bool_)     # our cancel of this order was already in flight
    f_tarr = np.zeros(maxf, np.int64)    # when the order reached the book
    f_tsend = np.zeros(maxf, np.int64)   # when we decided to send it
    # Why a separate cancel latency: the oracle "instant cancel" isolates the cancel race
    cx_lat = lat_ms if cx_lat_ms < 0 else cx_lat_ms
    nf = 0
    # outputs: orders
    maxlog = maxf * 4
    g_id = np.zeros(maxlog, np.int64)
    g_tsend = np.zeros(maxlog, np.int64)
    g_tarr = np.zeros(maxlog, np.int64)
    g_tend = np.zeros(maxlog, np.int64)
    g_s = np.zeros(maxlog, np.int64)
    g_pk = np.zeros(maxlog, np.int64)
    g_sz = np.zeros(maxlog)
    g_fill = np.zeros(maxlog)
    g_end = np.zeros(maxlog, np.int64)
    g_typ = np.zeros(maxlog, np.int64)
    ng = 0
    next_id = 0
    q = q0
    q_peak = abs(q0)
    n_dec = 0
    n_rej = 0
    overflow = 0
    bid = np.int64(-1)
    ask = np.int64(-1)
    m = -1.0
    v2 = 0.0
    j_back = 0
    pull_b_until = np.int64(-1)
    pull_a_until = np.int64(-1)
    pending = False  # a decision owed once the last market event of this instant is applied
    last_act_b = np.int64(0)  # Why 0, not -BIG: t − (−BIG) overflows int64
    last_act_a = np.int64(0)
    next_timer = t0
    req_ns = np.int64(prm[P_REQ_MS] * MS)
    gap_ns = np.int64(prm[P_GAP_MS] * MS)
    # the rate limiter starts one gap before t0, so the first order of a segment is never held back
    last_act_b = t0 - gap_ns
    last_act_a = t0 - gap_ns
    n_el, n_tr, n_bk, n_fr, n_sc = len(el_t), len(tr_t), len(bk_t), len(fr_t), len(sc_t)
    # ---------------- warm-up: book state up to t0 (no orders) ----------------
    i_el = 0
    while i_el < n_el and el_t[i_el] <= t0:
        if el_s[i_el] == 1:
            size_b[el_pk[i_el]] = el_L[i_el]
        else:
            size_a[el_pk[i_el]] = el_L[i_el]
        i_el += 1
    i_bk = 0
    while i_bk < n_bk and bk_t[i_bk] <= t0:
        bid = bk_bid[i_bk]
        ask = bk_ask[i_bk]
        i_bk += 1
    i_fr = 0
    while i_fr < n_fr and fr_t[i_fr] <= t0:
        m = fr_p[i_fr]
        v2 = fr_v2[i_fr]
        i_fr += 1
    i_tr = 0
    while i_tr < n_tr and tr_t[i_tr] <= t0:
        i_tr += 1
    i_sc = 0
    while i_sc < n_sc and sc_t[i_sc] < t0:
        i_sc += 1

    t = t0
    while True:
        # ---------- next market event (ties: book, level, trade, fair) ----------
        tb = bk_t[i_bk] if i_bk < n_bk else BIG
        te = el_t[i_el] if i_el < n_el else BIG
        tt = tr_t[i_tr] if i_tr < n_tr else BIG
        tf = fr_t[i_fr] if i_fr < n_fr else BIG
        tm = min(min(tb, te), min(tt, tf))
        # ---------- next own event ----------
        ti = BIG
        for k in range(maxo):
            if o_st[k] == 1 and o_tarr[k] < ti:
                ti = o_tarr[k]
            if o_st[k] >= 1 and o_tcx[k] < ti:
                ti = o_tcx[k]
        if rung == -1:
            if i_sc < n_sc and sc_t[i_sc] < ti:
                ti = sc_t[i_sc]
        elif rung >= 0 and next_timer < ti:
            ti = next_timer
        if tm >= t1 and ti >= t1:
            break
        decide = False
        if tm <= ti:
            t = tm
            if tb == tm:
                bid = bk_bid[i_bk]
                ask = bk_ask[i_bk]
                i_bk += 1
                decide = True
            elif te == tm:
                s = el_s[i_el]
                p = el_pk[i_el]
                Lb = size_b[p] if s == 1 else size_a[p]
                L = el_L[i_el]
                if s == 1:
                    size_b[p] = L
                else:
                    size_a[p] = L
                nlive = live_b[p] if s == 1 else live_a[p]
                if nlive > 0:
                    v = el_cons[i_el]
                    r = el_res[i_el]
                    for k in range(maxo):
                        if o_st[k] != 2 or o_s[k] != s or o_pk[k] != p:
                            continue
                        x = o_x[k]
                        if v > 0:
                            if x >= v:
                                x -= v
                            else:
                                o_E[k] += v - x
                                x = 0.0
                        if r < 0 and x > 0:
                            c = -r
                            if model == 1 or (model == 2 and el_addt[i_el] < 0):
                                base = Lb - v if Lb - v > 0 else 0.0
                                ahead = c * x / base if base > 0 else c
                            elif model == 2:
                                ahead = c if el_addt[i_el] <= o_tarr[k] else 0.0
                            elif model == 3:
                                ahead = c
                            else:
                                ahead = 0.0
                            x = x - ahead if x > ahead else 0.0
                        if x > L:
                            x = L
                        o_x[k] = x
                        newf = o_E[k] if o_E[k] < o_sz[k] else o_sz[k]
                        if newf > o_fill[k] + 1e-12:
                            qty = newf - o_fill[k]
                            o_fill[k] = newf
                            q += o_s[k] * qty
                            if abs(q) > q_peak:
                                q_peak = abs(q)
                            if nf < maxf:
                                f_t[nf] = t
                                f_tpre[nf] = el_tord[i_el]
                                f_s[nf] = o_s[k]
                                f_pk[nf] = p
                                f_q[nf] = qty
                                f_thr[nf] = False
                                f_xarr[nf] = o_xarr[k]
                                f_typ[nf] = o_typ[k]
                                f_oid[nf] = o_id[k]
                                f_inv[nf] = q
                                f_cxp[nf] = o_tcx[k] != BIG
                                f_tarr[nf] = o_tarr[k]
                                f_tsend[nf] = o_tsend[k]
                                nf += 1
                            else:
                                overflow += 1
                            decide = True
                i_el += 1
            elif tt == tm:
                s = tr_s[i_tr]
                pt = tr_pk[i_tr]
                for k in range(maxo):
                    if o_st[k] != 2 or o_s[k] != s:
                        continue
                    if (s == 1 and pt < o_pk[k]) or (s == -1 and pt > o_pk[k]):
                        o_x[k] = 0.0
                        o_E[k] += tr_v[i_tr]
                        newf = o_E[k] if o_E[k] < o_sz[k] else o_sz[k]
                        if newf > o_fill[k] + 1e-12:
                            qty = newf - o_fill[k]
                            o_fill[k] = newf
                            q += o_s[k] * qty
                            if abs(q) > q_peak:
                                q_peak = abs(q)
                            if nf < maxf:
                                f_t[nf] = t
                                f_tpre[nf] = tr_t[i_tr]
                                f_s[nf] = o_s[k]
                                f_pk[nf] = o_pk[k]
                                f_q[nf] = qty
                                f_thr[nf] = True
                                f_xarr[nf] = o_xarr[k]
                                f_typ[nf] = o_typ[k]
                                f_oid[nf] = o_id[k]
                                f_inv[nf] = q
                                f_cxp[nf] = o_tcx[k] != BIG
                                f_tarr[nf] = o_tarr[k]
                                f_tsend[nf] = o_tsend[k]
                                nf += 1
                            else:
                                overflow += 1
                            decide = True
                i_tr += 1
            else:
                m = fr_p[i_fr]
                v2 = fr_v2[i_fr]
                i_fr += 1
                decide = True
            # orders that are completely filled leave the book
            for k in range(maxo):
                if o_st[k] == 2 and o_fill[k] >= o_sz[k] - 1e-12:
                    if ng < maxlog:
                        g_id[ng] = o_id[k]; g_tsend[ng] = o_tsend[k]; g_tarr[ng] = o_tarr[k]; g_tend[ng] = t
                        g_s[ng] = o_s[k]; g_pk[ng] = o_pk[k]; g_sz[ng] = o_sz[k]; g_fill[ng] = o_fill[k]; g_end[ng] = 1
                        g_typ[ng] = o_typ[k]
                        ng += 1
                    if o_s[k] == 1:
                        live_b[o_pk[k]] -= 1
                    else:
                        live_a[o_pk[k]] -= 1
                    o_st[k] = 0
                    o_tcx[k] = BIG
            # one decision per instant: more market events stamped t → apply them first
            tb = bk_t[i_bk] if i_bk < n_bk else BIG
            te = el_t[i_el] if i_el < n_el else BIG
            tt = tr_t[i_tr] if i_tr < n_tr else BIG
            tf = fr_t[i_fr] if i_fr < n_fr else BIG
            if min(min(tb, te), min(tt, tf)) == t:
                if decide:
                    pending = True
                continue
            if pending:
                decide = True
                pending = False
        else:
            t = ti
            # arrivals first, then cancels landing at the same instant
            for k in range(maxo):
                if o_st[k] == 1 and o_tarr[k] == t:
                    s = o_s[k]
                    p = o_pk[k]
                    rej = bid < 0 or ask < 0 or (s == 1 and ask <= p) or (s == -1 and bid >= p)
                    if rej:
                        n_rej += 1
                        if ng < maxlog:
                            g_id[ng] = o_id[k]; g_tsend[ng] = o_tsend[k]; g_tarr[ng] = t; g_tend[ng] = t
                            g_s[ng] = s; g_pk[ng] = p; g_sz[ng] = o_sz[k]; g_fill[ng] = 0.0; g_end[ng] = 3; g_typ[ng] = 3
                            ng += 1
                        o_st[k] = 0
                        o_tcx[k] = BIG
                        decide = True
                    else:
                        o_st[k] = 2
                        # queue_front: the oracle "first in line" (x = 0 on arrival)
                        o_x[k] = 0.0 if queue_front else (size_b[p] if s == 1 else size_a[p])
                        o_xarr[k] = o_x[k]
                        o_E[k] = 0.0
                        o_typ[k] = _type_of(s, p, bid, ask, tick)
                        if s == 1:
                            live_b[p] += 1
                        else:
                            live_a[p] += 1
            for k in range(maxo):
                if o_st[k] >= 1 and o_tcx[k] == t:
                    if ng < maxlog:
                        g_id[ng] = o_id[k]; g_tsend[ng] = o_tsend[k]; g_tarr[ng] = o_tarr[k]; g_tend[ng] = t
                        g_s[ng] = o_s[k]; g_pk[ng] = o_pk[k]; g_sz[ng] = o_sz[k]; g_fill[ng] = o_fill[k]; g_end[ng] = 2
                        g_typ[ng] = o_typ[k]
                        ng += 1
                    if o_st[k] == 2:
                        if o_s[k] == 1:
                            live_b[o_pk[k]] -= 1
                        else:
                            live_a[o_pk[k]] -= 1
                    o_st[k] = 0
                    o_tcx[k] = BIG
            if rung == -1:
                while i_sc < n_sc and sc_t[i_sc] == t:
                    for k in range(maxo):
                        if o_st[k] == 0:
                            o_st[k] = 1
                            o_s[k] = sc_s[i_sc]
                            o_pk[k] = sc_pk[i_sc]
                            o_sz[k] = sc_sz[i_sc]
                            o_fill[k] = 0.0
                            o_tsend[k] = t
                            o_tarr[k] = t
                            o_tcx[k] = sc_tc[i_sc] if sc_tc[i_sc] >= t else BIG
                            o_id[k] = i_sc
                            break
                    i_sc += 1
            elif rung == 0 and t == next_timer:
                next_timer = t + req_ns
                decide = True
            elif rung >= 1 and t == next_timer:
                # one decision at the segment start: event-driven rungs otherwise wait for the next
                # book or fair-value change before quoting
                next_timer = BIG
                decide = True
        # ---------------- strategy ----------------
        if rung < 0 or not decide or t < t0 or t >= t1:
            continue
        if rung == 0 and t != next_timer - req_ns:
            continue
        n_dec += 1
        want_b = np.int64(-1)
        want_a = np.int64(-1)
        typ_b = 3
        typ_a = 3
        ok = m > 0 and bid >= 50 and ask <= 950 and ask > bid
        if ok:
            # jump detector: the fair value now vs jump_ms ago
            if rung >= 3:
                tb0 = t - np.int64(prm[P_JUMP_MS] * MS)
                while j_back + 1 < i_fr and fr_t[j_back + 1] <= tb0:
                    j_back += 1
                if fr_t[j_back] <= tb0:
                    dm = (m - fr_p[j_back]) * 100.0
                    if dm >= prm[P_JUMP_C]:
                        pull_a_until = t + np.int64(prm[P_HOLD_MS] * MS)
                    elif dm <= -prm[P_JUMP_C]:
                        pull_b_until = t + np.int64(prm[P_HOLD_MS] * MS)
            r = m
            tau = (t_end - t) / NSL
            if rung >= 2:
                sk = q * prm[P_GAMMA] * v2 * tau
                # Why a cap: σ_p²·(T − t) has a fat tail (p90 5× the median); an uncapped lean can
                # push a quote far through the book, which is a taker exit, not a skew (audit 1)
                if sk > SKEW_CAP:
                    sk = SKEW_CAP
                elif sk < -SKEW_CAP:
                    sk = -SKEW_CAP
                r = m - sk
            h = prm[P_H] / 100.0
            if rung == 5:
                # rewards: join the best price while it is on our side of r and within h of it
                # (inside the reward band when h = v/2); otherwise rest at r ∓ h, which still scores.
                # Why 0 ≤ r − bid: a best bid ABOVE r would have us buy above fair (audit 2)
                rk = r * 1000.0
                jb = 0.0 <= rk - bid <= prm[P_H] * 10.0
                ja = 0.0 <= ask - rk <= prm[P_H] * 10.0
                want_b = bid if jb else np.int64(math.floor((r - h) * 1000.0 / tick + 1e-9)) * tick
                want_a = ask if ja else np.int64(math.ceil((r + h) * 1000.0 / tick - 1e-9)) * tick
                if want_b >= ask:
                    want_b = ask - tick
                if want_a <= bid:
                    want_a = bid + tick
            elif rung == 4:
                # value table: [type, spread bucket, minute bucket]; best placement per side
                sb = _spread_b((ask - bid) // tick)
                mb = _min_b((t // NSL - t_wstart) // 60)
                rk = r * 1000.0
                bestv = -1e18
                for ty in range(3):
                    p = bid + tick if ty == 0 else (bid if ty == 1 else bid - tick)
                    if ty == 0 and ask - bid < 2 * tick:
                        continue
                    if p > rk or vtab_n[ty, sb, mb] < vmin_n:
                        continue
                    if vtab[ty, sb, mb] > bestv:
                        bestv = vtab[ty, sb, mb]
                        want_b = p
                        typ_b = ty
                if bestv <= prm[P_VTHR]:
                    want_b = -1
                bestv = -1e18
                for ty in range(3):
                    p = ask - tick if ty == 0 else (ask if ty == 1 else ask + tick)
                    if ty == 0 and ask - bid < 2 * tick:
                        continue
                    if p < rk or vtab_n[ty, sb, mb] < vmin_n:
                        continue
                    if vtab[ty, sb, mb] > bestv:
                        bestv = vtab[ty, sb, mb]
                        want_a = p
                        typ_a = ty
                if bestv <= prm[P_VTHR]:
                    want_a = -1
            else:
                want_b = np.int64(math.floor((r - h) * 1000.0 / tick + 1e-9)) * tick
                want_a = np.int64(math.ceil((r + h) * 1000.0 / tick - 1e-9)) * tick
                if want_b >= ask:
                    want_b = ask - tick
                if want_a <= bid:
                    want_a = bid + tick
            if want_b < 50 or want_b > 950:
                want_b = -1
            if want_a < 50 or want_a > 950:
                want_a = -1
            # risk limit, end of the market, guards
            if q >= prm[P_QMAX]:
                want_b = -1
            if q <= -prm[P_QMAX]:
                want_a = -1
            if rung >= 2 and tau < prm[P_END_S]:
                if q >= 0:
                    want_b = -1
                if q <= 0:
                    want_a = -1
            if rung >= 3:
                if t < pull_b_until:
                    want_b = -1
                if t < pull_a_until:
                    want_a = -1
        # ---------------- reconcile wanted vs our orders, per side ----------------
        for s in (1, -1):
            want = want_b if s == 1 else want_a
            last = last_act_b if s == 1 else last_act_a
            keep = -1
            for k in range(maxo):
                if o_st[k] == 0 or o_s[k] != s or o_tcx[k] != BIG:
                    continue
                if want >= 0 and keep < 0:
                    if o_pk[k] == want:
                        keep = k
                    elif rung >= 1 and prm[P_HYST] > 0 and abs(o_pk[k] - want) <= prm[P_HYST] * tick:
                        # hysteresis: keep a nearby order unless the fair value crossed it
                        if (s == 1 and o_pk[k] <= m * 1000.0) or (s == -1 and o_pk[k] >= m * 1000.0):
                            keep = k
                if keep == k:
                    continue
                # cancel: lands after the latency, never before the order itself
                sec = t // NSL - lag_sec0
                burst = lag_extra[sec] if 0 <= sec < len(lag_extra) else 0.0
                tcx = t + _latency(cx_lat, jitter, burst)
                if tcx < o_tarr[k]:
                    tcx = o_tarr[k]
                o_tcx[k] = tcx
            if want >= 0 and keep < 0 and t - last >= gap_ns:
                for k in range(maxo):
                    if o_st[k] == 0:
                        sec = t // NSL - lag_sec0
                        burst = lag_extra[sec] if 0 <= sec < len(lag_extra) else 0.0
                        o_st[k] = 1
                        o_s[k] = s
                        o_pk[k] = want
                        o_sz[k] = prm[P_SIZE]
                        o_fill[k] = 0.0
                        o_tsend[k] = t
                        o_tarr[k] = t + _latency(lat_ms, jitter, burst)
                        o_tcx[k] = BIG
                        o_id[k] = next_id
                        next_id += 1
                        if s == 1:
                            last_act_b = t
                        else:
                            last_act_a = t
                        break
    # segment end: everything still out is gone (we stop quoting at a data gap / market end)
    for k in range(maxo):
        if o_st[k] >= 1:
            if ng < maxlog:
                g_id[ng] = o_id[k]; g_tsend[ng] = o_tsend[k]; g_tarr[ng] = o_tarr[k]; g_tend[ng] = t1
                g_s[ng] = o_s[k]; g_pk[ng] = o_pk[k]; g_sz[ng] = o_sz[k]; g_fill[ng] = o_fill[k]; g_end[ng] = 4
                g_typ[ng] = o_typ[k] if o_st[k] == 2 else 3
                ng += 1
    return (f_t[:nf], f_tpre[:nf], f_s[:nf], f_pk[:nf], f_q[:nf], f_thr[:nf], f_xarr[:nf], f_typ[:nf], f_oid[:nf], f_inv[:nf],
            g_id[:ng], g_tsend[:ng], g_tarr[:ng], g_tend[:ng], g_s[:ng], g_pk[:ng], g_sz[:ng], g_fill[:ng], g_end[:ng], g_typ[:ng],
            q, q_peak, n_dec, n_rej, next_id, overflow, f_cxp[:nf], f_tarr[:nf], f_tsend[:nf])

"""Replay: rebuild the order book at any past moment from stored events.

The recorder never writes a full book to disk, only events (`book_events`):
snapshots (whole book) and deltas (new total size at one level). Everything
downstream (spread, depth, microprice, markouts, the simulator) needs the book
*as of* a moment, so this module is the foundation: it feeds stored history
through the same `BookState` the live recorder uses, in the same order
(`ts_recv_ns`, per asset), and writes derived tables.

Reading:   polars reads the Parquet files whose [min, max] ts_recv_ns overlaps
           the requested window (from Parquet statistics; the hour= partition is
           set by a flush's FIRST row, so a file can hold later timestamps),
           sorts them, and hands the engine one time chunk at a time.
Engine:    one `_Asset` per token: BookState + cached top of book + a cheap
           level-set fingerprint + the last second of history, so a REST
           snapshot can be matched against *any* instant within +-match_window.
Outputs:   derived/tob, derived/depth_1s, derived/events_labeled,
           derived/clean_windows, derived/rest_checks (see `DERIVED` schemas).

Mirror rule (decision A1, DATA_PLAN §5): from 2026-11-07 the recorder stores
outcome 1 of a market only where it is not the exact mirror of outcome 0. The
engine replays a market's two tokens together: an outcome-0 delta frame that
has no outcome-1 rows is mirrored onto the sibling (side flipped, price 1-p).
On data recorded before A1 both tokens are always present, so the rule is a
no-op there; the same code handles both eras.
"""
from __future__ import annotations

import glob
import logging
import math
import os
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, NamedTuple

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from .book import BookState, flip_price

log = logging.getLogger(__name__)

NS = 1_000_000_000
_OPP = {"BUY": "SELL", "SELL": "BUY"}
_SIDE_I = {"BUY": 0, "SELL": 1}
SNAPSHOT_TYPES = ("snapshot_ws", "snapshot_rest")

# ---- derived table schemas ------------------------------------------------------
# Why here and not records.py: records.py is the recorder's (another session is
# editing it); the derived layer is owned by replay.

TOB = pa.schema(
    [
        ("ts_recv_ns", pa.int64()),
        ("asset_id", pa.string()),
        ("event_type", pa.string()),  # delta | snapshot_ws | snapshot_rest | reconnect | resync
        ("best_bid", pa.float64()),
        ("bid_size", pa.float64()),
        ("best_ask", pa.float64()),
        ("ask_size", pa.float64()),
        ("mid", pa.float64()),
        ("microprice", pa.float64()),
        ("spread", pa.float64()),
        ("valid", pa.bool_()),
    ]
)

DEPTH_LEVELS = 5


def _depth_schema(n: int) -> pa.Schema:
    """Wide layout: one row per (grid instant, asset). bid_px_k / ask_px_k is the
    k-th best price (k = 1 is best), bid_cum_k / ask_cum_k the CUMULATIVE size
    from the best level through level k; level k's own size = cum_k - cum_{k-1}.
    Missing levels are null. Why wide: `select asset_id, bid_cum_3 from depth_1s`
    is what every depth question looks like; a long layout needs a pivot first."""
    cols = [("ts_recv_ns", pa.int64()), ("asset_id", pa.string()), ("valid", pa.bool_()),
            ("n_bid_levels", pa.int32()), ("n_ask_levels", pa.int32())]
    for side in ("bid", "ask"):
        for k in range(1, n + 1):
            cols.append((f"{side}_px_{k}", pa.float64()))
        for k in range(1, n + 1):
            cols.append((f"{side}_cum_{k}", pa.float64()))
    return pa.schema(cols)


DEPTH_1S = _depth_schema(DEPTH_LEVELS)

EVENTS_LABELED = pa.schema(
    [
        ("ts_recv_ns", pa.int64()),
        ("asset_id", pa.string()),
        ("side", pa.string()),  # BUY = bid side, SELL = ask side (book side of the level)
        ("price", pa.float64()),
        ("size_before", pa.float64()),
        ("size_after", pa.float64()),
        ("size_change", pa.float64()),  # after - before
        ("label", pa.string()),  # add | fill | cancel | unchanged
        ("trade_ts_recv_ns", pa.int64()),  # the matched trade (fill only)
        ("trade_size", pa.float64()),
        ("trade_lag_ns", pa.int64()),  # delta ts - trade ts (negative: delta came first)
        ("mirrored", pa.bool_()),  # derived from the sibling token's stored row (A1)
        ("conn_id", pa.int32()),
    ]
)

CLEAN_WINDOWS = pa.schema(
    [
        ("asset_id", pa.string()),
        ("t_start", pa.int64()),
        ("t_end", pa.int64()),
        ("start_kind", pa.string()),  # snapshot_ws | snapshot_rest | rest_resync
        ("end_reason", pa.string()),  # reconnect | rest_gap | end
        ("last_verified", pa.int64()),  # ts of the last REST check that matched inside the window
        ("n_events", pa.int64()),
    ]
)

REST_CHECKS = pa.schema(
    [
        ("ts_recv_ns", pa.int64()),  # the REST snapshot's receive time
        ("asset_id", pa.string()),
        ("family", pa.string()),
        ("in_window", pa.bool_()),  # book initialised and not stale when the snapshot arrived
        ("matched", pa.bool_()),
        ("kind", pa.string()),  # exact | before | after | init | gap
        ("lag_ns", pa.int64()),  # ts of the matching stream state - REST ts (negative: REST behind)
        ("n_rest_levels", pa.int32()),
        ("n_diff", pa.int32()),  # levels that differ at confirmation time (gap only)
        ("reason", pa.string()),
    ]
)

DERIVED: dict[str, pa.Schema] = {
    "tob": TOB,
    "depth_1s": DEPTH_1S,
    "events_labeled": EVENTS_LABELED,
    "clean_windows": CLEAN_WINDOWS,
    "rest_checks": REST_CHECKS,
}

_EVENT_COLUMNS = ["ts_recv_ns", "ts_exch_ms", "asset_id", "event_type", "snapshot_id", "side", "price", "size",
                  "hash", "conn_id"]


# ---- configuration ---------------------------------------------------------------


@dataclass
class ReplayConfig:
    data_dir: Path
    out_dir: Path | None = None  # None: no derived tables written (validation only)
    t0: int = 0  # ns, inclusive
    t1: int = 0  # ns, exclusive
    assets: set[str] | None = None  # None = all
    chunk_s: int = 600  # time chunk read into memory at once
    match_window_s: float = 1.0  # REST vs stream tolerance (+-)
    label_window_s: float = 1.0  # delta vs trade tolerance (+-)
    grid_max_idle_s: float = 900.0  # stop depth grid rows this long after an asset's last event
    tables: tuple[str, ...] = ("tob", "depth_1s", "events_labeled", "clean_windows", "rest_checks")
    progress: bool = True


# ---- market index -------------------------------------------------------------------


@dataclass
class MarketIndex:
    cond_of: dict[str, str] = field(default_factory=dict)  # asset -> condition_id
    outcome_index: dict[str, int] = field(default_factory=dict)  # asset -> 0 | 1
    family: dict[str, str] = field(default_factory=dict)
    tokens: dict[str, list[str]] = field(default_factory=dict)  # condition_id -> [outcome0, outcome1]

    def sibling(self, asset_id: str) -> str | None:
        toks = self.tokens.get(self.cond_of.get(asset_id, ""), [])
        for t in toks:
            if t != asset_id:
                return t
        return None

    def add(self, asset_id: str, cond: str, oi: int, family: str) -> None:
        self.cond_of[asset_id] = cond
        self.outcome_index[asset_id] = oi
        self.family[asset_id] = family
        toks = self.tokens.setdefault(cond, [])
        if asset_id not in toks:
            toks.append(asset_id)
            toks.sort(key=lambda a: self.outcome_index[a])


def load_market_index(data_dir: Path) -> MarketIndex:
    files = sorted(glob.glob(str(data_dir / "reference" / "markets" / "**" / "*.parquet"), recursive=True))
    idx = MarketIndex()
    if not files:
        return idx
    # Why diagonal concat: `outcome_index` was added on 2026-11-07; older files lack it.
    cols = ["asset_id", "condition_id", "family", "outcome", "outcome_index"]
    df = pl.concat([pl.read_parquet(f).select([c for c in cols if c in pl.read_parquet_schema(f)]) for f in files],
                   how="diagonal_relaxed")
    if "outcome_index" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Int32).alias("outcome_index"))
    # Why: `outcome_index` exists only from 2026-11-07 (A1). Before that, Yes/Up is
    # outcome 0 by Polymarket convention (clobTokenIds order), No/Down is 1.
    df = df.with_columns(
        pl.when(pl.col("outcome_index").is_not_null()).then(pl.col("outcome_index"))
        .when(pl.col("outcome").is_in(["Yes", "Up"])).then(0).otherwise(1).cast(pl.Int32).alias("oi")
    )
    agg = df.group_by("asset_id").agg(pl.col("condition_id").first(), pl.col("family").first(), pl.col("oi").max())
    for aid, cond, fam, oi in agg.iter_rows():
        idx.add(aid, cond, int(oi), fam)
    return idx


# ---- file selection & reading -------------------------------------------------------------


class FileSpan(NamedTuple):
    path: str
    ts_min: int
    ts_max: int
    n_rows: int


def _dates_covering(t0: int, t1: int) -> list[str]:
    d = datetime.fromtimestamp(t0 / 1e9, tz=timezone.utc).date() - timedelta(days=1)
    end = datetime.fromtimestamp(max(t1 - 1, t0) / 1e9, tz=timezone.utc).date() + timedelta(days=1)
    out = []
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def scan_files(data_dir: Path, table: str, t0: int, t1: int, layer: str = "events") -> list[FileSpan]:
    """Files of `table` holding any row with t0 <= ts_recv_ns < t1, in write order.

    Why Parquet statistics instead of the dt=/hour= partition: the partition is
    the hour of a flush's first row; after a laptop sleep a file in hour=00 held
    rows from 01:29. Reading min/max from the footer costs ~0.4 ms per file."""
    spans: list[FileSpan] = []
    for date in _dates_covering(t0, t1):
        base = data_dir / layer / table / f"dt={date}"
        if not base.exists():
            continue
        # sorted(): hour=HH then part-HHMMSS-seq => the recorder's write order.
        for p in sorted(glob.glob(str(base / "hour=*" / "*.parquet"))):
            md = pq.read_metadata(p)
            ci = md.schema.to_arrow_schema().get_field_index("ts_recv_ns")
            lo = hi = None
            for i in range(md.num_row_groups):
                st = md.row_group(i).column(ci).statistics
                if st is None or not st.has_min_max:
                    lo, hi = 0, 2**62
                    break
                lo = st.min if lo is None else min(lo, st.min)
                hi = st.max if hi is None else max(hi, st.max)
            if lo is None:
                continue
            if hi >= t0 and lo < t1:
                spans.append(FileSpan(p, lo, hi, md.num_rows))
    return spans


def read_events(spans: list[FileSpan], t0: int, t1: int, assets: set[str] | None = None,
                columns: list[str] | None = None) -> pl.DataFrame:
    """Rows with t0 <= ts < t1 from `spans`, in replay order.

    Why a stable sort on ts_recv_ns only: rows of one frame share ts_recv_ns and
    their file order is the message order (bids before asks in a snapshot, the
    price_changes list order in a delta), which must survive."""
    columns = columns or _EVENT_COLUMNS
    if not spans:
        return pl.DataFrame(schema={c: pl.Null for c in columns})
    parts = []
    for s in spans:
        df = pl.read_parquet(s.path, columns=columns, missing_columns="insert")
        if s.ts_min < t0 or s.ts_max >= t1:
            df = df.filter((pl.col("ts_recv_ns") >= t0) & (pl.col("ts_recv_ns") < t1))
        if assets is not None:
            df = df.filter(pl.col("asset_id").is_in(list(assets)) | (pl.col("asset_id") == ""))
        parts.append(df)
    df = pl.concat(parts, how="vertical_relaxed")
    return df.sort("ts_recv_ns", maintain_order=True)


# ---- per-asset replay state -----------------------------------------------------------------


class _PendingRest:
    __slots__ = ("ts", "bids", "asks", "hash", "fp", "deadline", "n_levels")

    def __init__(self, ts, bids, asks, hash_, fp, deadline):
        self.ts, self.bids, self.asks, self.hash, self.fp, self.deadline = ts, bids, asks, hash_, fp, deadline
        self.n_levels = len(bids) + len(asks)


def _fingerprint(bids: dict, asks: dict) -> int:
    """Order-independent fingerprint of the level set: XOR of per-level hashes.

    Why XOR: a delta then costs O(1) (remove the old term, add the new one) and a
    book that returns to an earlier state returns to the same value, so one
    integer per event is enough to ask "did the stream ever pass through the
    REST book's state?". Float hashes are deterministic across processes; the
    side is an int so no str-hash randomisation is involved."""
    fp = 0
    for p, s in bids.items():
        fp ^= hash((0, p, s))
    for p, s in asks.items():
        fp ^= hash((1, p, s))
    return fp


class _Asset:
    __slots__ = (
        "asset_id", "family", "emit", "book", "fp", "initialised", "stale", "best_bid", "best_ask",
        "history", "recent", "pending", "win_start", "win_kind", "win_events", "last_verified",
        "next_grid", "last_event_ts", "last_reconnect", "windows", "n_events", "ws_exact", "ws_before", "ws_mismatch",
    )

    def __init__(self, asset_id: str, family: str, emit: bool):
        self.asset_id = asset_id
        self.family = family
        self.emit = emit
        self.book = BookState(asset_id)
        self.fp = 0
        self.initialised = False
        self.stale = False
        self.best_bid: float | None = None
        self.best_ask: float | None = None
        self.history: deque = deque()  # (ts, hash, fp) after each event, last match_window
        self.recent: deque = deque()  # (ts, side, price, size, hash, ts_exch) deltas, last 2*match_window
        self.pending: list[_PendingRest] = []
        self.win_start: int | None = None
        self.win_kind = ""
        self.win_events = 0
        self.last_verified: int | None = None
        self.next_grid: int | None = None
        self.last_event_ts = 0
        self.last_reconnect = 0
        self.windows: list[tuple] = []
        self.n_events = 0
        self.ws_exact = self.ws_before = self.ws_mismatch = 0

    # -- windows ---------------------------------------------------------------------

    def open_window(self, ts: int, kind: str) -> None:
        self.win_start, self.win_kind, self.win_events, self.last_verified = ts, kind, 0, None

    def close_window(self, ts: int, reason: str) -> None:
        if self.win_start is None:
            return
        self.windows.append((self.asset_id, self.win_start, ts, self.win_kind, reason, self.last_verified, self.win_events))
        self.win_start = None

    @property
    def valid(self) -> bool:
        b, a = self.best_bid, self.best_ask
        return self.initialised and not self.stale and b is not None and a is not None and b < a


class BookAt(NamedTuple):
    ts_recv_ns: int
    book: BookState
    valid: bool
    event_type: str


# ---- the engine -----------------------------------------------------------------------------


class Replayer:
    """Feeds stored events through BookState objects, in order, chunk by chunk."""

    def __init__(self, cfg: ReplayConfig, index: MarketIndex | None = None):
        self.cfg = cfg
        self.index = index if index is not None else load_market_index(cfg.data_dir)
        self.assets: dict[str, _Asset] = {}
        self.W = int(cfg.match_window_s * NS)
        self.LW = int(cfg.label_window_s * NS)
        self.grid_idle = int(cfg.grid_max_idle_s * NS)
        self.want = {t: (t in cfg.tables) and cfg.out_dir is not None for t in DERIVED}
        self.want["rest_checks"] = "rest_checks" in cfg.tables  # kept in memory for the report even without out_dir
        self._reset_buffers()
        self.rest_checks_all: list[tuple] = []
        self.ws_mismatches: list[tuple] = []  # (ts, asset_id, family, n_ws_levels, n_diff)
        self.stats = {"rows": 0, "deltas": 0, "snapshots_ws": 0, "snapshots_rest": 0, "reconnects": 0,
                      "mirrored_deltas": 0, "resyncs": 0, "loop_s": 0.0, "read_s": 0.0, "write_s": 0.0}
        self._seq = 0
        self._on_event = None  # callback(_Asset, ts, event_type) for iter_books

    def _reset_buffers(self) -> None:
        self.buf: dict[str, list] = {t: [] for t in DERIVED}

    # -- state helpers ----------------------------------------------------------------

    def _asset(self, aid: str) -> _Asset:
        a = self.assets.get(aid)
        if a is None:
            emit = self.cfg.assets is None or aid in self.cfg.assets
            a = self.assets[aid] = _Asset(aid, self.index.family.get(aid, "unknown"), emit)
        return a

    def _emit_tob(self, a: _Asset, ts: int, et: str) -> None:
        if not (a.emit and self.want["tob"]):
            return
        bb, ba = a.best_bid, a.best_ask
        if bb is None or ba is None:
            qb = a.book.bids[bb] if bb is not None else None
            qa = a.book.asks[ba] if ba is not None else None
            self.buf["tob"].append((ts, a.asset_id, et, bb, qb, ba, qa, None, None, None, False))
            return
        qb, qa = a.book.bids[bb], a.book.asks[ba]
        q = qb + qa
        micro = (ba * qb + bb * qa) / q if q else None
        self.buf["tob"].append((ts, a.asset_id, et, bb, qb, ba, qa, (ba + bb) / 2, micro, ba - bb,
                                a.initialised and not a.stale and bb < ba))

    def _grid_row(self, a: _Asset, t: int) -> None:
        n = DEPTH_LEVELS
        bids, asks = a.book.bids, a.book.asks
        bp = sorted(bids, reverse=True)[:n]
        ap = sorted(asks)[:n]
        row = [t, a.asset_id, a.valid, len(bids), len(asks)]
        for px, book in ((bp, bids), (ap, asks)):
            row.extend(px + [None] * (n - len(px)))
            c = 0.0
            cum = []
            for p in px:
                c += book[p]
                cum.append(c)
            row.extend(cum + [None] * (n - len(px)))
        self.buf["depth_1s"].append(tuple(row))

    def _grid_catch_up(self, a: _Asset, ts: int) -> None:
        """Emit depth rows for every whole second < ts (state as of that second:
        events with ts_recv_ns <= second included). Stops `grid_max_idle_s` after
        the asset's last event: a token that is unsubscribed (15-minute BTC
        windows expire) must not produce an empty-book row per second forever."""
        g = a.next_grid
        if g is None:
            return
        stop = min(ts, a.last_event_ts + self.grid_idle + 1)
        while g < stop:
            self._grid_row(a, g)
            g += NS
        if ts - a.last_event_ts > self.grid_idle:
            g = max(g, -(-ts // NS) * NS)  # idle gap: resume at the next whole second
        a.next_grid = g

    # -- applying events ------------------------------------------------------------------

    def _apply_delta(self, a: _Asset, ts: int, side: str, price: float, size: float, hash_: str,
                     ts_exch: int, conn: int, mirrored: bool) -> None:
        book = a.book
        d = book.bids if side == "BUY" else book.asks
        old = d.get(price)
        book.apply_delta(side, price, size, hash_, ts_exch)
        si = 0 if side == "BUY" else 1
        if old is not None:
            a.fp ^= hash((si, price, old))
        if size > 0:
            a.fp ^= hash((si, price, size))
        # cached top of book: O(1) except when the best level itself disappears
        if side == "BUY":
            if size > 0:
                if a.best_bid is None or price > a.best_bid:
                    a.best_bid = price
            elif price == a.best_bid:
                a.best_bid = max(book.bids) if book.bids else None
        else:
            if size > 0:
                if a.best_ask is None or price < a.best_ask:
                    a.best_ask = price
            elif price == a.best_ask:
                a.best_ask = min(book.asks) if book.asks else None
        a.history.append((ts, hash_, a.fp))
        a.recent.append((ts, side, price, size, hash_, ts_exch))
        a.win_events += 1
        if a.emit and self.want["events_labeled"]:
            self.buf["events_labeled"].append((ts, a.asset_id, side, price, old or 0.0, size, mirrored, conn))

    def _apply_snapshot(self, a: _Asset, ts: int, kind: str, bids: list, asks: list, hash_: str,
                        ts_exch: int) -> None:
        a.book.apply_snapshot(bids, asks, hash_, ts_exch)
        a.fp = _fingerprint(a.book.bids, a.book.asks)
        a.best_bid = max(a.book.bids) if a.book.bids else None
        a.best_ask = min(a.book.asks) if a.book.asks else None
        a.history.append((ts, hash_, a.fp))
        if not a.initialised or a.stale:
            a.initialised = True
            a.stale = False
            a.open_window(ts, kind)
            if a.next_grid is None:
                a.next_grid = -(-ts // NS) * NS
        a.win_events += 1

    def _trim(self, a: _Asset, ts: int) -> None:
        h, lim = a.history, ts - self.W
        while h and h[0][0] < lim:
            h.popleft()
        r, lim2 = a.recent, ts - 2 * self.W
        while r and r[0][0] < lim2:
            r.popleft()

    def _matches_before(self, a: _Asset, ts: int, hash_: str, fp: int) -> int | None:
        """ts of the most recent stream state within the window that equals the
        given book (by server hash or by our fingerprint), else None."""
        lim = ts - self.W
        for hts, hh, hf in reversed(a.history):
            if hts < lim:
                break
            if hf == fp or (hash_ and hh == hash_):
                return hts
        return None

    def _rest_snapshot(self, a: _Asset, ts: int, bids: list, asks: list, hash_: str, ts_exch: int) -> None:
        rb = {p: s for p, s in bids if s > 0}
        ra = {p: s for p, s in asks if s > 0}
        n = len(rb) + len(ra)
        in_window = a.initialised and not a.stale
        if not in_window:
            # Why: a REST book is a full book; after a reconnect (or before the
            # first WS snapshot) it is as good a starting point as a WS snapshot.
            self._apply_snapshot(a, ts, "snapshot_rest", bids, asks, hash_, ts_exch)
            self._record_check(a, ts, False, True, "init", 0, n, 0, "book not initialised or stale: used as anchor")
            return
        if a.book.bids == rb and a.book.asks == ra:
            self._record_check(a, ts, True, True, "exact", 0, n, 0, "")
            a.last_verified = ts
            return
        fp = _fingerprint(rb, ra)
        mts = self._matches_before(a, ts, hash_, fp)
        if mts is not None:
            self._record_check(a, ts, True, True, "before", mts - ts, n, 0, "")
            a.last_verified = ts
            return
        # Not seen yet: REST may be ahead of the stream. Decide after match_window.
        a.pending.append(_PendingRest(ts, rb, ra, hash_, fp, ts + self.W))

    def _check_pending_after(self, a: _Asset, ts: int, hash_: str) -> None:
        keep = []
        for p in a.pending:
            if a.fp == p.fp or (p.hash and hash_ == p.hash):
                self._record_check(a, p.ts, True, True, "after", ts - p.ts, p.n_levels, 0, "")
                a.last_verified = p.ts
            else:
                keep.append(p)
        a.pending = keep

    def _expire_pending(self, a: _Asset, ts: int) -> None:
        """Called before applying an event at `ts` (and at chunk end): a REST book
        that matched no stream state within the window is a confirmed gap."""
        keep = []
        for p in a.pending:
            if ts <= p.deadline:
                keep.append(p)
                continue
            self._confirm_gap(a, p, ts)
        a.pending = keep

    def _confirm_gap(self, a: _Asset, p: _PendingRest, now: int) -> None:
        bids, asks = a.book.bids, a.book.asks
        ob = sum(1 for k in p.bids if k not in bids) ; oo = sum(1 for k in bids if k not in p.bids)
        sb = sum(1 for k, v in p.bids.items() if k in bids and bids[k] != v)
        oa = sum(1 for k in p.asks if k not in asks) ; ao = sum(1 for k in asks if k not in p.asks)
        sa = sum(1 for k, v in p.asks.items() if k in asks and asks[k] != v)
        n_diff = ob + oo + sb + oa + ao + sa
        reason = (f"bids: {ob} only REST/{oo} only ours/{sb} size; asks: {oa} only REST/{ao} only ours/{sa} size"
                  f" (vs book at +{(now - p.ts) / 1e6:.0f}ms)")
        if not bids and not asks:
            reason = "our book empty; " + reason
        self._record_check(a, p.ts, True, False, "gap", 0, p.n_levels, n_diff, reason)
        a.close_window(p.ts, "rest_gap")
        # Resync: REST book, then re-apply the deltas since just before the REST
        # read. Why re-apply: deltas are absolute level sizes, so applying one the
        # REST book already reflects is harmless, and applying the ones it does not
        # yet reflect brings us to the present. Covers REST both behind and ahead.
        self._apply_snapshot(a, now, "rest_resync", list(p.bids.items()), list(p.asks.items()), p.hash, 0)
        lim = p.ts - self.W
        for rts, side, price, size, h, tx in list(a.recent):
            if rts >= lim:
                book = a.book
                d = book.bids if side == "BUY" else book.asks
                old = d.get(price)
                book.apply_delta(side, price, size, h, tx)
                si = 0 if side == "BUY" else 1
                if old is not None:
                    a.fp ^= hash((si, price, old))
                if size > 0:
                    a.fp ^= hash((si, price, size))
        a.best_bid = max(a.book.bids) if a.book.bids else None
        a.best_ask = min(a.book.asks) if a.book.asks else None
        a.history.clear()
        a.history.append((now, a.book.last_hash, a.fp))
        a.open_window(now, "rest_resync")
        self.stats["resyncs"] += 1
        self._emit_tob(a, now, "resync")

    def _record_check(self, a: _Asset, ts: int, in_window: bool, matched: bool, kind: str, lag: int,
                      n_levels: int, n_diff: int, reason: str) -> None:
        row = (ts, a.asset_id, a.family, in_window, matched, kind, lag, n_levels, n_diff, reason)
        self.rest_checks_all.append(row)
        if a.emit and self.want["rest_checks"] and self.cfg.out_dir is not None:
            self.buf["rest_checks"].append(row)

    def _reconnect(self, a: _Asset, ts: int) -> None:
        """Mark one book stale from `ts` until its next snapshot. Idempotent per
        (asset, reconnect): each market's loop and `_finish_chunk` may both see it."""
        if ts <= a.last_reconnect:
            return
        a.last_reconnect = ts
        if a.initialised and not a.stale:
            self._grid_catch_up(a, ts)
            a.stale = True
            a.close_window(ts, "reconnect")
            for p in a.pending:  # cannot be judged any more
                self._record_check(a, p.ts, True, False, "gap", 0, p.n_levels, -1, "reconnect before the window closed")
            a.pending = []
            self._emit_tob(a, ts, "reconnect")

    # -- the per-market loop ------------------------------------------------------------

    def _process_market(self, cond: str, df: pl.DataFrame, reconnects: list[int]) -> None:
        toks = self.index.tokens.get(cond)
        sib_of: dict[str, str | None] = {}
        if toks and len(toks) == 2:
            sib_of = {toks[0]: toks[1], toks[1]: toks[0]}
        # Mirror decision per frame (same ts_recv_ns): outcome-0 deltas with no
        # outcome-1 rows in the frame are applied to the sibling as well.
        if toks and len(toks) == 2:
            is_delta = pl.col("event_type") == "delta"
            o1 = (is_delta & (pl.col("asset_id") == toks[1])).max().over("ts_recv_ns")
            df = df.with_columns((is_delta & (pl.col("asset_id") == toks[0]) & ~o1).alias("mirror"))
        else:
            df = df.with_columns(pl.lit(False).alias("mirror"))

        ts_l = df["ts_recv_ns"].to_list()
        et_l = df["event_type"].to_list()
        aid_l = df["asset_id"].to_list()
        sid_l = df["snapshot_id"].to_list()
        side_l = df["side"].to_list()
        px_l = df["price"].to_list()
        sz_l = df["size"].to_list()
        h_l = df["hash"].to_list()
        tx_l = df["ts_exch_ms"].to_list()
        cn_l = df["conn_id"].to_list()
        mir_l = df["mirror"].to_list()

        assets = self.assets
        get_asset = self._asset
        market_assets = [get_asset(t) for t in (toks or sorted(set(aid_l)))]
        apply_delta, emit_tob, grid_catch_up, trim = self._apply_delta, self._emit_tob, self._grid_catch_up, self._trim
        on_event = self._on_event
        n_rec = len(reconnects)
        ri = 0
        # snapshot accumulator
        snap_key = None
        snap_bids: list = []
        snap_asks: list = []
        snap_meta = None  # (asset, ts, kind, hash, ts_exch)

        def flush_snapshot():
            a, ts, kind, h, tx = snap_meta
            self._pre_event(a, ts)
            if kind == "snapshot_ws":
                self.stats["snapshots_ws"] += 1
                self._ws_snapshot(a, ts, snap_bids, snap_asks, h, tx)
            else:
                self.stats["snapshots_rest"] += 1
                self._rest_snapshot(a, ts, snap_bids, snap_asks, h, tx)
            a.last_event_ts = ts
            a.n_events += 1
            trim(a, ts)
            if a.pending:
                self._check_pending_after(a, ts, h)
            emit_tob(a, ts, kind)
            if on_event is not None:
                on_event(a, ts, kind)

        W2 = 2 * self.W
        for i in range(len(ts_l)):
            ts = ts_l[i]
            et = et_l[i]
            aid = aid_l[i]
            # reconnect markers (asset_id "") apply to every book
            while ri < n_rec and reconnects[ri] <= ts:
                if snap_key is not None:
                    flush_snapshot()
                    snap_key = None
                for ma in market_assets:
                    self._reconnect(ma, reconnects[ri])
                ri += 1
            if et == "delta":
                if snap_key is not None:
                    flush_snapshot()
                    snap_key = None
                a = assets.get(aid) or get_asset(aid)
                side, price, size, h, tx, cn = side_l[i], px_l[i], sz_l[i], h_l[i], tx_l[i], cn_l[i]
                self._pre_event(a, ts)
                apply_delta(a, ts, side, price, size, h, tx, cn, False)
                a.last_event_ts = ts
                a.n_events += 1
                if a.history[0][0] < ts - self.W:
                    trim(a, ts)
                if a.pending:
                    self._check_pending_after(a, ts, h)
                emit_tob(a, ts, et)
                if on_event is not None:
                    on_event(a, ts, et)
                if mir_l[i]:
                    sa = assets.get(sib_of[aid]) or get_asset(sib_of[aid])
                    self._pre_event(sa, ts)
                    apply_delta(sa, ts, _OPP[side], flip_price(price), size, "", tx, cn, True)
                    sa.last_event_ts = ts
                    sa.n_events += 1
                    self.stats["mirrored_deltas"] += 1
                    if sa.history[0][0] < ts - self.W:
                        trim(sa, ts)
                    if sa.pending:
                        self._check_pending_after(sa, ts, "")
                    emit_tob(sa, ts, et)
                    if on_event is not None:
                        on_event(sa, ts, et)
            elif et == "snapshot_ws" or et == "snapshot_rest":
                key = (aid, sid_l[i])
                if key != snap_key:
                    if snap_key is not None:
                        flush_snapshot()
                    snap_key = key
                    snap_bids, snap_asks = [], []
                    snap_meta = (assets.get(aid) or get_asset(aid), ts, et, h_l[i], tx_l[i])
                side = side_l[i]
                if side == "BUY":
                    snap_bids.append((px_l[i], sz_l[i]))
                elif side == "SELL":
                    snap_asks.append((px_l[i], sz_l[i]))
                # side null = empty book snapshot: nothing to add
            # 'reconnect' rows are handled through `reconnects`; anything else is ignored
        if snap_key is not None:
            flush_snapshot()
        for r in reconnects[ri:]:
            for ma in market_assets:
                self._reconnect(ma, r)
        self.stats["rows"] += len(ts_l)
        self.stats["deltas"] += et_l.count("delta")

    def _pre_event(self, a: _Asset, ts: int) -> None:
        if a.pending:
            self._expire_pending(a, ts)
        if a.next_grid is not None and a.next_grid < ts:
            self._grid_catch_up(a, ts)

    def _ws_snapshot(self, a: _Asset, ts: int, bids: list, asks: list, hash_: str, ts_exch: int) -> None:
        if a.initialised and not a.stale:
            # Secondary validation: a WS snapshot should equal the replayed book exactly.
            sb = {p: s for p, s in bids if s > 0}
            sa = {p: s for p, s in asks if s > 0}
            if a.book.bids == sb and a.book.asks == sa:
                a.ws_exact += 1
            elif self._matches_before(a, ts, hash_, _fingerprint(sb, sa)) is not None:
                a.ws_before += 1
            else:
                a.ws_mismatch += 1
                nd = sum(1 for k, v in sb.items() if a.book.bids.get(k) != v) + sum(1 for k in a.book.bids if k not in sb) \
                    + sum(1 for k, v in sa.items() if a.book.asks.get(k) != v) + sum(1 for k in a.book.asks if k not in sa)
                self.ws_mismatches.append((ts, a.asset_id, a.family, len(sb) + len(sa), nd))
        self._apply_snapshot(a, ts, "snapshot_ws", bids, asks, hash_, ts_exch)

    # -- chunk driver ------------------------------------------------------------------------

    def _chunk_conditions(self, df: pl.DataFrame) -> tuple[dict[str, pl.DataFrame], list[int]]:
        """Split a sorted chunk into per-market frames (both tokens together) and
        the list of reconnect instants."""
        rec = df.filter(pl.col("event_type") == "reconnect")["ts_recv_ns"].to_list()
        df = df.filter(pl.col("event_type") != "reconnect")
        cond_map = self.index.cond_of
        df = df.with_columns(pl.col("asset_id").replace_strict(cond_map, default=pl.col("asset_id"), return_dtype=pl.String).alias("cond"))
        parts = df.partition_by("cond", maintain_order=True, as_dict=True, include_key=False)
        return {k[0]: v for k, v in parts.items()}, rec

    def _finish_chunk(self, t_end: int, reconnects: list[int]) -> None:
        """Reconnects for assets without rows in this chunk, expired REST checks,
        and the depth grid up to the chunk end."""
        self.stats["reconnects"] += len(reconnects)
        for a in self.assets.values():
            for r in reconnects:
                self._reconnect(a, r)
            if a.pending:
                self._expire_pending(a, t_end)
            self._grid_catch_up(a, t_end)

    def run(self) -> dict:
        cfg = self.cfg
        t0, t1 = cfg.t0, cfg.t1
        assert t1 > t0, "t1 must be after t0"
        spans = scan_files(cfg.data_dir, "book_events", t0, t1)
        tspans = scan_files(cfg.data_dir, "trades", t0 - self.LW, t1 + self.LW) if self.want["events_labeled"] else []
        asset_filter = None
        if cfg.assets is not None:
            asset_filter = set()
            for aid in cfg.assets:
                asset_filter.add(aid)
                sib = self.index.sibling(aid)
                if sib:
                    asset_filter.add(sib)
        step = cfg.chunk_s * NS
        wall0 = time.perf_counter()
        c0 = t0
        n_chunks = math.ceil((t1 - t0) / step)
        k = 0
        while c0 < t1:
            c1 = min(c0 + step, t1)
            k += 1
            r0 = time.perf_counter()
            df = read_events([s for s in spans if s.ts_max >= c0 and s.ts_min < c1], c0, c1, asset_filter)
            self.stats["read_s"] += time.perf_counter() - r0
            l0 = time.perf_counter()
            if df.height:
                parts, rec = self._chunk_conditions(df)
                del df
                for cond, pdf in parts.items():
                    self._process_market(cond, pdf, rec)
            else:
                rec = []
            self._finish_chunk(c1, rec)
            self.stats["loop_s"] += time.perf_counter() - l0
            w0 = time.perf_counter()
            if self.want["events_labeled"]:
                self._label_events(tspans, c0, c1)
            self._write_chunk(c0)
            self.stats["write_s"] += time.perf_counter() - w0
            if cfg.progress:
                el = time.perf_counter() - wall0
                log.info("chunk %d/%d %s rows=%d (%.0f s elapsed, %.0f k rows/s)", k, n_chunks,
                         datetime.fromtimestamp(c0 / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                         self.stats["rows"], el, self.stats["rows"] / el / 1e3 if el else 0)
            c0 = c1
        for a in self.assets.values():
            a.close_window(t1, "end")
        if self.want["clean_windows"]:
            self.buf["clean_windows"].extend(w for a in self.assets.values() if a.emit for w in a.windows)
            self._write_chunk(t0, only=("clean_windows",))
        self.stats["wall_s"] = time.perf_counter() - wall0
        self.stats["ws_exact"] = sum(a.ws_exact for a in self.assets.values())
        self.stats["ws_before"] = sum(a.ws_before for a in self.assets.values())
        self.stats["ws_mismatch"] = sum(a.ws_mismatch for a in self.assets.values())
        return self.stats

    # -- labelling ---------------------------------------------------------------------------

    def _label_events(self, tspans: list[FileSpan], c0: int, c1: int) -> None:
        rows = self.buf["events_labeled"]
        self.buf["events_labeled"] = []
        if not rows:
            return
        ev = pl.DataFrame(
            {k: v for k, v in zip(("ts_recv_ns", "asset_id", "side", "price", "size_before", "size_after", "mirrored", "conn_id"), zip(*rows))},
            schema={"ts_recv_ns": pl.Int64, "asset_id": pl.String, "side": pl.String, "price": pl.Float64,
                    "size_before": pl.Float64, "size_after": pl.Float64, "mirrored": pl.Boolean, "conn_id": pl.Int32},
        ).with_row_index("_i")
        ev = ev.with_columns((pl.col("size_after") - pl.col("size_before")).alias("size_change"),
                             (pl.col("price") * 1e6).round(0).cast(pl.Int64).alias("_px"))
        tr = read_events([s for s in tspans if s.ts_max >= c0 - self.LW and s.ts_min < c1 + self.LW],
                         c0 - self.LW, c1 + self.LW, None, ["ts_recv_ns", "asset_id", "price", "size", "side"])
        if tr.height:
            # Why both own and mirrored trades: a trade is stored once, on the token the
            # taker traded. A taker BUY on YES at p consumes the YES ask at p, which is
            # the NO bid at 1-p: the sibling's delta needs the mirrored trade to be
            # recognised as a fill.
            sib = {a: s for a in self.index.cond_of for s in [self.index.sibling(a)] if s}
            own = tr.with_columns(
                pl.when(pl.col("side") == "BUY").then(pl.lit("SELL")).otherwise(pl.lit("BUY")).alias("side"))
            mir = tr.filter(pl.col("asset_id").is_in(list(sib))).with_columns(
                pl.col("asset_id").replace_strict(sib, return_dtype=pl.String),
                (1.0 - pl.col("price")).round(9).alias("price"),
                pl.when(pl.col("side") == "BUY").then(pl.lit("BUY")).otherwise(pl.lit("SELL")).alias("side"))
            trades = pl.concat([own, mir]).with_columns((pl.col("price") * 1e6).round(0).cast(pl.Int64).alias("_px")) \
                .rename({"ts_recv_ns": "trade_ts_recv_ns", "size": "trade_size"}).select("trade_ts_recv_ns", "asset_id", "side", "_px", "trade_size") \
                .sort("trade_ts_recv_ns")
            down = ev.filter(pl.col("size_change") < 0).sort("ts_recv_ns")
            j = down.join_asof(trades, left_on="ts_recv_ns", right_on="trade_ts_recv_ns", by=["asset_id", "side", "_px"],
                               strategy="nearest", tolerance=self.LW, check_sortedness=False).select("_i", "trade_ts_recv_ns", "trade_size")
            ev = ev.join(j, on="_i", how="left")
        else:
            ev = ev.with_columns(pl.lit(None, dtype=pl.Int64).alias("trade_ts_recv_ns"), pl.lit(None, dtype=pl.Float64).alias("trade_size"))
        ev = ev.with_columns(
            pl.when(pl.col("size_change") > 0).then(pl.lit("add"))
            .when(pl.col("size_change") == 0).then(pl.lit("unchanged"))
            .when(pl.col("trade_ts_recv_ns").is_not_null()).then(pl.lit("fill"))
            .otherwise(pl.lit("cancel")).alias("label"),
            (pl.col("ts_recv_ns") - pl.col("trade_ts_recv_ns")).alias("trade_lag_ns"),
        ).sort("_i").select([f.name for f in EVENTS_LABELED])
        self._labeled_df = ev

    # -- writing --------------------------------------------------------------------------------

    def _write_chunk(self, t_chunk: int, only: tuple[str, ...] | None = None) -> None:
        out = self.cfg.out_dir
        if out is None:
            self._reset_buffers()
            return
        d = datetime.fromtimestamp(t_chunk / 1e9, tz=timezone.utc)
        for table, schema in DERIVED.items():
            if only is not None and table not in only:
                continue
            if not self.want[table]:
                continue
            if table == "events_labeled":
                df = getattr(self, "_labeled_df", None)
                self._labeled_df = None
                if df is None or df.height == 0:
                    continue
                tbl = df.to_arrow().cast(schema)
            else:
                rows = self.buf[table]
                if not rows:
                    continue
                cols = list(zip(*rows))
                tbl = pa.Table.from_arrays([pa.array(c, type=f.type) for c, f in zip(cols, schema)], schema=schema)
            self.buf[table] = []
            dest = out / "derived" / table / f"dt={d:%Y-%m-%d}" / f"hour={d:%H}"
            dest.mkdir(parents=True, exist_ok=True)
            self._seq += 1
            final = dest / f"part-{d:%H%M%S}-{self._seq:05d}.parquet"
            tmp = final.with_suffix(".parquet.tmp")
            pq.write_table(tbl, tmp, compression="zstd")
            os.replace(tmp, final)
        self._reset_buffers()


# ---- public API ----------------------------------------------------------------------------------


def _find_anchor(data_dir: Path, asset_id: str, t: int, max_lookback_s: int = 6 * 3600) -> int | None:
    """ts_recv_ns of the latest snapshot (WS or REST) for `asset_id` at or before t."""
    back = 600
    while back <= max_lookback_s:
        lo = t - back * NS
        spans = scan_files(data_dir, "book_events", lo, t + 1)
        if spans:
            df = read_events(spans, lo, t + 1, {asset_id}, ["ts_recv_ns", "asset_id", "event_type"])
            snaps = df.filter(pl.col("event_type").is_in(list(SNAPSHOT_TYPES)) & (pl.col("asset_id") == asset_id))
            if snaps.height:
                return int(snaps["ts_recv_ns"].max())
        back *= 4
    return None


def iter_books(data_dir: Path, asset_id: str, t0: int, t1: int, index: MarketIndex | None = None,
               chunk_s: int = 600) -> Iterator[BookAt]:
    """Yield (ts_recv_ns, BookState, valid, event_type) after every event of
    `asset_id` with t0 <= ts < t1. Replay starts at the last snapshot before t0,
    so the first yielded book is already complete (if one exists within 6 h).

    The yielded BookState is the live object: copy `bids`/`asks` to keep them."""
    index = index or load_market_index(data_dir)
    anchor = _find_anchor(data_dir, asset_id, t0)
    start = anchor if anchor is not None else t0
    cfg = ReplayConfig(data_dir=data_dir, out_dir=None, t0=start, t1=t1, assets={asset_id}, chunk_s=chunk_s,
                       tables=(), progress=False)
    rp = Replayer(cfg, index)
    out: list[BookAt] = []

    def on_event(a: _Asset, ts: int, et: str) -> None:
        if a.asset_id == asset_id and ts >= t0:
            out.append(BookAt(ts, a.book, a.valid, et))

    rp._on_event = on_event
    # Why run chunk by chunk here: the engine is a batch driver; yielding from
    # inside its loop would need a thread. Collecting per chunk keeps memory
    # bounded to one chunk of events for one asset.
    step = chunk_s * NS
    c0 = start
    spans = scan_files(data_dir, "book_events", start, t1)
    asset_filter = {asset_id} | ({index.sibling(asset_id)} if index.sibling(asset_id) else set())
    while c0 < t1:
        c1 = min(c0 + step, t1)
        df = read_events([s for s in spans if s.ts_max >= c0 and s.ts_min < c1], c0, c1, asset_filter)
        if df.height:
            parts, rec = rp._chunk_conditions(df)
            for cond, pdf in parts.items():
                rp._process_market(cond, pdf, rec)
        else:
            rec = []
        rp._finish_chunk(c1, rec)
        for b in out:
            yield b
        out.clear()
        c0 = c1


def book_at(data_dir: Path, asset_id: str, t: int, index: MarketIndex | None = None) -> BookAt | None:
    """The book of `asset_id` as of t (events with ts_recv_ns <= t applied), or
    None if no snapshot precedes t within the lookback."""
    index = index or load_market_index(data_dir)
    anchor = _find_anchor(data_dir, asset_id, t)
    if anchor is None:
        return None
    last: BookAt | None = None
    for b in iter_books(data_dir, asset_id, anchor, t + 1, index):
        last = b
    return last


# ---- validation report ------------------------------------------------------------------------------


def validation_report(checks: list[tuple], stats: dict | None = None, mismatch_limit: int = 50,
                      ws_mismatches: list[tuple] | None = None) -> str:
    """Text report over rest_checks rows (tuples in REST_CHECKS column order)."""
    if not checks:
        return "no REST snapshots in the window\n"
    df = pl.DataFrame([list(r) for r in checks], schema=[f.name for f in REST_CHECKS], orient="row")
    inw = df.filter(pl.col("in_window"))
    lines = []
    n, m = inw.height, int(inw["matched"].sum())
    lines.append(f"REST snapshot checks: {df.height} total, {df.height - n} used as anchors (book not initialised / stale)")
    lines.append(f"in clean windows: {n} checks, {m} matched = {100 * m / n if n else 0:.3f} %")
    kinds = inw.group_by("kind").agg(pl.len().alias("n")).sort("kind")
    lines.append("  by kind: " + ", ".join(f"{k}={v}" for k, v in kinds.iter_rows()))
    lag = inw.filter(pl.col("kind").is_in(["before", "after"]))["lag_ns"]
    if lag.len():
        q = lag.quantile
        lines.append(f"  timing races: lag p5={q(0.05) / 1e6:.0f} ms p50={q(0.5) / 1e6:.0f} ms p95={q(0.95) / 1e6:.0f} ms "
                     f"(negative = REST behind the stream)")
    lines.append("\nper family:")
    for fam, nn, mm in inw.group_by("family").agg(pl.len(), pl.col("matched").sum()).sort("family").iter_rows():
        lines.append(f"  {fam:<18} {mm:>6}/{nn:<6} {100 * mm / nn:7.3f} %")
    lines.append("\nper hour (UTC):")
    hr = inw.with_columns((pl.col("ts_recv_ns") // (3600 * NS) * 3600).alias("h"))
    for h, nn, mm in hr.group_by("h").agg(pl.len(), pl.col("matched").sum()).sort("h").iter_rows():
        lines.append(f"  {datetime.fromtimestamp(h, tz=timezone.utc):%Y-%m-%d %H:00}  {mm:>6}/{nn:<6} {100 * mm / nn:7.3f} %")
    mis = inw.filter(~pl.col("matched")).sort("ts_recv_ns")
    lines.append(f"\nmismatches: {mis.height}")
    for ts, aid, fam, _, _, kind, lag_, nl, nd, reason in mis.head(mismatch_limit).iter_rows():
        lines.append(f"  {datetime.fromtimestamp(ts / 1e9, tz=timezone.utc):%H:%M:%S.%f} {fam:<16} {aid[:10]}… "
                     f"{nl:>3} REST levels, {nd:>3} differ: {reason}")
    if mis.height > mismatch_limit:
        lines.append(f"  … {mis.height - mismatch_limit} more")
    if stats:
        lines.append("\nstream self-check (WS snapshots vs replayed book): "
                     f"exact={stats.get('ws_exact', 0)} within-window={stats.get('ws_before', 0)} mismatch={stats.get('ws_mismatch', 0)}")
        for ts, aid, fam, nl, nd in (ws_mismatches or [])[:mismatch_limit]:
            lines.append(f"  ws mismatch {datetime.fromtimestamp(ts / 1e9, tz=timezone.utc):%H:%M:%S.%f} {fam:<16} {aid[:10]}… "
                         f"{nl} WS levels, {nd} differ")
        lines.append(f"rows={stats['rows']:,} mirrored_deltas={stats['mirrored_deltas']} reconnects={stats['reconnects']} "
                     f"resyncs={stats['resyncs']}  read={stats['read_s']:.0f}s loop={stats['loop_s']:.0f}s write={stats['write_s']:.0f}s "
                     f"wall={stats.get('wall_s', 0):.0f}s ({stats['rows'] / max(stats.get('wall_s', 1), 1) / 1e3:.0f} k rows/s)")
    return "\n".join(lines) + "\n"


def parse_ts(s: str) -> int:
    """'2026-11-06T13:10:00' / '2026-11-06 13:10' / integer ns -> ns since epoch (UTC)."""
    if s.isdigit():
        return int(s)
    s = s.replace(" ", "T")
    if len(s) == 10:
        s += "T00:00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * NS)

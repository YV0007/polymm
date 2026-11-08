"""MarketParser: raw frame -> typed rows + book state update.

One class knows the Polymarket market-channel message format. When they change
a field name, this is the only file that changes. Rows are tuples in the column
order of `records.py` schemas.

Message shapes (verified live 2026-11-05):
  book              {market, asset_id, timestamp, hash, bids[{price,size}], asks[...], tick_size, last_trade_price}
  price_change      {market, timestamp, price_changes:[{asset_id, price, size, side, hash, best_bid, best_ask}]}
  last_trade_price  {market, asset_id, price, size, side, fee_rate_bps, timestamp, transaction_hash}
  best_bid_ask      {market, asset_id, best_bid, best_ask, spread, timestamp}
  tick_size_change  {market, asset_id, old_tick_size, new_tick_size, timestamp}
  market_resolved   {market, assets_ids, winning_asset_id, winning_outcome, timestamp}
  new_market        (broadcast for every new market on the venue; raw only)
A frame may also be a JSON *list* of such messages.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field

import orjson

from .book import BookState, flip_price, mirror
from .sources import RECONNECT

log = logging.getLogger(__name__)


@dataclass(slots=True)
class ParserStats:
    msgs_total: int = 0
    msgs_book: int = 0
    msgs_delta: int = 0
    rows_delta: int = 0
    msgs_trade: int = 0
    msgs_tob: int = 0
    msgs_other: int = 0
    parse_errors: int = 0
    reconnects: int = 0
    mirror_dropped: int = 0
    mirror_kept: int = 0
    other_types: dict[str, int] = field(default_factory=dict)


_OPPOSITE = {"BUY": "SELL", "SELL": "BUY"}


def _f(x) -> float | None:
    try:
        return float(x) if x is not None and x != "" else None
    except (TypeError, ValueError):
        return None


def _i(x, default=0) -> int:
    try:
        return int(x)
    except (TypeError, ValueError):
        return default


class MarketParser:
    def __init__(
        self,
        books: dict[str, BookState],
        buffers: dict[str, list[tuple]],
        token_cond: dict[str, str],
        token_pair: dict[str, tuple[str, int]] | None = None,
    ):
        self.books = books
        self.buffers = buffers
        self.token_cond = token_cond  # asset_id -> condition_id (from UniverseWatcher)
        # asset_id -> (sibling, outcome index), from UniverseWatcher. Empty = store everything.
        self.token_pair = token_pair if token_pair is not None else {}
        self.stats = ParserStats()

    def _secondary(self, aid: str) -> str | None:
        """Sibling asset_id if `aid` is outcome 1 of a known pair, else None."""
        pair = self.token_pair.get(aid)
        return pair[0] if pair is not None and pair[1] == 1 else None

    def _book(self, asset_id: str) -> BookState:
        b = self.books.get(asset_id)
        if b is None:
            b = self.books[asset_id] = BookState(asset_id)
        return b

    # ---- entry -------------------------------------------------------------------

    def reconnect(self, ts_recv_ns: int, conn_id: int, conds: list[str] | None = None) -> None:
        """Record where a connection broke.

        Why a row: analysis must be able to see where the stream broke without
        consulting logs. asset_id is always "". condition_id "" = the break applies
        to every book (one shared connection). With one connection per family
        (D-022) the recorder passes that connection's markets, and we write one row
        per market: a break of the ladder feed must not mark the 15-minute books
        stale, they kept streaming on their own connection."""
        self.stats.reconnects += 1
        buf = self.buffers["book_events"]
        # Why None vs []: None = unknown scope (legacy single connection), mark all;
        # [] = the connection carried no markets, nothing went stale.
        for cond in [""] if conds is None else conds:
            buf.append((ts_recv_ns, 0, "", cond, "reconnect", None, None, None, None, "", None, None, None, conn_id))

    def feed(self, ts_recv_ns: int, conn_id: int, raw: str) -> None:
        if raw == RECONNECT:
            self.reconnect(ts_recv_ns, conn_id)
            return
        try:
            msg = orjson.loads(raw)
        except orjson.JSONDecodeError:
            self.stats.parse_errors += 1
            return
        if isinstance(msg, list):
            for m in msg:
                self._one(ts_recv_ns, conn_id, m)
        else:
            self._one(ts_recv_ns, conn_id, msg)

    def _one(self, ts: int, conn: int, m: dict) -> None:
        self.stats.msgs_total += 1
        et = m.get("event_type")
        try:
            if et == "price_change":
                self._price_change(ts, conn, m)
            elif et == "book":
                self._book_snapshot(ts, conn, m)
            elif et == "last_trade_price":
                self._trade(ts, conn, m)
            elif et == "best_bid_ask":
                self._tob(ts, conn, m)
            elif et == "tick_size_change":
                self._tick(ts, conn, m)
            elif et == "market_resolved":
                self._resolved(ts, conn, m)
            else:
                self.stats.msgs_other += 1
                self.stats.other_types[str(et)] = self.stats.other_types.get(str(et), 0) + 1
        except Exception as e:  # never let one bad message kill the consumer
            self.stats.parse_errors += 1
            log.warning("parse error on %s: %r", et, e)

    # ---- handlers ----------------------------------------------------------------

    def _price_change(self, ts: int, conn: int, m: dict) -> None:
        self.stats.msgs_delta += 1
        ts_exch = _i(m.get("timestamp"))
        cond = m.get("market", "")
        buf = self.buffers["book_events"]
        changes = []
        for ch in m.get("price_changes", ()):
            price = _f(ch.get("price"))
            size = _f(ch.get("size"))
            if price is None or size is None:
                self.stats.parse_errors += 1
                continue
            changes.append((ch.get("asset_id", ""), ch.get("side", ""), price, size, ch.get("hash", ""),
                            _f(ch.get("best_bid")), _f(ch.get("best_ask"))))

        # Why (decision A1): one order shows up twice in a message, once per token
        # (YES bid p == NO ask 1-p, same size). If the outcome-1 changes of this
        # message are EXACTLY the mirror of the outcome-0 changes (as multisets:
        # Counter, so duplicates must match too), they carry no information and are
        # not stored. Otherwise all of them are stored, so research can rebuild
        # outcome 1 per message: its own rows if present, else mirror(outcome 0).
        implied: Counter = Counter()
        actual: Counter = Counter()
        for aid, side, price, size, *_ in changes:
            pair = self.token_pair.get(aid)
            if pair is None:
                continue
            if pair[1] == 0:
                implied[(pair[0], _OPPOSITE.get(side, ""), flip_price(price), size)] += 1
            else:
                actual[(aid, side, round(price, 9), size)] += 1
        mirrored = implied == actual
        if not mirrored:
            # Also counts outcome-0 changes with no outcome-1 twin: that case can't be
            # stored (there is no row to keep), so it must at least be visible in health.
            self.stats.mirror_kept += max(1, (implied - actual).total())

        for aid, side, price, size, h, bb, ba in changes:
            # In-memory books always get both tokens: the REST gap check and
            # check_tob compare per token.
            b = self._book(aid)
            b.apply_delta(side, price, size, h, ts_exch)
            b.check_tob(bb, ba)
            if self._secondary(aid) is not None:
                if mirrored:
                    self.stats.mirror_dropped += 1
                    continue
                self.stats.mirror_kept += 1
            buf.append((ts, ts_exch, aid, cond, "delta", None, side, price, size, h, bb, ba, None, conn))
            self.stats.rows_delta += 1

    def _book_snapshot(self, ts: int, conn: int, m: dict) -> None:
        self.stats.msgs_book += 1
        aid = m.get("asset_id", "")
        cond = m.get("market", "")
        ts_exch = _i(m.get("timestamp"))
        h = m.get("hash", "")
        tick = _f(m.get("tick_size"))
        bids = [(_f(l.get("price")), _f(l.get("size"))) for l in m.get("bids", ())]
        asks = [(_f(l.get("price")), _f(l.get("size"))) for l in m.get("asks", ())]
        self.emit_snapshot(ts, ts_exch, aid, cond, "snapshot_ws", bids, asks, h, tick, conn)
        self._book(aid).apply_snapshot(bids, asks, h, ts_exch)

    def emit_snapshot(self, ts, ts_exch, aid, cond, kind, bids, asks, h, tick, conn) -> None:
        """Shared by WS `book` and the REST poller. snapshot_id = ts_recv_ns (unique per snapshot).

        Must be called BEFORE the snapshot is applied to the in-memory book: an
        outcome-1 snapshot is compared with the sibling's book as it stands now."""
        sib = self._secondary(aid)
        if sib is not None and self._redundant_snapshot(aid, sib, bids, asks):
            self.stats.mirror_dropped += len(bids) + len(asks)
            return
        if sib is not None:
            self.stats.mirror_kept += max(1, len(bids) + len(asks))
        buf = self.buffers["book_events"]
        if not bids and not asks:
            buf.append((ts, ts_exch, aid, cond, kind, ts, None, None, None, h, None, None, tick, conn))
            return
        for p, s in bids:
            buf.append((ts, ts_exch, aid, cond, kind, ts, "BUY", p, s, h, None, None, tick, conn))
        for p, s in asks:
            buf.append((ts, ts_exch, aid, cond, kind, ts, "SELL", p, s, h, None, None, tick, conn))

    def _redundant_snapshot(self, aid: str, sib: str, bids: list[tuple], asks: list[tuple]) -> bool:
        """True if this outcome-1 snapshot changes nothing: it equals the token's current
        book AND that book equals mirror(sibling). Both are needed. Matching the mirror
        alone is not enough: on 2026-11-06 the server's NO book briefly differed from
        mirror(YES) at ~0.03% of checks, and a snapshot that resyncs such a NO book is
        a real change that research must see."""
        own, other = self.books.get(aid), self.books.get(sib)
        if own is None or other is None or not (other.bids or other.asks):
            return False  # first snapshot of a token, or nothing to derive from: store it
        m_bids, m_asks = mirror(other.bids, other.asks)
        if own.bids != m_bids or own.asks != m_asks:
            return False

        def as_dict(levels: list[tuple]) -> dict | None:
            out = {}
            for p, s in levels:
                if p is None or s is None:
                    return None  # unparsable level: never drop, let the row show it
                if s > 0:
                    out[round(p, 9)] = s
            return out

        return as_dict(bids) == m_bids and as_dict(asks) == m_asks

    def _trade(self, ts: int, conn: int, m: dict) -> None:
        self.stats.msgs_trade += 1
        self.buffers["trades"].append(
            (
                ts, _i(m.get("timestamp")), m.get("asset_id", ""), m.get("market", ""),
                _f(m.get("price")), _f(m.get("size")), m.get("side", ""), _i(m.get("fee_rate_bps")),
                m.get("transaction_hash", ""), conn,
            )
        )

    def _tob(self, ts: int, conn: int, m: dict) -> None:
        self.stats.msgs_tob += 1
        self.buffers["tob"].append(
            (
                ts, _i(m.get("timestamp")), m.get("asset_id", ""), m.get("market", ""),
                _f(m.get("best_bid")), _f(m.get("best_ask")), _f(m.get("spread")), conn,
            )
        )

    def _tick(self, ts: int, conn: int, m: dict) -> None:
        self.stats.msgs_other += 1
        self.buffers["tick_changes"].append(
            (
                ts, _i(m.get("timestamp")), m.get("asset_id", ""), m.get("market", ""),
                _f(m.get("old_tick_size")), _f(m.get("new_tick_size")), conn,
            )
        )

    def _resolved(self, ts: int, conn: int, m: dict) -> None:
        self.stats.msgs_other += 1
        self.buffers["resolutions"].append(
            (
                ts, _i(m.get("timestamp")), m.get("market", ""), m.get("winning_asset_id", ""),
                m.get("winning_outcome", ""), conn,
            )
        )

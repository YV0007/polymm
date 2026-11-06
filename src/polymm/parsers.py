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
from dataclasses import dataclass, field

import orjson

from .book import BookState
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
    other_types: dict[str, int] = field(default_factory=dict)


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
    def __init__(self, books: dict[str, BookState], buffers: dict[str, list[tuple]], token_cond: dict[str, str]):
        self.books = books
        self.buffers = buffers
        self.token_cond = token_cond  # asset_id -> condition_id (from UniverseWatcher)
        self.stats = ParserStats()

    def _book(self, asset_id: str) -> BookState:
        b = self.books.get(asset_id)
        if b is None:
            b = self.books[asset_id] = BookState(asset_id)
        return b

    # ---- entry -------------------------------------------------------------------

    def feed(self, ts_recv_ns: int, conn_id: int, raw: str) -> None:
        if raw == RECONNECT:
            self.stats.reconnects += 1
            # Why a row: analysis must be able to see where the stream broke
            # without consulting logs. asset_id "" = applies to all.
            self.buffers["book_events"].append(
                (ts_recv_ns, 0, "", "", "reconnect", None, None, None, None, "", None, None, None, conn_id)
            )
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
        for ch in m.get("price_changes", ()):
            aid = ch.get("asset_id", "")
            side = ch.get("side", "")
            price = _f(ch.get("price"))
            size = _f(ch.get("size"))
            h = ch.get("hash", "")
            bb, ba = _f(ch.get("best_bid")), _f(ch.get("best_ask"))
            if price is None or size is None:
                self.stats.parse_errors += 1
                continue
            buf.append((ts, ts_exch, aid, cond, "delta", None, side, price, size, h, bb, ba, None, conn))
            b = self._book(aid)
            b.apply_delta(side, price, size, h, ts_exch)
            b.check_tob(bb, ba)
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
        """Shared by WS `book` and the REST poller. snapshot_id = ts_recv_ns (unique per snapshot)."""
        buf = self.buffers["book_events"]
        if not bids and not asks:
            buf.append((ts, ts_exch, aid, cond, kind, ts, None, None, None, h, None, None, tick, conn))
            return
        for p, s in bids:
            buf.append((ts, ts_exch, aid, cond, kind, ts, "BUY", p, s, h, None, None, tick, conn))
        for p, s in asks:
            buf.append((ts, ts_exch, aid, cond, kind, ts, "SELL", p, s, h, None, None, tick, conn))

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

"""RestPoller: the auditor. Every N seconds, compare REST /books with our local books.

Why it exists: the WS has no sequence numbers. A dropped delta is invisible
from inside the stream. The REST book is an independent witness.

Sync test (robust to the race between the REST read and in-flight deltas):
  in_sync  = REST hash is among the last K hashes we applied
             OR REST levels equal our levels exactly.
Gap       = not in_sync. Then: count it, replace our book with REST (only if
            the REST timestamp is not older than our last applied delta), and
            emit a `snapshot_rest` so replay has a clean restart point.
Anchor    = even when in sync, emit a `snapshot_rest` every `anchor_interval`
            so replay never has to walk back more than that.
"""
from __future__ import annotations

import asyncio
import logging
import time

from .book import BookState
from .parsers import MarketParser
from .rest import PolyRest

log = logging.getLogger(__name__)


def _f(x) -> float:
    return float(x)


class RestPoller:
    def __init__(
        self,
        rest: PolyRest,
        books: dict[str, BookState],
        parser: MarketParser,
        token_cond: dict[str, str],
        interval_s: float,
        anchor_interval_s: float,
    ):
        self.rest = rest
        self.books = books
        self.parser = parser
        self.token_cond = token_cond
        self.interval_s = interval_s
        self.anchor_interval_s = anchor_interval_s
        self.last_anchor: dict[str, float] = {}
        self.polls = 0
        self.gaps = 0
        self.stale_rest = 0
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.interval_s)
            tokens = list(self.token_cond)
            if not tokens:
                continue
            try:
                resp = await self.rest.books(tokens)
            except Exception as e:
                log.warning("REST /books failed: %r", e)
                continue
            self.polls += 1
            now = time.time()
            ts_recv = time.time_ns()
            for b in resp:
                aid = b.get("asset_id", "")
                h = b.get("hash", "")
                ts_exch = int(b.get("timestamp") or 0)
                bids = [(_f(l["price"]), _f(l["size"])) for l in b.get("bids", ())]
                asks = [(_f(l["price"]), _f(l["size"])) for l in b.get("asks", ())]
                tick = float(b["tick_size"]) if b.get("tick_size") else None
                cond = b.get("market", "") or self.token_cond.get(aid, "")
                local = self.books.get(aid)

                in_sync = False
                if local is not None:
                    if h and h in local.recent_hashes:
                        in_sync = True
                    elif local.bids == {p: s for p, s in bids if s > 0} and local.asks == {p: s for p, s in asks if s > 0}:
                        in_sync = True

                anchor_due = now - self.last_anchor.get(aid, 0.0) >= self.anchor_interval_s
                if in_sync and not anchor_due:
                    continue
                if not in_sync:
                    self.gaps += 1
                    if local is None:
                        self.parser._book(aid)  # creates it
                        local = self.books[aid]
                    if ts_exch and ts_exch < local.last_ts_exch_ms:
                        # REST answer is older than our newest delta: cannot judge; keep ours.
                        self.stale_rest += 1
                        log.info("rest older than local for %s… (rest=%d local=%d), not resetting", aid[:10], ts_exch, local.last_ts_exch_ms)
                    else:
                        log.info("gap detected for %s… (rest hash %s not in recent history); resetting book", aid[:10], h[:8])
                        local.apply_snapshot(bids, asks, h, ts_exch)
                self.parser.emit_snapshot(ts_recv, ts_exch, aid, cond, "snapshot_rest", bids, asks, h, tick, 0)
                self.last_anchor[aid] = now

"""RestPoller: the auditor. Every N seconds, compare REST /books with our local books.

Why it exists: the WS has no sequence numbers. A dropped delta is invisible
from inside the stream. The REST book is an independent witness.

Sync test (robust to the race between the REST read and in-flight deltas):
  in_sync  = REST hash is among the last K hashes we applied
             OR REST levels equal our levels exactly.
Gap       = still not in_sync after `confirm_delay_s` AND against a fresh REST
            read. Then: count it, replace our book with the fresh REST book (only
            if its timestamp is not older than our last applied delta), and emit a
            `snapshot_rest` so replay has a clean restart point.
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


def _levels(b: dict) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    bids = [(float(l["price"]), float(l["size"])) for l in b.get("bids", ())]
    asks = [(float(l["price"]), float(l["size"])) for l in b.get("asks", ())]
    return bids, asks


class RestPoller:
    def __init__(
        self,
        rest: PolyRest,
        books: dict[str, BookState],
        parser: MarketParser,
        token_cond: dict[str, str],
        interval_s: float,
        anchor_interval_s: float,
        confirm_delay_s: float = 2.0,
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
        self.races = 0  # mismatches that resolved themselves on the second look
        self.confirm_delay_s = confirm_delay_s
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
            suspects: list[dict] = []
            for b in resp:
                aid = b.get("asset_id", "")
                if self._in_sync(b):
                    if now - self.last_anchor.get(aid, 0.0) >= self.anchor_interval_s:
                        self._emit(b, ts_recv)
                        self.last_anchor[aid] = now
                else:
                    suspects.append(b)
            if suspects:
                await self._confirm(suspects)

    def _in_sync(self, b: dict) -> bool:
        local = self.books.get(b.get("asset_id", ""))
        if local is None:
            return False
        h = b.get("hash", "")
        if h and h in local.recent_hashes:
            return True
        bids, asks = _levels(b)
        return local.bids == {p: s for p, s in bids if s > 0} and local.asks == {p: s for p, s in asks if s > 0}

    async def _confirm(self, suspects: list[dict]) -> None:
        """Why a second look: a mismatch is usually a race, not a gap. Measured on
        2026-11-06: of 3,561 checks, 656 mismatched only because the REST state was
        still in our queue (REST behind by ~30 ms) or not yet on the wire (REST
        ahead by ~95 ms); only ~20 never appeared in the stream. Resetting on a
        race is harmful: queued older deltas then land on top of a newer snapshot.
        So: wait, re-check against what we have applied since, and only for books
        that still disagree fetch a *fresh* REST copy and reset from that."""
        await asyncio.sleep(self.confirm_delay_s)
        still = [b for b in suspects if not self._in_sync(b)]
        self.races += len(suspects) - len(still)
        if not still:
            return
        try:
            fresh = await self.rest.books([b["asset_id"] for b in still])
        except Exception as e:
            log.warning("REST /books (confirm) failed: %r", e)
            return
        ts_recv = time.time_ns()
        for b in fresh:
            if self._in_sync(b):
                self.races += 1
                continue
            aid = b.get("asset_id", "")
            if aid not in self.books:
                self.parser._book(aid)  # creates it
            local = self.books[aid]
            ts_exch = int(b.get("timestamp") or 0)
            # Why emit before reset: the parser decides whether an outcome-1 snapshot
            # is redundant by comparing it with the book as it stands BEFORE it lands.
            self._emit(b, ts_recv)
            self.last_anchor[aid] = time.time()
            if ts_exch and ts_exch < local.last_ts_exch_ms:
                # REST answer is older than our newest delta: cannot judge; keep ours.
                self.stale_rest += 1
                log.info("rest older than local for %s… (rest=%d local=%d), not resetting", aid[:10], ts_exch, local.last_ts_exch_ms)
            else:
                # Why count here and not above: a REST answer older than our newest delta is not
                # a gap (on 2026-11-08, 24 of 28 "gaps" were REST lagging by ~4 ms); it is counted
                # in stale_rest. `gaps` now means: our book was wrong and was reset.
                self.gaps += 1
                log.info("gap confirmed for %s… (rest hash %s not in stream after %.0fs); resetting book",
                         aid[:10], b.get("hash", "")[:8], self.confirm_delay_s)
                bids, asks = _levels(b)
                local.apply_snapshot(bids, asks, b.get("hash", ""), ts_exch)

    def _emit(self, b: dict, ts_recv: int) -> None:
        aid = b.get("asset_id", "")
        bids, asks = _levels(b)
        tick = float(b["tick_size"]) if b.get("tick_size") else None
        cond = b.get("market", "") or self.token_cond.get(aid, "")
        self.parser.emit_snapshot(ts_recv, int(b.get("timestamp") or 0), aid, cond, "snapshot_rest", bids, asks, b.get("hash", ""), tick, 0)

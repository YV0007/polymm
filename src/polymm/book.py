"""In-memory L2 order book for one outcome token.

This is the "whiteboard": the running state we maintain from one snapshot plus
every delta since. It is never written to disk directly (the deltas are); it
exists so that
  1. the REST poller can compare Polymarket's book hash with the hash of the
     last delta we applied (gap detection — the feed has no sequence numbers),
  2. every delta's server-side best_bid/best_ask can be checked against ours
     (cheap continuous sanity check),
  3. the Phase-5 simulator can replay stored events through the same class.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


@dataclass(slots=True)
class BookState:
    asset_id: str
    bids: dict[float, float] = field(default_factory=dict)  # price -> total size
    asks: dict[float, float] = field(default_factory=dict)
    last_hash: str = ""
    last_ts_exch_ms: int = 0
    snapshots_applied: int = 0
    deltas_applied: int = 0
    tob_mismatches: int = 0
    # Why a hash history: the REST poller's answer may predate deltas we have
    # already applied; "is the REST hash one we saw recently" tolerates that race.
    recent_hashes: deque[str] = field(default_factory=lambda: deque(maxlen=2000))

    # Why dict keyed by float price: with <= ~100 levels per side, max()/min()
    # over the keys is microseconds; a sorted container buys nothing here.

    def apply_snapshot(
        self, bids: list[tuple[float, float]], asks: list[tuple[float, float]], hash_: str, ts_exch_ms: int
    ) -> None:
        self.bids = {p: s for p, s in bids if s > 0}
        self.asks = {p: s for p, s in asks if s > 0}
        self.last_hash = hash_
        self.last_ts_exch_ms = ts_exch_ms
        self.recent_hashes.append(hash_)
        self.snapshots_applied += 1

    def apply_delta(self, side: str, price: float, size: float, hash_: str, ts_exch_ms: int) -> None:
        # Why: `size` is the new TOTAL at the level, not an increment. size == 0
        # means the level disappeared. (Confirmed against live snapshots, step 3.)
        book = self.bids if side == "BUY" else self.asks
        if size > 0:
            book[price] = size
        else:
            book.pop(price, None)
        self.last_hash = hash_
        self.last_ts_exch_ms = ts_exch_ms
        self.recent_hashes.append(hash_)
        self.deltas_applied += 1

    # ---- derived -------------------------------------------------------------

    @property
    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None

    @property
    def mid(self) -> float | None:
        b, a = self.best_bid, self.best_ask
        return (a + b) / 2 if (a is not None and b is not None) else None

    @property
    def microprice(self) -> float | None:
        b, a = self.best_bid, self.best_ask
        if a is None or b is None:
            return None
        qb, qa = self.bids[b], self.asks[a]
        if qb + qa == 0:
            return None
        return (a * qb + b * qa) / (qb + qa)

    def check_tob(self, best_bid: float | None, best_ask: float | None) -> bool:
        """Compare server-side top of book (carried on every delta) with ours."""
        ok = (best_bid == self.best_bid) and (best_ask == self.best_ask)
        if not ok:
            self.tob_mismatches += 1
        return ok

    def levels(self) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        return sorted(self.bids.items(), reverse=True), sorted(self.asks.items())

"""UniverseWatcher: turns family *rules* into the set of live tokens.

Why: BTC 15-minute markets are new markets every 15 minutes, each with new
token ids. A static token list would be stale before the first file is
written. So the config names families, and this class answers, every minute,
"which slugs should exist right now?", resolves them through Gamma, and tells
the WebSocket source what to add and what to drop. Every change is also
written to the `markets` reference table, so a recording run is reproducible
from the data alone.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .config import Config, Family
from .rest import MarketInfo, PolyRest

log = logging.getLogger(__name__)


class UniverseWatcher:
    def __init__(self, cfg: Config, rest: PolyRest):
        self.cfg = cfg
        self.rest = rest
        self.markets: dict[str, MarketInfo] = {}  # condition_id -> info (currently subscribed)
        self.token_cond: dict[str, str] = {}  # asset_id -> condition_id
        self._missing: dict[str, float] = {}  # slug -> last time Gamma had nothing (retry later)
        self._last_refresh_rows_ts = 0.0

    # ---- rules ----------------------------------------------------------------

    def desired_slugs(self, now: float | None = None) -> list[tuple[Family, str]]:
        """Pure function of time + rules. Testable without network."""
        now = now or time.time()
        out: list[tuple[Family, str]] = []
        for fam in self.cfg.families:
            if fam.kind == "updown":
                w = fam.window_s or 900
                t0 = int(now // w) * w
                for k in range(fam.lookahead_windows + 1):
                    out.append((fam, f"{fam.slug_prefix}-{t0 + k * w}"))
            elif fam.kind == "daily_ladder":
                tz = ZoneInfo(fam.timezone)
                today = datetime.fromtimestamp(now, tz).date()
                for k in range(fam.lookahead_days + 1):
                    d = today + timedelta(days=k)
                    # Why ET date: the slug carries the calendar date of the 12:00 ET resolution.
                    out.append((fam, f"{fam.slug_prefix}-{d.strftime('%B').lower()}-{d.day}-{d.year}"))
            elif fam.kind == "fixed_event":
                out.append((fam, fam.event_slug or ""))
        return out

    # ---- refresh --------------------------------------------------------------

    async def refresh(self) -> tuple[list[MarketInfo], list[MarketInfo], list[MarketInfo]]:
        """Returns (added, removed, refreshed) market lists."""
        now = time.time()
        grace = self.cfg.recorder.unsubscribe_grace_s
        added: list[MarketInfo] = []
        refreshed: list[MarketInfo] = []

        for fam, slug in self.desired_slugs(now):
            # Why retry throttling: a 15m slug may not exist yet; no point hitting
            # Gamma for it more than once per refresh interval.
            if self._missing.get(slug, 0) > now - self.cfg.recorder.universe_refresh_s * 0.9:
                continue
            try:
                ev = await self.rest.event_by_slug(slug)
            except Exception as e:  # network blip: keep what we have, try next round
                log.warning("gamma lookup failed for %s: %s", slug, e)
                continue
            if not ev:
                self._missing[slug] = now
                continue
            for mi in PolyRest.markets_from_event(ev, fam.name):
                if mi.end_dt and mi.end_dt.timestamp() + grace < now:
                    continue  # already over
                if mi.condition_id in self.markets:
                    refreshed.append(mi)
                    self.markets[mi.condition_id] = mi
                    continue
                try:
                    mi.rate_per_day = await self.rest.rewards_rate_per_day(mi.condition_id)
                except Exception as e:
                    log.warning("rewards lookup failed for %s: %s", mi.market_slug, e)
                self.markets[mi.condition_id] = mi
                for aid, _ in mi.tokens:
                    self.token_cond[aid] = mi.condition_id
                added.append(mi)

        removed: list[MarketInfo] = []
        for cid, mi in list(self.markets.items()):
            if mi.end_dt and mi.end_dt.timestamp() + grace < now:
                removed.append(mi)
                del self.markets[cid]
                for aid, _ in mi.tokens:
                    self.token_cond.pop(aid, None)

        if added:
            log.info("universe +%d markets: %s", len(added), ", ".join(m.market_slug for m in added)[:300])
        if removed:
            log.info("universe -%d markets: %s", len(removed), ", ".join(m.market_slug for m in removed)[:300])
        return added, removed, refreshed

    def desired_tokens(self) -> set[str]:
        return set(self.token_cond)

    # ---- rows for the `markets` table --------------------------------------------

    @staticmethod
    def rows(action: str, markets: list[MarketInfo], ts_ns: int | None = None) -> list[tuple]:
        ts_ns = ts_ns or time.time_ns()
        out = []
        for mi in markets:
            for aid, outcome in mi.tokens:
                out.append(
                    (
                        ts_ns, action, mi.family, mi.event_slug, mi.market_slug, mi.question, mi.condition_id,
                        aid, outcome, mi.start_date, mi.end_date, mi.tick_size, mi.min_order_size, mi.neg_risk,
                        mi.fees_enabled, mi.fee_rate, mi.fee_rebate_rate, mi.rewards_min_size,
                        mi.rewards_max_spread, mi.rate_per_day, mi.accepting_orders,
                    )
                )
        return out

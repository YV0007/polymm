"""Thin async REST clients for Gamma (catalogue) and CLOB (exchange).

Why one small module and not the official SDK: we only read four endpoints,
the SDK pulls in signing/wallet code we do not need for recording, and keeping
the HTTP layer tiny makes it obvious what we depend on when Polymarket changes it.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

from .config import Endpoints

log = logging.getLogger(__name__)


@dataclass(slots=True)
class MarketInfo:
    family: str
    event_slug: str
    market_slug: str
    question: str
    condition_id: str
    tokens: list[tuple[str, str]]  # [(asset_id, outcome), ...]
    start_date: str
    end_date: str
    tick_size: float
    min_order_size: float
    neg_risk: bool
    fees_enabled: bool
    fee_rate: float | None
    fee_rebate_rate: float | None
    rewards_min_size: float | None
    rewards_max_spread: float | None
    accepting_orders: bool
    rate_per_day: float | None = None
    end_dt: datetime | None = field(default=None)

    def __post_init__(self) -> None:
        self.end_dt = _parse_iso(self.end_date)


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _f(x, default=None) -> float | None:
    try:
        return float(x) if x is not None else default
    except (TypeError, ValueError):
        return default


class PolyRest:
    def __init__(self, ep: Endpoints):
        self.ep = ep
        # Why an honest UA: Gamma/CLOB are public, but identifying the client is
        # polite and makes our traffic attributable if they ever ask.
        self.http = httpx.AsyncClient(
            timeout=httpx.Timeout(20.0), headers={"User-Agent": "polymm-recorder/0.1 (research)"}
        )

    async def aclose(self) -> None:
        await self.http.aclose()

    # ---- Gamma ----------------------------------------------------------------

    async def event_by_slug(self, slug: str) -> dict | None:
        r = await self.http.get(f"{self.ep.gamma}/events", params={"slug": slug})
        r.raise_for_status()
        data = r.json()
        return data[0] if data else None

    @staticmethod
    def markets_from_event(event: dict, family: str) -> list[MarketInfo]:
        out: list[MarketInfo] = []
        for m in event.get("markets", []):
            try:
                token_ids = json.loads(m.get("clobTokenIds") or "[]")
                outcomes = json.loads(m.get("outcomes") or "[]")
            except json.JSONDecodeError:
                continue
            if not token_ids:
                continue
            fs = m.get("feeSchedule") or {}
            out.append(
                MarketInfo(
                    family=family,
                    event_slug=event.get("slug", ""),
                    market_slug=m.get("slug", ""),
                    question=m.get("question", ""),
                    condition_id=m.get("conditionId", ""),
                    tokens=list(zip(token_ids, outcomes if len(outcomes) == len(token_ids) else [""] * len(token_ids))),
                    start_date=m.get("startDate") or "",
                    end_date=m.get("endDate") or "",
                    tick_size=_f(m.get("orderPriceMinTickSize"), 0.01),
                    min_order_size=_f(m.get("orderMinSize"), 0.0),
                    neg_risk=bool(m.get("negRisk")),
                    fees_enabled=bool(m.get("feesEnabled")),
                    fee_rate=_f(fs.get("rate")),
                    fee_rebate_rate=_f(fs.get("rebateRate")),
                    rewards_min_size=_f(m.get("rewardsMinSize")),
                    rewards_max_spread=_f(m.get("rewardsMaxSpread")),
                    accepting_orders=bool(m.get("acceptingOrders")),
                )
            )
        return out

    # ---- CLOB -----------------------------------------------------------------

    async def rewards_rate_per_day(self, condition_id: str) -> float | None:
        """Total daily reward pool for a market, or None if it has none."""
        r = await self.http.get(f"{self.ep.clob}/rewards/markets/{condition_id}")
        if r.status_code != 200:
            return None
        data = r.json().get("data") or []
        if not data:
            return None
        row = data[0]
        total = row.get("total_daily_rate")
        if total is not None:
            return _f(total)
        return sum(_f(c.get("rate_per_day"), 0.0) for c in row.get("rewards_config") or [])

    async def books(self, token_ids: list[str]) -> list[dict]:
        """POST /books in chunks of 500 (API limit)."""
        out: list[dict] = []
        for i in range(0, len(token_ids), 500):
            chunk = token_ids[i : i + 500]
            r = await self.http.post(f"{self.ep.clob}/books", json=[{"token_id": t} for t in chunk])
            r.raise_for_status()
            out.extend(r.json())
        return out

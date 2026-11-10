"""R0: what every market pays today, and which sector it is in.

Sources (public, no key; checked 2026-11-10):
    CLOB  GET /rewards/markets/current   every market with a live pool: total_daily_rate (USD/day,
                                          native + sponsored), rewards_max_spread (¢), rewards_min_size
    CLOB  GET /sampling-markets          the same markets with question, tokens, tags, end date,
                                          neg_risk, taker delay (seconds_delay)
    CLOB  GET /fee-rate?token_id=…        base_fee > 0 iff fees are on (spot check)
Why not /rewards/markets/multi: it lists every market ever created (100k+ rows, most with an empty
pool) and paging it takes over ten minutes; the two endpoints above return the ~11k live ones in
seconds.

Snapshots go to <data>/reference/rewards/dt=<UTC date>/markets.parquet (data, never git). Only the
current configuration is published, so history exists only from the day we start snapshotting.
"""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from .rules import FEES

CLOB = "https://clob.polymarket.com"
# Why a plain browser-like agent: the default Python agent is refused (HTTP 403); this names no person.
UA = {"User-Agent": "Mozilla/5.0 (polymm research)", "Content-Type": "application/json"}


def http_json(url: str, data=None, timeout: int = 60, tries: int = 4):
    for k in range(tries):
        try:
            req = urllib.request.Request(url, data=None if data is None else json.dumps(data).encode(), headers=UA,
                                         method="GET" if data is None else "POST")
            return json.load(urllib.request.urlopen(req, timeout=timeout))
        except Exception:
            if k == tries - 1:
                raise
            time.sleep(2 * (k + 1))


def _all_pages(path: str, cap: int = 400) -> list[dict]:
    rows, cur = [], ""
    for _ in range(cap):
        d = http_json(f"{CLOB}/{path}?" + urllib.parse.urlencode({"next_cursor": cur} if cur else {}))
        rows += d.get("data") or []
        cur = d.get("next_cursor") or ""
        if not cur or cur == "LTE=" or not d.get("data"):
            break
    return rows


# Tag → fee category. Why ordered: a market carries many tags ("Economy", "Trump", "Fed"); the
# first matching rule wins, most specific first, so a Fed decision is Economics, not Politics.
SECTOR_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("Sports", ("sports", "nfl", "nba", "nhl", "mlb", "soccer", "football", "tennis", "golf", "ufc", "mma", "boxing", "f1",
                "formula 1", "cricket", "basketball", "baseball", "hockey", "olympics", "esports", "league of legends",
                "counter-strike", "cs2", "dota", "dota 2", "valorant", "ncaa", "epl", "premier league", "champions league",
                "la liga", "serie a")),
    ("Crypto", ("crypto", "bitcoin", "ethereum", "solana", "xrp", "memecoin", "airdrop", "hyperliquid", "defi", "crypto prices")),
    ("Economics", ("fed", "fed rates", "fomc", "economy", "inflation", "cpi", "gdp", "jobs", "unemployment", "recession",
                   "macro indicators", "global rates", "interest rates", "economic policy")),
    ("Finance", ("finance", "stocks", "business", "ipo", "ipos", "earnings", "commodities", "s&p 500", "nasdaq", "tariffs")),
    ("Tech", ("tech", "ai", "openai", "artificial intelligence", "big tech", "spacex", "science")),
    ("Mentions", ("mentions", "mention markets")),
    ("Weather", ("weather", "climate", "temperature", "hurricanes")),
    ("Geopolitics", ("geopolitics", "world affairs", "middle east", "israel", "iran", "ukraine", "russia", "china", "gaza",
                     "war", "foreign policy", "nato")),
    ("Politics", ("politics", "elections", "us election", "election", "trump", "congress", "senate", "president",
                  "primaries", "democrats", "republicans", "global elections", "us politics")),
    ("Culture", ("culture", "pop culture", "movies", "music", "awards", "oscars", "celebrities", "tv", "entertainment",
                 "youtube", "tweets", "social media")),
]


def sector_of(tags: list[str]) -> str:
    low = {t.lower() for t in tags}
    for sector, keys in SECTOR_RULES:
        if low & set(keys):
            return sector
    return "Other"


def fetch_live() -> pl.DataFrame:
    """One row per market with a live pool: rate, scoring parameters, tokens, tags, sector, fees."""
    cur = {r["condition_id"]: r for r in _all_pages("rewards/markets/current")}
    meta = {r["condition_id"]: r for r in _all_pages("sampling-markets")}
    rows = []
    for cid, r in cur.items():
        m = meta.get(cid, {})
        toks = m.get("tokens") or []
        yes = next((t for t in toks if (t.get("outcome") or "").lower() == "yes"), toks[0] if toks else {})
        no = next((t for t in toks if t is not yes), {})
        tags = [t for t in (m.get("tags") or []) if isinstance(t, str)]
        rows.append({
            "condition_id": cid, "question": m.get("question"), "market_slug": m.get("market_slug"),
            "yes_token": yes.get("token_id"), "no_token": no.get("token_id"), "price": yes.get("price"),
            "rate_per_day": float(r.get("total_daily_rate") or 0), "native_rate": float(r.get("native_daily_rate") or 0),
            "max_spread_c": float(r.get("rewards_max_spread") or 0), "min_size": float(r.get("rewards_min_size") or 0),
            "end_date": m.get("end_date_iso"), "neg_risk": bool(m.get("neg_risk")), "taker_delay_s": float(m.get("seconds_delay") or 0),
            "tick": float(m.get("minimum_tick_size") or 0.01), "tags": tags, "sector": sector_of(tags),
            "accepting_orders": bool(m.get("accepting_orders", True)), "taker_base_fee": float(m.get("taker_base_fee") or 0),
        })
    df = pl.DataFrame(rows, schema_overrides={"tags": pl.List(pl.String)}, infer_schema_length=None)
    # Why fees from the market's own taker_base_fee: tags put some fee-charging markets in
    # "Geopolitics" (fee-free as a category) and some fee-free ones elsewhere (audit finding 4).
    # The market says WHETHER it charges; the sector only picks the rate (a charging market tagged
    # Geopolitics gets the Politics rate, the category it overlaps with).
    fee = pl.DataFrame([{"sector": k, "fee_rate_cat": v[0], "rebate_rate_cat": v[1]} for k, v in FEES.items()])
    df = df.join(fee, on="sector", how="left")
    pol_f, pol_r = FEES["Politics"]
    on = pl.col("taker_base_fee") > 0
    return df.with_columns(
        pl.when(~on).then(0.0).when(pl.col("fee_rate_cat").fill_null(0) > 0).then(pl.col("fee_rate_cat")).otherwise(pol_f).alias("fee_rate"),
        pl.when(~on).then(0.0).when(pl.col("rebate_rate_cat").fill_null(0) > 0).then(pl.col("rebate_rate_cat")).otherwise(pol_r).alias("rebate_rate"),
    ).drop("fee_rate_cat", "rebate_rate_cat")


def snapshot(data_root: Path, day: str | None = None) -> pl.DataFrame:
    """Fetch every market with a live pool and write the day's file."""
    day = day or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df = fetch_live().with_columns(pl.lit(day).alias("snapshot_day"), pl.lit(datetime.now(timezone.utc).isoformat()).alias("fetched_at"))
    out = data_root / "reference" / "rewards" / f"dt={day}"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "markets.parquet")
    return df


def fee_spot_check(df: pl.DataFrame, n: int = 20) -> dict:
    """GET /fee-rate for a few markets per sector: base_fee > 0 should mean the sector has a taker fee."""
    out = {}
    for sec in df["sector"].unique().to_list():
        s = df.filter((pl.col("sector") == sec) & pl.col("yes_token").is_not_null()).head(max(1, n // 10))
        vals = []
        for t in s["yes_token"].to_list():
            try:
                vals.append(int(http_json(f"{CLOB}/fee-rate?token_id={t}").get("base_fee") or 0))
            except Exception:
                pass
        out[sec] = {"checked": len(vals), "fee_on": sum(v > 0 for v in vals)}
    return out

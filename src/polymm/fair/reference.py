"""F0b: one row per market with what the fair-value model needs from the catalogue.

    family, event_slug, condition_id, yes_asset, no_asset,
    t_start_s   15m: window start (eventStartTime); ladder/Fed: null
    t_end_s     settlement instant (endDate)
    strike      15m: priceToBeat (Chainlink at window start); ladder: groupItemTitle; Fed: null
    final_price 15m: finalPrice (Chainlink at window end); else null
    outcome     1 if YES/Up won, 0 if NO/Down won, null if not resolved yet

Why Gamma and not our own recording: the 15-minute strike is a Chainlink value we do not
record (paid stream), and the resolution needs the official outcome, not our guess from
Binance. Checks (`check_reference`): finalPrice of window k equals priceToBeat of window
k + 1; every outcome agrees with the `market_resolved` messages we recorded; every ladder
strike parses.
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path

import duckdb
import httpx
import polars as pl

log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)  # one INFO line per request is noise

GAMMA = "https://gamma-api.polymarket.com"
SCHEMA = {
    "family": pl.String, "event_slug": pl.String, "condition_id": pl.String, "question": pl.String,
    "yes_asset": pl.String, "no_asset": pl.String, "t_start_s": pl.Int64, "t_end_s": pl.Int64,
    "strike": pl.Float64, "final_price": pl.Float64, "outcome": pl.Int8, "closed": pl.Boolean,
}


def _iso_s(s: str | None) -> int | None:
    if not s:
        return None
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def _num(s) -> float | None:
    try:
        return float(str(s).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return None


def load_markets(data_dirs: list[Path]) -> pl.DataFrame:
    """Distinct markets from the recorder's `markets` reference table(s), with both token ids.

    Why union_by_name: early files lack `outcome_index` (added 2026-11-07); for those the
    outcome label decides (Yes/Up = 0)."""
    globs = [str(d / "reference" / "markets" / "**" / "*.parquet") for d in data_dirs if (d / "reference" / "markets").exists()]
    q = f"""
        select family, event_slug, condition_id, any_value(question) as question, any_value(end_date) as end_date,
               max(case when coalesce(outcome_index, case when outcome in ('Yes','Up') then 0 else 1 end) = 0 then asset_id end) as yes_asset,
               max(case when coalesce(outcome_index, case when outcome in ('Yes','Up') then 0 else 1 end) = 1 then asset_id end) as no_asset
        from read_parquet({globs!r}, union_by_name=true)
        group by 1, 2, 3"""
    return pl.from_arrow(duckdb.sql(q).arrow()).sort("family", "event_slug", "condition_id")


def _fetch_event(client: httpx.Client, slug: str) -> dict | None:
    for attempt in range(3):
        try:
            r = client.get(f"{GAMMA}/events", params={"slug": slug})
            r.raise_for_status()
            data = r.json()
            return data[0] if data else None
        except (httpx.HTTPError, json.JSONDecodeError) as e:
            log.warning("gamma %s attempt %d: %r", slug, attempt + 1, e)
            time.sleep(1.0 + attempt)
    return None


def _outcome(m: dict) -> int | None:
    """1 if outcome 0 (Yes/Up) won. Why require umaResolutionStatus=resolved and a 1/0 pair:
    outcomePrices of an open market are live prices, not a result."""
    if not m.get("closed"):
        return None
    try:
        px = [float(x) for x in json.loads(m.get("outcomePrices") or "[]")]
    except (json.JSONDecodeError, ValueError):
        return None
    if len(px) != 2 or sorted(px) != [0.0, 1.0]:
        return None
    return int(px[0] == 1.0)


def build_reference(data_dirs: list[Path], out_path: Path, refresh_open: bool = True) -> pl.DataFrame:
    """Fetch Gamma once per event; reuse cached rows of markets that are already resolved.

    Why cache: resolved markets never change, and the 15-minute family adds ~96 events a day."""
    mk = load_markets(data_dirs)
    cached = pl.read_parquet(out_path) if out_path.exists() else pl.DataFrame(schema=SCHEMA)
    done = set(cached.filter(pl.col("outcome").is_not_null())["condition_id"].to_list()) if cached.height else set()
    todo = mk.filter(~pl.col("condition_id").is_in(list(done)))
    if not refresh_open:
        todo = todo.filter(~pl.col("condition_id").is_in(cached["condition_id"].to_list()))
    rows: list[dict] = []
    with httpx.Client(timeout=20, headers={"User-Agent": "polymm-research"}) as client:
        for slug in todo["event_slug"].unique().sort().to_list():
            ev = _fetch_event(client, slug)
            if ev is None:
                log.warning("gamma: no event for %s", slug)
                continue
            meta = ev.get("eventMetadata") or {}
            fam = todo.filter(pl.col("event_slug") == slug)["family"][0]
            for m in ev.get("markets", []):
                cond = m.get("conditionId", "")
                hit = mk.filter(pl.col("condition_id") == cond)
                if not hit.height:
                    continue  # a market of the event we never subscribed to
                h = hit.row(0, named=True)
                if fam == "btc_15m":
                    strike, final = _num(meta.get("priceToBeat")), _num(meta.get("finalPrice"))
                    t_start = _iso_s(m.get("eventStartTime"))
                elif fam == "btc_daily_ladder":
                    # Why groupItemTitle first: it is the bare strike ("68,000"); the question
                    # text is the fallback ("... above $68,000 on November 8?").
                    strike = _num(m.get("groupItemTitle"))
                    if strike is None:
                        mm = re.search(r"\$([\d,]+(?:\.\d+)?)", m.get("question", ""))
                        strike = _num(mm.group(1)) if mm else None
                    final, t_start = None, None
                else:
                    strike, final, t_start = None, None, None
                rows.append({
                    "family": fam, "event_slug": slug, "condition_id": cond, "question": m.get("question", ""),
                    "yes_asset": h["yes_asset"], "no_asset": h["no_asset"], "t_start_s": t_start,
                    "t_end_s": _iso_s(m.get("endDate")), "strike": strike, "final_price": final,
                    "outcome": _outcome(m), "closed": bool(m.get("closed")),
                })
            time.sleep(0.05)  # polite: ~20 requests/s at most
    new = pl.DataFrame(rows, schema=SCHEMA) if rows else pl.DataFrame(schema=SCHEMA)
    keep = cached.filter(~pl.col("condition_id").is_in(new["condition_id"].to_list())) if cached.height else cached
    ref = pl.concat([keep, new], how="vertical_relaxed").sort("family", "t_end_s", "strike", nulls_last=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".tmp")
    ref.write_parquet(tmp)
    tmp.replace(out_path)
    log.info("reference: %d markets (%d fetched now, %d resolved)", ref.height, new.height, ref["outcome"].is_not_null().sum())
    return ref


def check_reference(ref: pl.DataFrame, data_dirs: list[Path]) -> dict:
    """The three F0b checks. Returns counts; the caller decides what is fatal."""
    out: dict = {}
    w = ref.filter(pl.col("family") == "btc_15m").sort("t_start_s")
    nxt = w.with_columns(pl.col("strike").shift(-1).alias("next_strike"), pl.col("t_start_s").shift(-1).alias("next_start"))
    adj = nxt.filter((pl.col("next_start") == pl.col("t_end_s")) & pl.col("final_price").is_not_null() & pl.col("next_strike").is_not_null())
    out["chain_pairs"] = adj.height
    out["chain_mismatch"] = adj.filter((pl.col("final_price") - pl.col("next_strike")).abs() > 1e-6).height
    # outcome rule: Up iff finalPrice >= priceToBeat
    r15 = w.filter(pl.col("outcome").is_not_null() & pl.col("final_price").is_not_null())
    out["rule_checked"] = r15.height
    out["rule_mismatch"] = r15.filter((pl.col("final_price") >= pl.col("strike")).cast(pl.Int8) != pl.col("outcome")).height
    lad = ref.filter(pl.col("family") == "btc_daily_ladder")
    out["ladder_markets"] = lad.height
    out["ladder_strike_missing"] = lad.filter(pl.col("strike").is_null()).height
    # our recorded market_resolved messages vs Gamma
    globs = [str(d / "events" / "resolutions" / "**" / "*.parquet") for d in data_dirs if (d / "events" / "resolutions").exists()]
    if globs:
        res = pl.from_arrow(duckdb.sql(f"select distinct condition_id, winning_asset_id from read_parquet({globs!r}, union_by_name=true)").arrow())
        j = ref.filter(pl.col("outcome").is_not_null()).join(res, on="condition_id", how="inner")
        out["ws_resolutions_compared"] = j.height
        out["ws_resolution_mismatch"] = j.filter((pl.col("winning_asset_id") == pl.col("yes_asset")).cast(pl.Int8) != pl.col("outcome")).height
    return out

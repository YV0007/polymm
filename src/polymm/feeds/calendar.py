"""Macro event calendar (D-042): the scheduled releases that move BTC inside a 15-minute window.

Source of truth is `macro_calendar.csv` next to this file, human-maintained, tracked in git:
  date (YYYY-MM-DD, local to `tz`), time (HH:MM), tz, event, source, importance, note
Why a CSV and not an API: the schedules are published months ahead on bls.gov, bea.gov and
federalreserve.gov; a dozen rows a month, checked by hand, beats a scraper that breaks.
Why local time + tz: US releases are fixed in Eastern time; UTC shifts an hour at the
daylight-saving boundaries, and the conversion belongs in code, not in the sheet.

`load_calendar()` returns the rows with `ts_utc` plus the rule-based rows that are not worth
listing: weekly initial jobless claims (Thursdays 08:30 ET) and Deribit option expiries
(daily 08:00 UTC; the Friday ones are the large weekly/monthly expiries).
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

CSV = Path(__file__).with_name("macro_calendar.csv")


def _utc(d: date, t: time, tz: str) -> datetime:
    return datetime.combine(d, t, tzinfo=ZoneInfo(tz)).astimezone(ZoneInfo("UTC"))


def load_calendar(start: date | None = None, end: date | None = None, rules: bool = True) -> pl.DataFrame:
    rows = []
    df = pl.read_csv(CSV, schema_overrides={"date": pl.String, "time": pl.String})
    for r in df.iter_rows(named=True):
        d = date.fromisoformat(r["date"])
        hh, mm = map(int, r["time"].split(":"))
        rows.append((_utc(d, time(hh, mm), r["tz"]), r["event"], r["source"], r["importance"], r["note"] or ""))
    if rules:
        lo = start or min(x[0].date() for x in rows)
        hi = end or max(x[0].date() for x in rows)
        d = lo
        while d <= hi:
            if d.weekday() == 3:  # Thursday
                rows.append((_utc(d, time(8, 30), "America/New_York"), "initial_jobless_claims", "rule: Thursdays 08:30 ET (holiday weeks shift; check dol.gov)", "medium", ""))
            rows.append((_utc(d, time(8, 0), "UTC"), "deribit_expiry", "rule: daily 08:00 UTC", "high" if d.weekday() == 4 else "low",
                         "weekly/monthly expiry" if d.weekday() == 4 else "daily expiry"))
            d += timedelta(days=1)
    out = pl.DataFrame(rows, schema=["ts_utc", "event", "source", "importance", "note"], orient="row").sort("ts_utc")
    if start:
        out = out.filter(pl.col("ts_utc") >= datetime.combine(start, time(0), tzinfo=ZoneInfo("UTC")))
    if end:
        out = out.filter(pl.col("ts_utc") < datetime.combine(end + timedelta(days=1), time(0), tzinfo=ZoneInfo("UTC")))
    return out

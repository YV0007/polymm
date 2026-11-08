"""Typed configuration loaded from config.yaml.

Why pydantic: a typo in the yaml ("flush_interval" vs "flush_interval_s") fails at
startup with a clear message instead of silently using a default at 3 am.
"""
from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field


class Paths(BaseModel):
    data_dir: Path = Path("data")
    log_dir: Path = Path("logs")


class Endpoints(BaseModel):
    ws_market: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    clob: str = "https://clob.polymarket.com"
    gamma: str = "https://gamma-api.polymarket.com"
    data_api: str = "https://data-api.polymarket.com"


class RecorderSettings(BaseModel):
    queue_maxsize: int = 200_000
    flush_interval_s: float = 30
    flush_max_rows: int = 200_000
    raw_zstd_level: int = 12
    raw_rotate_minutes: int = 60
    ping_interval_s: float = 10
    rest_poll_interval_s: float = 60
    rest_anchor_interval_s: float = 600
    universe_refresh_s: float = 60
    unsubscribe_grace_s: float = 600
    health_interval_s: float = 10
    custom_feature_enabled: bool = True
    # Why (D-022): one WS connection per market family. Polymarket cuts a connection
    # whose server-side send buffer fills ("slow consumer"); smaller per-connection
    # flow fills it less often, and a cut on one family leaves the others streaming.
    ws_shard_by_family: bool = True


class Family(BaseModel):
    """One rule that expands to a set of live markets.

    kind=updown       rolling windows: slug = f"{slug_prefix}-{window_start_unix}"
    kind=daily_ladder one event per calendar day: slug = f"{slug_prefix}-{month}-{day}-{year}"
    kind=fixed_event  a single event slug, all its markets
    """

    name: str
    kind: Literal["updown", "daily_ladder", "fixed_event"]
    slug_prefix: str | None = None
    window_s: int | None = None
    lookahead_windows: int = 2
    timezone: str = "America/New_York"
    lookahead_days: int = 2
    event_slug: str | None = None


class Spot(BaseModel):
    """Binance spot feed recorded alongside Polymarket (the price that drives the BTC markets).

    Why in the same process: one clock on one box stamps both feeds, which is what
    makes "Binance moved at t, Polymarket followed at t+δ" measurable.
    """

    enabled: bool = True
    # Why this host: Binance's market-data-only endpoint (no account, no key); the
    # trading endpoint stream.binance.com carries the same public streams.
    ws: str = "wss://data-stream.binance.vision"
    symbols: list[str] = ["BTCUSDT"]
    streams: list[str] = ["bookTicker", "trade"]
    history_base: str = "https://data.binance.vision/data/spot/daily"


class Config(BaseModel):
    paths: Paths = Field(default_factory=Paths)
    endpoints: Endpoints = Field(default_factory=Endpoints)
    recorder: RecorderSettings = Field(default_factory=RecorderSettings)
    spot: Spot = Field(default_factory=Spot)
    families: list[Family]

    # Why: resolve relative paths against the config file's directory, so
    # `polymm record` works from any cwd and from launchd/systemd.
    root: Path = Path(".")

    @property
    def data_dir(self) -> Path:
        p = self.paths.data_dir
        return p if p.is_absolute() else (self.root / p)

    @property
    def log_dir(self) -> Path:
        p = self.paths.log_dir
        return p if p.is_absolute() else (self.root / p)


def load_config(path: str | Path = "config.yaml") -> Config:
    path = Path(path).resolve()
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    cfg = Config(**raw)
    cfg.root = path.parent
    return cfg

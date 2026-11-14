"""What the feeds recorder connects to (D-042): one spec per connection.

Why specs and not classes per venue: every public market-data WebSocket is the same loop
(connect, send a few subscribe frames, answer pings, stamp and store every frame). What
differs is data, not behaviour: the URL, the subscribe frames, the ping convention, the
pong text to drop and, for Polymarket's PolyBolt, an auth frame built from a secrets file.
A spec holds that data; `recorder.WsFeed` runs it.

Raw layout (same line format as the main recorder, `sinks.RawSink`):
  data/feeds/raw/<feed>/dt=YYYY-MM-DD/hour=HH/frames-<HHMMSS>-<seq>.jsonl.zst
  line = <ts_recv_ns>\\t<conn_id>\\t<raw frame>\\n           conn_id 0 = REST poll, wrapped as
                                                           {"_rest": <name>, "ts": ..., "body": ...}
Only the feed's own messages are stored. Pongs and subscribe acknowledgements are dropped
by `keep`; the `rtds` feed keeps only BTC 15m activity (`ACTIVITY_PREFIXES`).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import orjson

ACTIVITY_PREFIXES = ("btc-updown-15m",)  # Polymarket activity: trades of these events only


def read_env_file(path: str | Path) -> dict[str, str]:
    """KEY=VALUE lines of a secrets file (mode 600, outside the repo). Why a file and not
    the environment: systemd units can read it with EnvironmentFile= and nothing in the
    process list, the repo or a deploy shows the secret."""
    out: dict[str, str] = {}
    p = Path(os.path.expanduser(path))
    if not p.exists():
        return out
    for line in p.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


@dataclass
class RestPoll:
    """A REST request repeated every `every_s`, stored in the feed's raw stream (conn_id 0).
    `on_connect`: also run right after each (re)connect of the feed's WebSocket; that is how
    a diff-depth stream gets the book snapshot it is applied to."""

    name: str
    url: str
    every_s: float
    on_connect: bool = False


@dataclass
class FeedSpec:
    name: str
    url: str
    subscribe: list[str] = field(default_factory=list)  # frames sent after connect (after auth)
    ping: tuple[str, float] | None = None  # (text frame, every s); None = protocol pings only
    drop: tuple[str, ...] = ()  # exact frames never stored (pongs)
    auth_env: str | None = None  # secrets file; `auth_frame` builds the frame from it
    auth_frame: str | None = None  # python format string over the env dict keys
    keep_prefix_field: str | None = None  # see `keep`
    polls: list[RestPoll] = field(default_factory=list)
    max_size: int = 16 * 2**20
    # Why an idle limit: a socket can stay open and answer pings while the server has silently
    # dropped our subscription. RTDS did exactly that on the first night (2026-11-13/14): each
    # connection streamed 2–77 min, then nothing for 60–90 min until the server closed it;
    # 482 of the first 720 minutes were lost. Every feed here sends at least ~1 frame/s when
    # healthy, so "no frame for idle_s" means reconnect, not "quiet market".
    idle_s: float = 60.0

    def keep(self, raw: str) -> bool:
        if raw in self.drop:
            return False
        if self.keep_prefix_field and '"activity"' in raw:
            # Why parse here, on the hot path: the activity topic is every trade on every
            # Polymarket market (~35/s); BTC 15m is ~1 % of it and the rest is not ours.
            try:
                v = orjson.loads(raw).get("payload", {}).get(self.keep_prefix_field, "")
            except Exception:
                return True  # unknown shape: keep, the parser will count it
            return v.startswith(ACTIVITY_PREFIXES)
        return True

    def auth(self) -> str | None:
        if not self.auth_env or not self.auth_frame:
            return None
        env = read_env_file(self.auth_env)
        return self.auth_frame.format(**env)


def _j(o) -> str:
    return orjson.dumps(o).decode()


BINANCE_F = "wss://fstream.binance.com"
BINANCE_S = "wss://data-stream.binance.vision"
POLY_SECRETS = "~/.config/polymm/polybolt.env"

# Why these streams: docs/DATA_PLAN.md §6 (sizes measured 2026-11-13). One connection per
# venue where the venue allows it; Binance futures splits its streams over two paths since
# 2026-03 (public: book; market: trades, mark price, liquidations), so it is two connections.
FEEDS: dict[str, FeedSpec] = {
    # Why best bid/ask on its own connection: measured 2026-11-13 on one shared connection the
    # bookTicker (~1,300 msg/s in a busy minute) arrived 630 ms late at the median while trades on
    # another connection arrived in 128 ms; the depth batches queue behind it. The stamp on the
    # fastest stream is the one lead–lag depends on, so it shares a socket with nothing.
    "binance_perp_bbo": FeedSpec(name="binance_perp_bbo", url=f"{BINANCE_F}/public/stream?streams=btcusdt@bookTicker"),
    "binance_perp_book": FeedSpec(
        name="binance_perp_book",
        url=f"{BINANCE_F}/public/stream?streams=btcusdt@depth@100ms",
        polls=[RestPoll("depth_snapshot", "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000", 600, on_connect=True)],
    ),
    "binance_perp_market": FeedSpec(
        name="binance_perp_market",
        url=f"{BINANCE_F}/market/stream?streams=btcusdt@aggTrade/btcusdt@markPrice@1s/btcusdt@forceOrder",
    ),
    "binance_spot_depth": FeedSpec(
        name="binance_spot_depth",
        url=f"{BINANCE_S}/stream?streams=btcusdt@depth@100ms",
        polls=[RestPoll("depth_snapshot", "https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=5000", 600, on_connect=True)],
    ),
    "coinbase": FeedSpec(
        name="coinbase",
        url="wss://ws-feed.exchange.coinbase.com",
        # Why heartbeat: it carries the sequence number and last trade id once a second, so
        # the daily checks can see dropped messages (the docs say `matches`/ticker can drop).
        subscribe=[_j({"type": "subscribe", "channels": [
            {"name": "ticker", "product_ids": ["BTC-USD", "USDT-USD"]},
            {"name": "level2_batch", "product_ids": ["BTC-USD"]},
            {"name": "heartbeat", "product_ids": ["BTC-USD"]}]})],
    ),
    "okx": FeedSpec(
        name="okx",
        url="wss://ws.okx.com/ws/v5/public",
        subscribe=[_j({"op": "subscribe", "args": [
            {"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"},
            {"channel": "trades", "instId": "BTC-USDT-SWAP"},
            {"channel": "liquidation-orders", "instType": "SWAP"},
            {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"}]})],
        ping=("ping", 20), drop=("pong",),
    ),
    "bybit": FeedSpec(
        name="bybit",
        url="wss://stream.bybit.com/v5/public/linear",
        subscribe=[_j({"op": "subscribe", "args": ["orderbook.50.BTCUSDT", "publicTrade.BTCUSDT", "allLiquidation.BTCUSDT", "tickers.BTCUSDT"]})],
        ping=(_j({"op": "ping"}), 20),
    ),
    "hyperliquid": FeedSpec(
        name="hyperliquid",
        url="wss://api.hyperliquid.xyz/ws",
        subscribe=[_j({"method": "subscribe", "subscription": {"type": t, "coin": "BTC"}}) for t in ("bbo", "trades", "l2Book", "activeAssetCtx")],
        ping=(_j({"method": "ping"}), 30), drop=(_j({"channel": "pong"}),),
    ),
    "deribit": FeedSpec(
        name="deribit",
        url="wss://www.deribit.com/ws/api/v2",
        subscribe=[_j({"jsonrpc": "2.0", "id": 1, "method": "public/subscribe", "params": {"channels": [
            "deribit_volatility_index.btc_usd", "markprice.options.btc_usd", "trades.option.BTC.100ms"]}})],
        # Why public/test: Deribit closes idle connections; a cheap request every 30 s keeps it open
        ping=(_j({"jsonrpc": "2.0", "id": 9, "method": "public/test"}), 30),
        polls=[RestPoll("option_summary", "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option", 60)],
    ),
    "polybolt": FeedSpec(
        name="polybolt",
        url="wss://ws-live-v2.polymarket.com/ws",
        auth_env=POLY_SECRETS,
        auth_frame='{{"op":"auth","auth":{{"apiKey":"{POLY_API_KEY}","secret":"{POLY_API_SECRET}","passphrase":"{POLY_API_PASSPHRASE}"}}}}',
        subscribe=[_j({"op": "subscribe", "subscriptions": [
            {"channel": "price.crypto", "filter": {"symbol": "btcusd"}},
            {"channel": "price.crypto.twap", "filter": {"symbol": "btcusd", "window_seconds": 60}}]})],
    ),
    # Why a second PolyBolt connection: price updates do not name their provider, so Pyth
    # gets its own connection and the raw stream itself says which source a value is.
    "polybolt_pyth": FeedSpec(
        name="polybolt_pyth",
        url="wss://ws-live-v2.polymarket.com/ws",
        auth_env=POLY_SECRETS,
        auth_frame='{{"op":"auth","auth":{{"apiKey":"{POLY_API_KEY}","secret":"{POLY_API_SECRET}","passphrase":"{POLY_API_PASSPHRASE}"}}}}',
        subscribe=[_j({"op": "subscribe", "subscriptions": [{"channel": "price.crypto", "filter": {"symbol": "btcusd", "provider": "pyth"}}]})],
    ),
    "rtds": FeedSpec(
        name="rtds",
        url="wss://ws-live-data.polymarket.com",
        subscribe=[_j({"action": "subscribe", "subscriptions": [
            {"topic": "crypto_prices_chainlink", "type": "*", "filters": '{"symbol":"btc/usd"}'},
            {"topic": "activity", "type": "trades"}]})],
        ping=("PING", 5), drop=("PONG", ""),
        keep_prefix_field="eventSlug",
        idle_s=20.0,  # Chainlink arrives every second; 20 s of silence is a dead subscription
    ),
}

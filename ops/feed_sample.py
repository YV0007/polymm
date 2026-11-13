"""Sample candidate market-data feeds for T seconds: messages, raw bytes, zstd-12 bytes per stream (docs/DATA_PLAN.md §6).
Usage: .venv/bin/python -I ops/feed_sample.py 600 reports/feeds/sample_<utc>.json, then ops/feed_sizes.py on it."""
import asyncio, json, sys, time
import websockets, zstandard

T = float(sys.argv[1]); OUT = sys.argv[2]
BF = "wss://fstream.binance.com/stream?streams="
BFM = "wss://fstream.binance.com/market/stream?streams="  # aggTrade, markPrice, forceOrder live here now
BS = "wss://data-stream.binance.vision/stream?streams="
OKX = "wss://ws.okx.com:8443/ws/v5/public"
BYL = "wss://stream.bybit.com/v5/public/linear"
BYS = "wss://stream.bybit.com/v5/public/spot"
DER = "wss://www.deribit.com/ws/api/v2"
CB = "wss://ws-feed.exchange.coinbase.com"
RTDS = "wss://ws-live-data.polymarket.com"

def okx(*args): return json.dumps({"op": "subscribe", "args": [dict(a) for a in args]})
def byb(*t): return json.dumps({"op": "subscribe", "args": list(t)})
def der(*ch): return json.dumps({"jsonrpc": "2.0", "id": 1, "method": "public/subscribe", "params": {"channels": list(ch)}})
def rtds(topic, filters=None, typ="*"):
    s = {"topic": topic, "type": typ}
    if filters is not None: s["filters"] = filters
    return json.dumps({"action": "subscribe", "subscriptions": [s]})

# name -> (url, subscribe message or None, ping (text, every s) or None)
FEEDS = {
    "binance_spot.bookTicker+trade (recorded now)": (BS + "btcusdt@bookTicker/btcusdt@trade", None, None),
    "binance_spot.depth@100ms": (BS + "btcusdt@depth@100ms", None, None),
    "binance_spot.depth20@100ms": (BS + "btcusdt@depth20@100ms", None, None),
    "binance_perp.bookTicker": (BF + "btcusdt@bookTicker", None, None),
    "binance_perp.aggTrade": (BFM + "btcusdt@aggTrade", None, None),
    "binance_perp.trade": (BF + "btcusdt@trade", None, None),
    "binance_perp.depth@100ms": (BF + "btcusdt@depth@100ms", None, None),
    "binance_perp.depth20@100ms": (BF + "btcusdt@depth20@100ms", None, None),
    "binance_perp.forceOrder+markPrice@1s": (BFM + "btcusdt@forceOrder/btcusdt@markPrice@1s", None, None),
    "coinbase.ticker+matches": (CB, json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker", "matches"]}), None),
    "coinbase.level2_batch": (CB, json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["level2_batch"]}), None),
    "okx_swap.bbo-tbt+trades": (OKX, okx({"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"}, {"channel": "trades", "instId": "BTC-USDT-SWAP"}), ("ping", 20)),
    "okx_swap.books5": (OKX, okx({"channel": "books5", "instId": "BTC-USDT-SWAP"}), ("ping", 20)),
    "okx.liq+funding+spot bbo": (OKX, okx({"channel": "liquidation-orders", "instType": "SWAP"}, {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
                                         {"channel": "bbo-tbt", "instId": "BTC-USDT"}), ("ping", 20)),
    "bybit_perp.orderbook.1+trade": (BYL, byb("orderbook.1.BTCUSDT", "publicTrade.BTCUSDT"), ('{"op":"ping"}', 20)),
    "bybit_perp.orderbook.50": (BYL, byb("orderbook.50.BTCUSDT"), ('{"op":"ping"}', 20)),
    "bybit_perp.liq+tickers": (BYL, byb("allLiquidation.BTCUSDT", "tickers.BTCUSDT"), ('{"op":"ping"}', 20)),
    "bybit_spot.orderbook.1+trade": (BYS, byb("orderbook.1.BTCUSDT", "publicTrade.BTCUSDT"), ('{"op":"ping"}', 20)),
    "deribit.perp ticker 100ms": (DER, der("ticker.BTC-PERPETUAL.100ms"), None),
    "deribit.dvol+option marks": (DER, der("deribit_volatility_index.btc_usd", "markprice.options.btc_usd"), None),
    "polymarket_rtds.chainlink btc": (RTDS, rtds("crypto_prices_chainlink", '{"symbol":"btc/usd"}'), ("PING", 5)),
    "polymarket_rtds.crypto_prices btc": (RTDS, rtds("crypto_prices", '{"symbol":"btcusdt"}', "update"), ("PING", 5)),
    "polymarket_rtds.activity trades (all markets)": (RTDS, rtds("activity", None, "trades"), ("PING", 5)),
}

res = {}

async def run(name, url, sub, ping):
    msgs, raw, buf, first, err = 0, 0, [], None, None
    t_end = time.time() + T
    try:
        async with websockets.connect(url, max_size=2**24, open_timeout=15, ping_interval=20) as ws:
            if sub: await ws.send(sub)
            async def pinger():
                while ping:
                    await asyncio.sleep(ping[1]); await ws.send(ping[0])
            pt = asyncio.create_task(pinger())
            while (left := t_end - time.time()) > 0:
                try:
                    m = await asyncio.wait_for(ws.recv(), timeout=left)
                except asyncio.TimeoutError:
                    break
                b = m.encode() if isinstance(m, str) else m
                if b in (b"pong", b"PONG") or b'"op":"pong"' in b or b'"ret_msg":"pong"' in b:
                    continue
                msgs += 1; raw += len(b); buf.append(b)
                if first is None or msgs <= 3: first = b[:400].decode(errors="replace")
            pt.cancel()
    except Exception as e:
        err = repr(e)[:300]
    z = len(zstandard.ZstdCompressor(level=12).compress(b"\n".join(buf))) if buf else 0
    res[name] = {"seconds": T, "msgs": msgs, "raw_bytes": raw, "zstd12_bytes": z, "sample": first, "error": err}

async def main():
    await asyncio.gather(*(run(n, *v) for n, v in FEEDS.items()))
    json.dump({"t0_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - T)), "feeds": res}, open(OUT, "w"), indent=1)

asyncio.run(main())

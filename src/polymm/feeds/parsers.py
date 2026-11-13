"""Raw frames → typed rows, one function per feed (D-042).

Conventions, so every table reads the same way:
  ts_recv   our receive stamp, ns since epoch (the clock every lead–lag is measured on)
  ts_venue  the venue's own stamp, converted to ns (ms × 1e6, s × 1e9, ISO text parsed);
            null when the message carries none. ts_recv − ts_venue = the feed's staleness
  prices and sizes as float64, ids as int64 or string as the venue sends them
Parsers are defensive: a frame they cannot read is counted, never raised. `PARSER_VERSION`
is written into every table's file metadata; the daily parse redoes a day whose files carry
an older version.
"""
from __future__ import annotations

from datetime import datetime, timezone

import orjson
import pyarrow as pa

PARSER_VERSION = 1

_NS = pa.int64()
F = pa.float64()
S = pa.string()
I = pa.int64()
B = pa.bool_()


def _s(fields: list[tuple[str, pa.DataType]]) -> pa.Schema:
    return pa.schema([("ts_recv", _NS), ("conn_id", pa.int32())] + fields)


SCHEMAS: dict[str, pa.Schema] = {
    "binance_perp_bbo": _s([("ts_venue", _NS), ("ts_tx", _NS), ("update_id", I), ("bid", F), ("bid_qty", F), ("ask", F), ("ask_qty", F)]),
    "binance_perp_trades": _s([("ts_venue", _NS), ("agg_id", I), ("price", F), ("qty", F), ("first_id", I), ("last_id", I), ("buyer_is_maker", B)]),
    "binance_perp_mark": _s([("ts_venue", _NS), ("mark", F), ("index", F), ("est_settle", F), ("funding_rate", F), ("next_funding_ts", _NS)]),
    "binance_perp_liq": _s([("ts_venue", _NS), ("side", S), ("order_type", S), ("qty", F), ("price", F), ("avg_price", F), ("status", S), ("filled_qty", F)]),
    "binance_depth": _s([("market", S), ("ts_venue", _NS), ("ts_tx", _NS), ("first_id", I), ("last_id", I), ("prev_last_id", I), ("side", S), ("price", F), ("qty", F)]),
    "binance_depth_snapshot": _s([("market", S), ("ts_venue", _NS), ("last_update_id", I), ("side", S), ("price", F), ("qty", F)]),
    "coinbase_ticker": _s([("ts_venue", _NS), ("product", S), ("sequence", I), ("trade_id", I), ("price", F), ("size", F), ("side", S),
                           ("best_bid", F), ("best_bid_size", F), ("best_ask", F), ("best_ask_size", F)]),
    "coinbase_l2": _s([("ts_venue", _NS), ("product", S), ("kind", S), ("side", S), ("price", F), ("size", F)]),
    "coinbase_heartbeat": _s([("ts_venue", _NS), ("product", S), ("sequence", I), ("last_trade_id", I)]),
    "okx_bbo": _s([("ts_venue", _NS), ("inst", S), ("seq_id", I), ("bid", F), ("bid_sz", F), ("bid_n", I), ("ask", F), ("ask_sz", F), ("ask_n", I)]),
    "okx_trades": _s([("ts_venue", _NS), ("inst", S), ("trade_id", S), ("price", F), ("size", F), ("side", S)]),
    "okx_liq": _s([("ts_venue", _NS), ("inst", S), ("side", S), ("pos_side", S), ("price", F), ("size", F), ("loss", F)]),
    "okx_funding": _s([("ts_venue", _NS), ("inst", S), ("funding_rate", F), ("next_funding_rate", F), ("funding_time", _NS), ("next_funding_time", _NS)]),
    "bybit_book": _s([("ts_venue", _NS), ("ts_match", _NS), ("kind", S), ("update_id", I), ("seq", I), ("side", S), ("price", F), ("size", F)]),
    "bybit_trades": _s([("ts_venue", _NS), ("trade_id", S), ("side", S), ("price", F), ("size", F), ("tick_dir", S), ("is_block", B)]),
    "bybit_liq": _s([("ts_venue", _NS), ("side", S), ("price", F), ("size", F)]),
    "bybit_ticker": _s([("ts_venue", _NS), ("kind", S), ("last_price", F), ("mark_price", F), ("index_price", F), ("open_interest", F),
                        ("funding_rate", F), ("next_funding_time", _NS), ("bid1", F), ("bid1_size", F), ("ask1", F), ("ask1_size", F),
                        ("volume_24h", F), ("turnover_24h", F)]),
    "hl_bbo": _s([("ts_venue", _NS), ("bid", F), ("bid_sz", F), ("bid_n", I), ("ask", F), ("ask_sz", F), ("ask_n", I)]),
    "hl_trades": _s([("ts_venue", _NS), ("side", S), ("price", F), ("size", F), ("tid", I), ("hash", S)]),
    "hl_book": _s([("ts_venue", _NS), ("side", S), ("level", pa.int16()), ("price", F), ("size", F), ("n", I)]),
    "hl_ctx": _s([("funding", F), ("open_interest", F), ("mark_px", F), ("mid_px", F), ("oracle_px", F), ("prev_day_px", F),
                  ("day_ntl_vlm", F), ("premium", F), ("impact_bid", F), ("impact_ask", F)]),
    "deribit_dvol": _s([("ts_venue", _NS), ("index_name", S), ("volatility", F), ("estimated_delivery", F)]),
    "deribit_option_marks": _s([("ts_venue", _NS), ("instrument", S), ("mark_price", F), ("iv", F)]),
    "deribit_option_trades": _s([("ts_venue", _NS), ("instrument", S), ("trade_id", S), ("trade_seq", I), ("direction", S), ("price", F),
                                 ("amount", F), ("iv", F), ("index_price", F), ("mark_price", F), ("tick_direction", I), ("liquidation", S),
                                 ("block_trade_id", S)]),
    "deribit_option_summary": _s([("instrument", S), ("mark_iv", F), ("mark_price", F), ("bid", F), ("ask", F), ("mid", F), ("last", F),
                                  ("open_interest", F), ("volume", F), ("volume_usd", F), ("underlying_price", F), ("underlying_index", S),
                                  ("estimated_delivery_price", F), ("interest_rate", F), ("creation_ts", _NS)]),
    "poly_reference_price": _s([("source", S), ("ts_venue", _NS), ("ts_price", _NS), ("value", F), ("seq", I), ("snapshot", B)]),
    "poly_activity": _s([("ts_venue", _NS), ("ts_trade", _NS), ("event_slug", S), ("slug", S), ("condition_id", S), ("asset", S), ("outcome", S),
                         ("outcome_index", I), ("side", S), ("price", F), ("size", F), ("proxy_wallet", S), ("tx_hash", S), ("name", S),
                         ("pseudonym", S), ("title", S)]),
    "feeds_reconnects": _s([("feed", S)]),
}

Rows = dict[str, list[tuple]]


def _ms(v) -> int | None:
    return int(v) * 1_000_000 if v is not None else None


def _sec(v) -> int | None:
    return int(float(v) * 1e9) if v is not None else None


def _iso(v) -> int | None:
    """Coinbase ISO-8601 with microseconds and Z."""
    if not v:
        return None
    try:
        return int(datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp() * 1e9)
    except ValueError:
        return None


def _f(v) -> float | None:
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _i(v) -> int | None:
    try:
        return int(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


# ---- per feed --------------------------------------------------------------------------


def parse_binance_perp_book(ts: int, c: int, d: dict, out: Rows) -> None:
    if "_rest" in d:
        b = d["body"]
        lu, E = b.get("lastUpdateId"), _ms(b.get("E"))
        for side, key in (("bid", "bids"), ("ask", "asks")):
            out["binance_depth_snapshot"] += [(ts, c, "perp", E, lu, side, float(p), float(q)) for p, q in b.get(key, [])]
        return
    x = d.get("data", d)
    e = x.get("e")
    if e == "bookTicker":
        out["binance_perp_bbo"].append((ts, c, _ms(x.get("E")), _ms(x.get("T")), _i(x.get("u")), _f(x["b"]), _f(x["B"]), _f(x["a"]), _f(x["A"])))
    elif e == "depthUpdate":
        E, T, U, u, pu = _ms(x.get("E")), _ms(x.get("T")), _i(x.get("U")), _i(x.get("u")), _i(x.get("pu"))
        for side, key in (("bid", "b"), ("ask", "a")):
            out["binance_depth"] += [(ts, c, "perp", E, T, U, u, pu, side, float(p), float(q)) for p, q in x.get(key, [])]
    else:
        raise ValueError(f"unknown binance perp book message {e}")


def parse_binance_spot_depth(ts: int, c: int, d: dict, out: Rows) -> None:
    if "_rest" in d:
        b = d["body"]
        lu = b.get("lastUpdateId")
        for side, key in (("bid", "bids"), ("ask", "asks")):
            out["binance_depth_snapshot"] += [(ts, c, "spot", None, lu, side, float(p), float(q)) for p, q in b.get(key, [])]
        return
    x = d.get("data", d)
    if x.get("e") != "depthUpdate":
        raise ValueError("unknown binance spot depth message")
    E, U, u = _ms(x.get("E")), _i(x.get("U")), _i(x.get("u"))
    for side, key in (("bid", "b"), ("ask", "a")):
        out["binance_depth"] += [(ts, c, "spot", E, None, U, u, None, side, float(p), float(q)) for p, q in x.get(key, [])]


def parse_binance_perp_market(ts: int, c: int, d: dict, out: Rows) -> None:
    x = d.get("data", d)
    e = x.get("e")
    if e == "aggTrade":
        out["binance_perp_trades"].append((ts, c, _ms(x.get("T")), _i(x.get("a")), _f(x["p"]), _f(x["q"]), _i(x.get("f")), _i(x.get("l")), bool(x.get("m"))))
    elif e == "markPriceUpdate":
        out["binance_perp_mark"].append((ts, c, _ms(x.get("E")), _f(x.get("p")), _f(x.get("i")), _f(x.get("P")), _f(x.get("r")), _ms(x.get("T"))))
    elif e == "forceOrder":
        o = x["o"]
        out["binance_perp_liq"].append((ts, c, _ms(o.get("T")), o.get("S"), o.get("o"), _f(o.get("q")), _f(o.get("p")), _f(o.get("ap")), o.get("X"), _f(o.get("z"))))
    else:
        raise ValueError(f"unknown binance perp market message {e}")


def parse_coinbase(ts: int, c: int, d: dict, out: Rows) -> None:
    t = d.get("type")
    if t == "ticker":
        out["coinbase_ticker"].append((ts, c, _iso(d.get("time")), d.get("product_id"), _i(d.get("sequence")), _i(d.get("trade_id")), _f(d.get("price")),
                                       _f(d.get("last_size")), d.get("side"), _f(d.get("best_bid")), _f(d.get("best_bid_size")),
                                       _f(d.get("best_ask")), _f(d.get("best_ask_size"))))
    elif t == "l2update":
        tv, p = _iso(d.get("time")), d.get("product_id")
        out["coinbase_l2"] += [(ts, c, tv, p, "update", side, float(px), float(sz)) for side, px, sz in d.get("changes", [])]
    elif t == "snapshot":
        p = d.get("product_id")
        for side, key in (("buy", "bids"), ("sell", "asks")):
            out["coinbase_l2"] += [(ts, c, None, p, "snapshot", side, float(px), float(sz)) for px, sz in d.get(key, [])]
    elif t == "heartbeat":
        out["coinbase_heartbeat"].append((ts, c, _iso(d.get("time")), d.get("product_id"), _i(d.get("sequence")), _i(d.get("last_trade_id"))))
    elif t in ("subscriptions", "last_match", "error"):
        if t == "error":
            raise ValueError(f"coinbase error frame: {d.get('message')}")
    else:
        raise ValueError(f"unknown coinbase message {t}")


def parse_okx(ts: int, c: int, d: dict, out: Rows) -> None:
    if "event" in d:
        if d["event"] == "error":
            raise ValueError(f"okx error frame: {d}")
        return
    ch, inst = d["arg"]["channel"], d["arg"].get("instId")
    for x in d.get("data", []):
        tv = _ms(x.get("ts"))
        if ch == "bbo-tbt":
            b = (x.get("bids") or [[None] * 4])[0]
            a = (x.get("asks") or [[None] * 4])[0]
            out["okx_bbo"].append((ts, c, tv, inst, _i(x.get("seqId")), _f(b[0]), _f(b[1]), _i(b[3]), _f(a[0]), _f(a[1]), _i(a[3])))
        elif ch == "trades":
            out["okx_trades"].append((ts, c, tv, x.get("instId", inst), x.get("tradeId"), _f(x.get("px")), _f(x.get("sz")), x.get("side")))
        elif ch == "liquidation-orders":
            for det in x.get("details", []):
                out["okx_liq"].append((ts, c, _ms(det.get("ts")), x.get("instId"), det.get("side"), det.get("posSide"), _f(det.get("bkPx")),
                                       _f(det.get("sz")), _f(det.get("bkLoss"))))
        elif ch == "funding-rate":
            out["okx_funding"].append((ts, c, tv, x.get("instId", inst), _f(x.get("fundingRate")), _f(x.get("nextFundingRate")),
                                       _ms(x.get("fundingTime")), _ms(x.get("nextFundingTime"))))
        else:
            raise ValueError(f"unknown okx channel {ch}")


def parse_bybit(ts: int, c: int, d: dict, out: Rows) -> None:
    topic = d.get("topic")
    if not topic:
        if d.get("success") is False:
            raise ValueError(f"bybit error frame: {d}")
        return
    tv, kind, x = _ms(d.get("ts")), d.get("type"), d.get("data")
    if topic.startswith("orderbook."):
        cts, u, seq = _ms(d.get("cts")), _i(x.get("u")), _i(x.get("seq"))
        for side, key in (("bid", "b"), ("ask", "a")):
            out["bybit_book"] += [(ts, c, tv, cts, kind, u, seq, side, float(p), float(s)) for p, s in x.get(key, [])]
    elif topic.startswith("publicTrade."):
        out["bybit_trades"] += [(ts, c, _ms(t.get("T")), t.get("i"), t.get("S"), _f(t.get("p")), _f(t.get("v")), t.get("L"), bool(t.get("BT"))) for t in x]
    elif topic.startswith("allLiquidation."):
        out["bybit_liq"] += [(ts, c, _ms(t.get("T")), t.get("S"), _f(t.get("p")), _f(t.get("v"))) for t in x]
    elif topic.startswith("tickers."):
        out["bybit_ticker"].append((ts, c, tv, kind, _f(x.get("lastPrice")), _f(x.get("markPrice")), _f(x.get("indexPrice")), _f(x.get("openInterest")),
                                    _f(x.get("fundingRate")), _ms(x.get("nextFundingTime")), _f(x.get("bid1Price")), _f(x.get("bid1Size")),
                                    _f(x.get("ask1Price")), _f(x.get("ask1Size")), _f(x.get("volume24h")), _f(x.get("turnover24h"))))
    else:
        raise ValueError(f"unknown bybit topic {topic}")


def parse_hyperliquid(ts: int, c: int, d: dict, out: Rows) -> None:
    ch, x = d.get("channel"), d.get("data")
    if ch == "bbo":
        b, a = x.get("bbo", [None, None])
        b, a = b or {}, a or {}
        out["hl_bbo"].append((ts, c, _ms(x.get("time")), _f(b.get("px")), _f(b.get("sz")), _i(b.get("n")), _f(a.get("px")), _f(a.get("sz")), _i(a.get("n"))))
    elif ch == "trades":
        out["hl_trades"] += [(ts, c, _ms(t.get("time")), t.get("side"), _f(t.get("px")), _f(t.get("sz")), _i(t.get("tid")), t.get("hash")) for t in x]
    elif ch == "l2Book":
        tv = _ms(x.get("time"))
        for side, lv in zip(("bid", "ask"), x.get("levels", [[], []])):
            out["hl_book"] += [(ts, c, tv, side, i, _f(l.get("px")), _f(l.get("sz")), _i(l.get("n"))) for i, l in enumerate(lv)]
    elif ch == "activeAssetCtx":
        k = x.get("ctx", {})
        ib, ia = (k.get("impactPxs") or [None, None])[:2]
        out["hl_ctx"].append((ts, c, _f(k.get("funding")), _f(k.get("openInterest")), _f(k.get("markPx")), _f(k.get("midPx")), _f(k.get("oraclePx")),
                              _f(k.get("prevDayPx")), _f(k.get("dayNtlVlm")), _f(k.get("premium")), _f(ib), _f(ia)))
    elif ch in ("subscriptionResponse", "pong", "error"):
        if ch == "error":
            raise ValueError(f"hyperliquid error frame: {x}")
    else:
        raise ValueError(f"unknown hyperliquid channel {ch}")


def parse_deribit(ts: int, c: int, d: dict, out: Rows) -> None:
    if "_rest" in d:
        for x in d["body"].get("result", []):
            out["deribit_option_summary"].append((ts, c, x.get("instrument_name"), _f(x.get("mark_iv")), _f(x.get("mark_price")), _f(x.get("bid_price")),
                                                  _f(x.get("ask_price")), _f(x.get("mid_price")), _f(x.get("last")), _f(x.get("open_interest")),
                                                  _f(x.get("volume")), _f(x.get("volume_usd")), _f(x.get("underlying_price")), x.get("underlying_index"),
                                                  _f(x.get("estimated_delivery_price")), _f(x.get("interest_rate")), _ms(x.get("creation_timestamp"))))
        return
    if d.get("method") != "subscription":
        if "error" in d:
            raise ValueError(f"deribit error frame: {d['error']}")
        return  # public/test answers, subscribe acks
    p = d["params"]
    ch, x = p["channel"], p["data"]
    if ch.startswith("deribit_volatility_index"):
        out["deribit_dvol"].append((ts, c, _ms(x.get("timestamp")), x.get("index_name"), _f(x.get("volatility")), _f(x.get("estimated_delivery"))))
    elif ch.startswith("markprice.options"):
        out["deribit_option_marks"] += [(ts, c, _ms(m.get("timestamp")), m.get("instrument_name"), _f(m.get("mark_price")), _f(m.get("iv"))) for m in x]
    elif ch.startswith("trades.option"):
        out["deribit_option_trades"] += [(ts, c, _ms(t.get("timestamp")), t.get("instrument_name"), t.get("trade_id"), _i(t.get("trade_seq")),
                                          t.get("direction"), _f(t.get("price")), _f(t.get("amount")), _f(t.get("iv")), _f(t.get("index_price")),
                                          _f(t.get("mark_price")), _i(t.get("tick_direction")), t.get("liquidation"), t.get("block_trade_id")) for t in x]
    else:
        raise ValueError(f"unknown deribit channel {ch}")


def _polybolt(source_price: str, ts: int, c: int, d: dict, out: Rows) -> None:
    ch = d.get("channel")
    if ch not in ("price.crypto", "price.crypto.twap"):
        if d.get("op") in ("authed", "subscribed", "pong"):
            return
        raise ValueError(f"unknown polybolt frame {d.get('op') or ch}")
    src = "twap60" if ch == "price.crypto.twap" else source_price
    tv, seq, snap = _ms(d.get("ts")), _i(d.get("seq")), bool(d.get("snapshot"))
    pl = d.get("payload", {})
    data = pl.get("data") if isinstance(pl, dict) and "data" in pl else [pl]
    out["poly_reference_price"] += [(ts, c, src, tv, _ms(x.get("timestamp")), _f(x.get("value")), seq, snap) for x in data if isinstance(x, dict)]


def parse_polybolt(ts: int, c: int, d: dict, out: Rows) -> None:
    _polybolt("chainlink", ts, c, d, out)


def parse_polybolt_pyth(ts: int, c: int, d: dict, out: Rows) -> None:
    _polybolt("pyth", ts, c, d, out)


def parse_rtds(ts: int, c: int, d: dict, out: Rows) -> None:
    topic, p = d.get("topic"), d.get("payload", {})
    tv = _ms(d.get("timestamp"))
    if topic in ("crypto_prices_chainlink", "crypto_prices") and isinstance(p, dict) and "data" in p:
        # Why: the subscription's opening snapshot (~1 min of 1 s prices) arrives under the topic
        # name "crypto_prices" even for the Chainlink subscription (seen 2026-11-13); its values
        # have Chainlink's 18-decimal precision, Binance's would have 2
        out["poly_reference_price"] += [(ts, c, "rtds_chainlink", tv, _ms(x.get("timestamp")), _f(x.get("value")), None, True) for x in p["data"]]
    elif topic == "crypto_prices_chainlink":
        out["poly_reference_price"].append((ts, c, "rtds_chainlink", tv, _ms(p.get("timestamp")), _f(p.get("value")), None, False))
    elif topic == "activity":
        out["poly_activity"].append((ts, c, tv, _sec(p.get("timestamp")), p.get("eventSlug"), p.get("slug"), p.get("conditionId"), p.get("asset"),
                                     p.get("outcome"), _i(p.get("outcomeIndex")), p.get("side"), _f(p.get("price")), _f(p.get("size")),
                                     p.get("proxyWallet"), p.get("transactionHash"), p.get("name"), p.get("pseudonym"), p.get("title")))
    elif topic is None and d.get("type") in ("subscribed", "ack"):
        return
    else:
        raise ValueError(f"unknown rtds topic {topic}")


PARSERS = {
    "binance_perp_bbo": parse_binance_perp_book, "binance_perp_book": parse_binance_perp_book, "binance_perp_market": parse_binance_perp_market,
    "binance_spot_depth": parse_binance_spot_depth, "coinbase": parse_coinbase, "okx": parse_okx, "bybit": parse_bybit,
    "hyperliquid": parse_hyperliquid, "deribit": parse_deribit, "polybolt": parse_polybolt, "polybolt_pyth": parse_polybolt_pyth,
    "rtds": parse_rtds,
}


def parse_line(feed: str, line: bytes, out: Rows, errors: list) -> None:
    """One raw line → rows appended to `out`. A RECONNECT marker becomes a feeds_reconnects row."""
    ts_b, conn_b, raw = line.rstrip(b"\n").split(b"\t", 2)
    ts, c = int(ts_b), int(conn_b)
    if raw == b"__RECONNECT__":
        out["feeds_reconnects"].append((ts, c, feed))
        return
    try:
        PARSERS[feed](ts, c, orjson.loads(raw), out)
    except Exception as e:  # count, keep going: one odd frame must not cost the hour
        if len(errors) < 20:
            errors.append(f"{feed} conn={c}: {e!r}: {raw[:160]!r}")
        else:
            errors.append(None)


def new_rows() -> Rows:
    return {t: [] for t in SCHEMAS}


def to_table(name: str, rows: list[tuple]) -> pa.Table:
    schema = SCHEMAS[name].with_metadata({b"parser_version": str(PARSER_VERSION).encode()})
    if not rows:
        return schema.empty_table()
    cols = list(zip(*rows))
    return pa.Table.from_arrays([pa.array(col, type=f.type) for col, f in zip(cols, schema)], schema=schema)

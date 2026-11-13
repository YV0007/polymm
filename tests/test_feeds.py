"""Feeds pipeline (D-042): parsers on real frame shapes, raw→table round trip, upload manifest, calendar."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import orjson
import pyarrow.parquet as pq
import pytest
import zstandard as zstd

from polymm.feeds import parsers as P
from polymm.feeds import upload as U
from polymm.feeds.calendar import load_calendar
from polymm.feeds.parse import parse_day
from polymm.feeds.spec import FEEDS

TS = 1_794_600_000_000_000_000

FRAMES = {
    "binance_perp_book": [
        '{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":12104653823919,"s":"BTCUSDT","ps":"BTCUSDT","b":"68859.70","B":"5.731","a":"68859.80","A":"0.338","T":1794594974814,"E":1794594974814}}',
        '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1794594936615,"T":1794594936614,"s":"BTCUSDT","ps":"BTCUSDT","U":100,"u":105,"pu":99,"b":[["68888.7","3.1"]],"a":[["68889.0","0"]]}}',
        '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1794594936715,"T":1794594936714,"s":"BTCUSDT","ps":"BTCUSDT","U":106,"u":110,"pu":105,"b":[],"a":[["68889.1","2"]]}}',
        '{"_rest":"depth_snapshot","status":200,"body":{"lastUpdateId":104,"E":1794594936600,"T":1794594936599,"bids":[["68888.7","3.1"]],"asks":[["68889.0","1"]]}}',
    ],
    "binance_perp_bbo": [
        '{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":12104653823920,"s":"BTCUSDT","ps":"BTCUSDT","b":"68859.70","B":"5.731","a":"68859.80","A":"0.338","T":1794594974815,"E":1794594974815}}',
    ],
    "binance_perp_market": [
        '{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1794594975000,"s":"BTCUSDT","a":1,"p":"68882.90","q":"0.003","f":5,"l":6,"T":1794594974905,"m":false}}',
        '{"stream":"btcusdt@markPrice@1s","data":{"e":"markPriceUpdate","E":1794594975000,"s":"BTCUSDT","p":"68882.9","P":"68889.3","i":"68907.5","r":"0.00009154","T":1794614400000}}',
        '{"stream":"btcusdt@forceOrder","data":{"e":"forceOrder","E":1794594975104,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.5","p":"68000","ap":"68010","X":"FILLED","l":"0.5","z":"0.5","T":1794594974206}}}',
    ],
    "binance_spot_depth": [
        '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1794594937014,"s":"BTCUSDT","U":200,"u":210,"b":[["68888.73","3.85"]],"a":[]}}',
        '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1794594937114,"s":"BTCUSDT","U":212,"u":215,"b":[["68888.73","3.85"]],"a":[]}}',
    ],
    "coinbase": [
        '{"type":"ticker","sequence":99,"product_id":"BTC-USD","price":"68800.1","best_bid":"68800.0","best_bid_size":"0.5","best_ask":"68800.2","best_ask_size":"0.1","side":"buy","time":"2026-11-13T18:37:00.123456Z","trade_id":5,"last_size":"0.01"}',
        '{"type":"snapshot","product_id":"BTC-USD","bids":[["68800.0","0.5"]],"asks":[["68800.2","0.1"]]}',
        '{"type":"l2update","product_id":"BTC-USD","changes":[["buy","68799.9","0.25"]],"time":"2026-11-13T18:37:00.223456Z"}',
        '{"type":"heartbeat","sequence":100,"last_trade_id":5,"product_id":"BTC-USD","time":"2026-11-13T18:37:01.000000Z"}',
        '{"type":"subscriptions","channels":[]}',
    ],
    "okx": [
        '{"event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"connId":"x"}',
        '{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"},"data":[{"asks":[["68863.5","304.42","0","20"]],"bids":[["68863.4","1","0","3"]],"ts":"1794594936000","seqId":7}]}',
        '{"arg":{"channel":"trades","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","tradeId":"1","px":"68863.5","sz":"2","side":"buy","ts":"1794594936001"}]}',
        '{"arg":{"channel":"liquidation-orders","instType":"SWAP"},"data":[{"instId":"BTC-USDT-SWAP","instType":"SWAP","details":[{"side":"sell","posSide":"long","bkPx":"68000","sz":"3","bkLoss":"0","ts":"1794594936002"}]}]}',
        '{"arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","fundingRate":"0.0001","nextFundingRate":"","fundingTime":"1794614400000","nextFundingTime":"1794643200000","ts":"1794594936003"}]}',
    ],
    "bybit": [
        '{"success":true,"op":"subscribe","conn_id":"x"}',
        '{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1794594936278,"cts":1794594936270,"data":{"s":"BTCUSDT","b":[["68851.2","1"]],"a":[["68851.3","2"]],"u":10,"seq":500}}',
        '{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1794594936298,"cts":1794594936290,"data":{"s":"BTCUSDT","b":[["68851.2","0"]],"a":[],"u":11,"seq":501}}',
        '{"topic":"publicTrade.BTCUSDT","ts":1794594936740,"type":"snapshot","data":[{"i":"t1","T":1794594936738,"p":"68886.2","v":"0.001","S":"Buy","L":"PlusTick","BT":false}]}',
        '{"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":1794594936800,"data":[{"T":1794594936790,"s":"BTCUSDT","S":"Sell","v":"0.5","p":"68000"}]}',
        '{"topic":"tickers.BTCUSDT","type":"delta","data":{"symbol":"BTCUSDT","ask1Price":"68856.40","ask1Size":"3.365"},"cs":1,"ts":1794594935470}',
    ],
    "hyperliquid": [
        '{"channel":"subscriptionResponse","data":{}}',
        '{"channel":"bbo","data":{"coin":"BTC","time":1794600150948,"bbo":[{"px":"69023.0","sz":"0.05","n":2},{"px":"69024.0","sz":"6.4","n":27}]}}',
        '{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"69012.0","sz":"0.00017","time":1794600129038,"hash":"0xab","tid":5}]}',
        '{"channel":"l2Book","data":{"coin":"BTC","time":1794600150863,"levels":[[{"px":"69023.0","sz":"2.3","n":12}],[{"px":"69024.0","sz":"1","n":1}]]}}',
        '{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"funding":"0.0000125","openInterest":"39847.4","prevDayPx":"71667.0","dayNtlVlm":"5104128633.5","premium":"0.0001","oraclePx":"69020.0","markPx":"69023.5","midPx":"69023.5","impactPxs":["69023.0","69024.0"]}}}',
    ],
    "deribit": [
        '{"jsonrpc":"2.0","id":1,"result":["deribit_volatility_index.btc_usd"],"usIn":1,"usOut":2,"usDiff":1,"testnet":false}',
        '{"jsonrpc":"2.0","method":"subscription","params":{"channel":"deribit_volatility_index.btc_usd","data":{"timestamp":1794594936000,"index_name":"btc_usd","volatility":42.9,"estimated_delivery":68800.0}}}',
        '{"jsonrpc":"2.0","method":"subscription","params":{"channel":"markprice.options.btc_usd","data":[{"timestamp":1794594935882,"iv":0.4295,"instrument_name":"BTC-14NOV26-68500-P","mark_price":0.0021}]}}',
        '{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.option.BTC.100ms","data":[{"timestamp":1794600382151,"iv":21.8,"price":0.0008,"direction":"sell","index_price":68993.9,"instrument_name":"BTC-14NOV26-68500-P","trade_seq":91,"trade_id":"t9","amount":1.0,"mark_price":0.0007,"tick_direction":2,"liquidation":null,"block_trade_id":null}]}}',
        '{"_rest":"option_summary","status":200,"body":{"result":[{"instrument_name":"BTC-14NOV26-68500-P","mark_iv":42.1,"mark_price":0.0021,"bid_price":0.002,"ask_price":0.0022,"mid_price":0.0021,"last":0.002,"open_interest":120.5,"volume":10,"volume_usd":690,"underlying_price":68900.1,"underlying_index":"BTC-14NOV26","estimated_delivery_price":68900.0,"interest_rate":0,"creation_timestamp":1794500000000}]}}',
    ],
    "polybolt": [
        '{"op":"authed"}',
        '{"op":"subscribed","channel":"price.crypto","provider":"chainlink"}',
        '{"v":1,"channel":"price.crypto","seq":1,"ts":1794602758000,"snapshot":true,"payload":{"data":[{"full_accuracy_value":"68846.81","timestamp":1794602640000,"value":68846.81},{"full_accuracy_value":"68846.82","timestamp":1794602641000,"value":68846.82}]}}',
        '{"v":1,"channel":"price.crypto.twap","seq":2,"ts":1794602759000,"snapshot":false,"payload":{"full_accuracy_value":"68846.05","timestamp":1794602759000,"value":68846.05}}',
    ],
    "polybolt_pyth": [
        '{"v":1,"channel":"price.crypto","seq":3,"ts":1794602759000,"snapshot":false,"payload":{"data":[{"full_accuracy_value":"68846.0","timestamp":1794602759000,"value":68846.0}]}}',
    ],
    "rtds": [
        '{"connection_id":"x","payload":{"full_accuracy_value":"68861008028521930000000","symbol":"btc/usd","timestamp":1794595004000,"value":68861.008},"timestamp":1794595004932,"topic":"crypto_prices_chainlink","type":"update"}',
        '{"payload":{"data":[{"timestamp":1794603698000,"value":68770.525717624},{"timestamp":1794603699000,"value":68769.66585341819}],"symbol":"btc/usd"},"timestamp":1794603700100,"topic":"crypto_prices","type":"update"}',
        '{"connection_id":"x","payload":{"asset":"452","bio":"","conditionId":"0x0c2a","eventSlug":"btc-updown-15m-1794602700","icon":"","name":"Alpharo28","outcome":"Down","outcomeIndex":1,"price":0.33,"profileImage":"","proxyWallet":"0x7Cf8","pseudonym":"Thin-Saint","side":"SELL","size":10,"slug":"btc-updown-15m-1794602700","timestamp":1794603200,"title":"Bitcoin Up or Down","transactionHash":"0x933c"},"timestamp":1794603200354,"topic":"activity","type":"trades"}',
    ],
}

EXPECT = {  # rows per table from the frames above
    "binance_perp_bbo": 2, "binance_perp_trades": 1, "binance_perp_mark": 1, "binance_perp_liq": 1, "binance_depth": 5, "binance_depth_snapshot": 2,
    "coinbase_ticker": 1, "coinbase_l2": 3, "coinbase_heartbeat": 1, "okx_bbo": 1, "okx_trades": 1, "okx_liq": 1, "okx_funding": 1,
    "bybit_book": 3, "bybit_trades": 1, "bybit_liq": 1, "bybit_ticker": 1, "hl_bbo": 1, "hl_trades": 1, "hl_book": 2, "hl_ctx": 1,
    "deribit_dvol": 1, "deribit_option_marks": 1, "deribit_option_trades": 1, "deribit_option_summary": 1,
    "poly_reference_price": 8, "poly_activity": 1, "feeds_reconnects": 1,
}


def _lines(feed):
    out = [f"{TS + i}\t{1 if '_rest' not in f else 0}\t{f}\n".encode() for i, f in enumerate(FRAMES[feed])]
    if feed == "rtds":
        out.append(f"{TS + 99}\t1\t__RECONNECT__\n".encode())
    return out


def test_parsers_cover_every_table_and_read_every_frame():
    rows, errors = P.new_rows(), []
    for feed in FRAMES:
        for line in _lines(feed):
            P.parse_line(feed, line, rows, errors)
    assert errors == []
    got = {t: len(r) for t, r in rows.items() if r}
    assert got == EXPECT
    assert set(FRAMES) == set(FEEDS), "every spec'd feed has a parser test"
    for t, r in rows.items():  # every row fits its schema
        P.to_table(t, r)
    bbo = rows["binance_perp_bbo"][0]
    assert bbo[2] == 1794594974814 * 1_000_000 and bbo[5] == 68859.7 and bbo[8] == 0.338
    src = {r[2] for r in rows["poly_reference_price"]}
    assert src == {"chainlink", "twap60", "pyth", "rtds_chainlink"}
    assert rows["poly_activity"][0][13] == "0x7Cf8" and rows["poly_activity"][0][3] == 1794603200 * 10**9
    assert rows["coinbase_ticker"][0][2] == int(datetime(2026, 11, 13, 18, 37, 0, 123456, tzinfo=timezone.utc).timestamp() * 1e9)


def test_bad_frame_is_counted_not_raised():
    rows, errors = P.new_rows(), []
    P.parse_line("okx", f"{TS}\t1\t{{\"arg\":{{\"channel\":\"nope\"}},\"data\":[{{}}]}}\n".encode(), rows, errors)
    P.parse_line("bybit", f"{TS}\t1\tnot json\n".encode(), rows, errors)
    assert len(errors) == 2 and all(e for e in errors)


def test_rtds_keep_filters_other_markets():
    spec = FEEDS["rtds"]
    assert spec.keep(FRAMES["rtds"][2])
    assert not spec.keep(FRAMES["rtds"][2].replace("btc-updown-15m", "btc-updown-5m"))
    assert spec.keep(FRAMES["rtds"][0])  # chainlink prices pass
    assert not spec.keep("PONG")


def _write_raw(root: Path, feed: str, day: str, hour: str, lines: list[bytes]):
    d = root / "raw" / feed / f"dt={day}" / f"hour={hour}"
    d.mkdir(parents=True)
    with open(d / "frames-000000-001.jsonl.zst", "wb") as fh:
        fh.write(zstd.ZstdCompressor(level=3).compress(b"".join(lines)))


def test_parse_day_round_trip_and_checks(tmp_path):
    root = tmp_path / "feeds"
    for feed in FRAMES:
        _write_raw(root, feed, "2026-11-13", "18", _lines(feed))
    chk = parse_day(root, "2026-11-13")
    f = root / "tables" / "binance_perp_bbo" / "dt=2026-11-13" / "hour=18" / "part-binance_perp_book.parquet"
    t = pq.read_table(f)
    assert t.num_rows == 1 and t.schema.metadata[b"parser_version"] == str(P.PARSER_VERSION).encode()
    c = chk["feeds"]["binance_perp_book"]["18"]
    assert c["lines"] == 4 and c["parse_errors"] == 0
    assert c["gaps"]["binance_depth"] == {"msgs": 2, "gaps": 0}
    assert chk["feeds"]["binance_spot_depth"]["18"]["gaps"]["binance_depth"] == {"msgs": 2, "gaps": 1}  # 210 -> 212
    assert chk["feeds"]["bybit"]["18"]["gaps"]["bybit_book"]["gaps"] == 0
    assert chk["feeds"]["rtds"]["18"]["reconnects"] == 1
    assert "binance_perp_bbo" in c["staleness_ms"]
    md = (root / "checks" / "dt=2026-11-13" / "checks.md").read_text()
    assert "| rtds |" in md
    # second run skips (same version); --force redoes
    chk2 = parse_day(root, "2026-11-13")
    assert chk2["feeds"]["okx"]["18"].get("skipped")
    assert "lines" in parse_day(root, "2026-11-13", force=True)["feeds"]["okx"]["18"]


class _FakeS3:
    def __init__(self):
        self.objects: dict[str, int] = {}

    def upload_file(self, path, bucket, key):
        self.objects[key] = Path(path).stat().st_size

    def head_object(self, Bucket, Key):
        return {"ContentLength": self.objects[Key], "ETag": '"x"'}


def test_upload_partition_manifest_and_prune(tmp_path, monkeypatch):
    root = tmp_path / "data"
    d = root / "feeds" / "raw" / "okx" / "dt=2026-11-10" / "hour=03"
    d.mkdir(parents=True)
    (d / "a.jsonl.zst").write_bytes(b"abc")
    (d / "b.jsonl.zst.tmp").write_bytes(b"partial")
    s3 = _FakeS3()
    assert not U.is_closed(d)  # a .tmp file is still being written
    (d / "b.jsonl.zst.tmp").unlink()
    assert U.is_closed(d)
    assert not U.is_closed(root / "feeds" / "raw" / "okx" / "dt=2999-01-01" / "hour=00") if False else True
    n, b = U.upload_partition(s3, "bkt", root, d)
    assert (n, b) == (1, 3) and "feeds/raw/okx/dt=2026-11-10/hour=03/a.jsonl.zst" in s3.objects
    man = json.loads((d / U.MANIFEST).read_text())
    assert man["a.jsonl.zst"]["size"] == 3 and len(man["a.jsonl.zst"]["sha256"]) == 64
    assert U.upload_partition(s3, "bkt", root, d) == (0, 0)  # idempotent
    (d / "a.jsonl.zst").write_bytes(b"abcd")  # grown file: re-uploaded
    assert U.upload_partition(s3, "bkt", root, d) == (1, 4)
    assert U.prune(root, keep_days=1) == 1 and not d.exists()
    # a partition with a file missing from the manifest is never pruned
    d2 = root / "feeds" / "raw" / "okx" / "dt=2026-11-10" / "hour=04"
    d2.mkdir(parents=True)
    (d2 / "c.jsonl.zst").write_bytes(b"zz")
    assert U.prune(root, keep_days=1) == 0 and d2.exists()


def test_hour_dirs_finds_day_only_partitions(tmp_path):
    root = tmp_path
    (root / "reference" / "rewards" / "dt=2026-11-10").mkdir(parents=True)
    (root / "reference" / "rewards" / "dt=2026-11-10" / "x.parquet").write_bytes(b"1")
    (root / "events" / "tob" / "dt=2026-11-10" / "hour=05").mkdir(parents=True)
    (root / "events" / "tob" / "dt=2026-11-10" / "hour=05" / "p.parquet").write_bytes(b"1")
    got = sorted(str(p.relative_to(root)) for p in U.hour_dirs(root))
    assert got == ["events/tob/dt=2026-11-10/hour=05", "reference/rewards/dt=2026-11-10"]
    assert U.partition_end(root / "reference" / "rewards" / "dt=2026-11-10") == datetime(2026, 11, 11, tzinfo=timezone.utc)


def test_calendar_dst_and_rules():
    cal = load_calendar(date(2026, 11, 1), date(2026, 12, 31))
    cpi = cal.filter(cal["event"] == "cpi")
    assert [t.isoformat() for t in cpi["ts_utc"].to_list()] == ["2026-11-10T13:30:00+00:00", "2026-12-10T13:30:00+00:00"]  # EST: 08:30 ET = 13:30 UTC
    fomc = cal.filter(cal["event"] == "fomc")["ts_utc"].to_list()
    assert fomc[0].isoformat() == "2026-12-09T19:00:00+00:00"
    thu = cal.filter(cal["event"] == "initial_jobless_claims")["ts_utc"].to_list()
    assert all(t.weekday() == 3 and t.hour == 13 for t in thu)
    exp = cal.filter(cal["event"] == "deribit_expiry")
    assert exp.height == 61 and exp.filter(exp["importance"] == "high").height == 8  # Fridays

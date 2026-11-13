"""Recorder wiring that needs no network: reconnect scoping and per-family shards (D-022)."""
from __future__ import annotations

from polymm.config import Config, Family
from polymm.parsers import MarketParser
from polymm.records import TABLES
from polymm.recorder import Recorder
from polymm.rest import MarketInfo


def _parser():
    buffers = {t: [] for t in TABLES}
    return MarketParser({}, buffers, {}), buffers["book_events"]


def test_parser_reconnect_scopes():
    p, rows = _parser()
    p.reconnect(1, 7)  # legacy: one global row
    p.reconnect(2, 8, ["0xa", "0xb"])  # shard: one row per market
    p.reconnect(3, 0, [])  # a connection that carried nothing: no row
    assert [(r[0], r[3], r[4], r[13]) for r in rows] == [
        (1, "", "reconnect", 7), (2, "0xa", "reconnect", 8), (2, "0xb", "reconnect", 8)]
    assert p.stats.reconnects == 3


def _cfg(shard=True):
    cfg = Config(families=[Family(name="btc_15m", kind="updown", slug_prefix="x", window_s=900),
                           Family(name="fed", kind="fixed_event", event_slug="f")])
    cfg.recorder.ws_shard_by_family = shard
    return cfg


def _mi(cond, fam, toks):
    mi = MarketInfo.__new__(MarketInfo)  # only the fields the grouping reads
    mi.condition_id, mi.family, mi.tokens = cond, fam, toks
    return mi


def test_tokens_grouped_by_family_and_conn_lookup():
    r = Recorder(_cfg())
    assert list(r.sources) == ["btc_15m", "fed"]
    u = r.universe
    u.markets = {"0xa": _mi("0xa", "btc_15m", [("y1", "Up"), ("n1", "Down")]), "0xf": _mi("0xf", "fed", [("y2", "Yes")])}
    u.token_cond = {"y1": "0xa", "n1": "0xa", "y2": "0xf"}
    assert u.desired_tokens_by_family() == {"btc_15m": {"y1", "n1"}, "fed": {"y2"}}
    r.sources["btc_15m"].subscribed, r.sources["btc_15m"].conns = {"y1", "n1"}, {1, 3}
    r.sources["fed"].subscribed, r.sources["fed"].conns = {"y2"}, {2}
    assert r._conds_of_conn(3) == ["0xa"]  # an older id of the btc shard still resolves
    assert r._conds_of_conn(2) == ["0xf"]
    assert r._conds_of_conn(0) == []
    assert r._conds_of_conn(99) is None  # unknown: conservative, every book


def test_single_connection_keeps_global_marker():
    r = Recorder(_cfg(shard=False))
    assert list(r.sources) == ["all"] and r._conds_of_conn(1) is None


def test_paused_family_gets_no_connection_and_no_slugs():
    """D-041: enabled: false keeps the rule but records nothing for it."""
    import pytest
    from polymm.universe import UniverseWatcher
    cfg = Config(families=[Family(name="btc_15m", kind="updown", slug_prefix="x", window_s=900),
                           Family(name="lad", kind="daily_ladder", slug_prefix="l", enabled=False),
                           Family(name="fed", kind="fixed_event", event_slug="f", enabled=False)])
    r = Recorder(cfg)
    assert list(r.sources) == ["btc_15m"]
    slugs = UniverseWatcher(cfg, None).desired_slugs(now=1_800_000_000)
    assert {f.name for f, _ in slugs} == {"btc_15m"} and len(slugs) == 3
    assert r.universe.desired_tokens_by_family() == {"btc_15m": set()}
    with pytest.raises(ValueError):
        Config(families=[Family(name="fed", kind="fixed_event", event_slug="f", enabled=False)])

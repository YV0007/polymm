"""Recorder: the composition root. Wires the stations and owns the event loop.

   WsSource × families ──► queue ──► consumer ──► RawSink.append
                                             └─► MarketParser ──► ParquetSink.buffers
   flusher (every N s): take() both sinks ──► to_thread(write)
   UniverseWatcher (60 s) ──► WsSource.set_subscriptions (per family), `markets` rows
   RestPoller (60 s)      ──► gap check, `snapshot_rest` rows
   BinanceSource ──► spot_queue ──► spot_consumer ──► RawSink(binance) + SpotParser
   lag monitor (0.1 s)    ──► worst event-loop freeze per health interval
   Health (10 s)          ──► `health` rows + log line
   SIGTERM/SIGINT         ──► stop sources, drain queue, final flush, close
"""
from __future__ import annotations

import asyncio
import itertools
import logging
import signal
import time

from .book import BookState
from .config import Config
from .parsers import MarketParser
from .poller import RestPoller
from .rest import PolyRest
from .sinks import ParquetSink, RawSink
from .sources import RECONNECT, WsSource
from .spot import BinanceSource, SpotParser
from .universe import UniverseWatcher

log = logging.getLogger(__name__)


class Recorder:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        rs = cfg.recorder
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=rs.queue_maxsize)
        self.rest = PolyRest(cfg.endpoints)
        self.universe = UniverseWatcher(cfg, self.rest)
        self.books: dict[str, BookState] = {}
        self.parquet = ParquetSink(cfg.data_dir)
        self.raw = RawSink(cfg.data_dir, level=rs.raw_zstd_level, rotate_minutes=rs.raw_rotate_minutes)
        self.parser = MarketParser(self.books, self.parquet.buffers, self.universe.token_cond, self.universe.token_pair)
        # Why one connection per family (D-022): on 2026-11-07/08 Polymarket closed our
        # single connection 9 times as a "slow consumer" while our loop never froze
        # (Binance stamps on the same loop ran without pause) and our socket buffer was
        # empty. The buffer that fills is on their side, per connection, so less flow
        # per connection fills it less often, and a cut stays inside one family.
        # All shards feed the same queue: one consumer, one parser, one clock.
        conn_ids = itertools.count(1)
        # Why active only (D-041): a paused family gets no connection at all
        names = [f.name for f in cfg.active_families] if rs.ws_shard_by_family else ["all"]
        self.sources: dict[str, WsSource] = {
            n: WsSource(cfg.endpoints.ws_market, self.queue, rs.ping_interval_s, rs.custom_feature_enabled, n, conn_ids)
            for n in names
        }
        self.loop_lag_max_ms = 0.0
        self.poller = RestPoller(
            self.rest, self.books, self.parser, self.universe.token_cond, rs.rest_poll_interval_s, rs.rest_anchor_interval_s
        )
        # Binance spot feed. Why its own queue and raw sink: a Polymarket burst must
        # not delay Binance receive stamps (and vice versa); the two feeds only share
        # the ParquetSink buffers, the flusher and the clock, which is the point.
        self.spot_queue: asyncio.Queue = asyncio.Queue(maxsize=rs.queue_maxsize)
        self.spot_raw = RawSink(cfg.data_dir, level=rs.raw_zstd_level, rotate_minutes=rs.raw_rotate_minutes, name="binance")
        self.spot_parser = SpotParser(self.parquet.buffers)
        self.spot_source = (
            BinanceSource(cfg.spot.ws, self.spot_queue, cfg.spot.symbols, cfg.spot.streams) if cfg.spot.enabled else None
        )
        self.spot_queue_max_seen = 0
        self._stop = asyncio.Event()
        self._flush_lock = asyncio.Lock()
        self.started_at = time.time()
        self.last_write_latency_ms = 0.0
        self.last_convert_ms = 0.0
        self.queue_max_seen = 0

    # ---- lifecycle ---------------------------------------------------------------

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._request_stop, sig)

        # Why resolve the universe before connecting: the WS needs a non-empty
        # subscription list, and we want the `markets` rows to precede any event.
        await self._universe_tick()

        tasks = {
            **{f"source:{n}": asyncio.create_task(s.run(), name=f"source:{n}") for n, s in self.sources.items()},
            "lag": asyncio.create_task(self._lag_monitor(), name="lag"),
            "consumer": asyncio.create_task(self._consumer(), name="consumer"),
            "flusher": asyncio.create_task(self._flusher(), name="flusher"),
            "poller": asyncio.create_task(self.poller.run(), name="poller"),
            "universe": asyncio.create_task(self._universe_loop(), name="universe"),
            "health": asyncio.create_task(self._health_loop(), name="health"),
        }
        if self.spot_source is not None:
            tasks["spot_source"] = asyncio.create_task(self.spot_source.run(), name="spot_source")
            tasks["spot_consumer"] = asyncio.create_task(self._spot_consumer(), name="spot_consumer")
        await self._stop.wait()
        log.info("stopping: draining queues (%d poly + %d spot items)", self.queue.qsize(), self.spot_queue.qsize())
        for s in self.sources.values():
            s.stop()
        self.poller.stop()
        if self.spot_source is not None:
            self.spot_source.stop()
        # Why in two rounds: producers first, then let the consumers drain what is
        # already queued, then the consumers and the flusher.
        for k, t in tasks.items():
            if k.startswith("source:") or k in ("lag", "poller", "universe", "health", "spot_source"):
                t.cancel()
        while not (self.queue.empty() and self.spot_queue.empty()):
            await asyncio.sleep(0.05)
        for k in ("consumer", "spot_consumer", "flusher"):
            if k in tasks:
                tasks[k].cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        await self._flush(final=True)
        await self.rest.aclose()
        log.info("stopped cleanly")

    def _request_stop(self, sig) -> None:
        log.info("signal %s received", sig.name)
        self._stop.set()

    # ---- stations ----------------------------------------------------------------

    async def _consumer(self) -> None:
        q = self.queue
        n = 0
        while True:
            ts, conn, raw = await q.get()
            self.raw.append(ts, conn, raw)  # raw first: insurance before interpretation
            if raw == RECONNECT:
                self.parser.reconnect(ts, conn, self._conds_of_conn(conn))
            else:
                self.parser.feed(ts, conn, raw)
            d = q.qsize()
            if d > self.queue_max_seen:
                self.queue_max_seen = d
            # Why: `await q.get()` on a non-empty queue returns WITHOUT yielding to
            # the event loop, so a burst would let this task monopolise the loop and
            # starve the WS reader -> TCP backpressure -> server closes us as a
            # "slow consumer". Yield explicitly every few messages.
            n += 1
            if n % 20 == 0:
                await asyncio.sleep(0)

    def _conds_of_conn(self, conn: int) -> list[str] | None:
        """Markets carried by the connection that broke, for scoped reconnect rows.

        Why look up by every conn_id a source used: the consumer may read the marker
        after that source already reconnected under a new id. conn 0 = the source
        never connected, so it carried no books: nothing goes stale ([]).
        With a single shared connection the scope is "all" (None), as before D-022."""
        if len(self.sources) == 1:
            return None
        if conn == 0:
            return []
        for s in self.sources.values():
            if conn in s.conns:
                return sorted({c for a in s.subscribed if (c := self.universe.token_cond.get(a))})
        return None  # unknown connection: be conservative, mark every book

    async def _lag_monitor(self, interval_s: float = 0.1) -> None:
        """Why: "slow consumer" can mean our loop froze or the venue's buffer filled.
        A timer that should fire every 100 ms and fires late measures the freeze
        directly; the worst value per health interval goes into `health`."""
        loop = asyncio.get_running_loop()
        while True:
            t = loop.time()
            await asyncio.sleep(interval_s)
            lag = (loop.time() - t - interval_s) * 1000
            if lag > self.loop_lag_max_ms:
                self.loop_lag_max_ms = lag

    async def _spot_consumer(self) -> None:
        q = self.spot_queue
        n = 0
        while True:
            ts, conn, raw = await q.get()
            self.spot_raw.append(ts, conn, raw)
            self.spot_parser.feed(ts, conn, raw)
            d = q.qsize()
            if d > self.spot_queue_max_seen:
                self.spot_queue_max_seen = d
            n += 1
            if n % 20 == 0:
                await asyncio.sleep(0)

    async def _flusher(self) -> None:
        rs = self.cfg.recorder
        while True:
            await asyncio.sleep(rs.flush_interval_s)
            await self._flush()

    async def _flush(self, final: bool = False) -> None:
        async with self._flush_lock:
            batches = self.parquet.take()
            lines = self.raw.take()
            spot_lines = self.spot_raw.take()
            if not batches and not lines and not spot_lines:
                return
            t0 = time.perf_counter()
            # 1) convert on the loop in slices, yielding between them (GIL-bound work)
            tables: dict[str, list] = {}
            first_ts = {t: int(rows[0][0]) for t, rows in batches.items()}
            for table, rows in batches.items():
                for part in self.parquet.convert_chunks(table, rows):
                    tables.setdefault(table, []).append(part)
                    await asyncio.sleep(0)
            t1 = time.perf_counter()
            # 2) compress + write in a thread (releases the GIL)
            def _io():
                self.raw.write(lines)
                self.spot_raw.write(spot_lines)
                self.parquet.write_tables(tables, first_ts)
                if final:
                    self.raw.close()
                    self.spot_raw.close()

            await asyncio.to_thread(_io)
            self.last_write_latency_ms = (time.perf_counter() - t0) * 1000
            self.last_convert_ms = (t1 - t0) * 1000

    async def _universe_tick(self) -> None:
        added, removed, refreshed = await self.universe.refresh()
        ts = time.time_ns()
        if added:
            self.parquet.buffers["markets"].extend(UniverseWatcher.rows("subscribe", added, ts))
        if removed:
            self.parquet.buffers["markets"].extend(UniverseWatcher.rows("unsubscribe", removed, ts))
        # hourly refresh rows capture parameter changes (rewards, fees) over time
        if refreshed and time.time() - self.universe._last_refresh_rows_ts > 3600:
            self.parquet.buffers["markets"].extend(UniverseWatcher.rows("refresh", refreshed, ts))
            self.universe._last_refresh_rows_ts = time.time()
        if len(self.sources) == 1:
            await next(iter(self.sources.values())).set_subscriptions(self.universe.desired_tokens())
        else:
            groups = self.universe.desired_tokens_by_family()
            first = next(iter(self.sources))
            want: dict[str, set[str]] = {n: set() for n in self.sources}
            for fam, toks in groups.items():
                want[fam if fam in want else first] |= toks  # unknown family: first connection
            for n, s in self.sources.items():
                await s.set_subscriptions(want[n])
        for mi in removed:
            for aid, _ in mi.tokens:
                self.books.pop(aid, None)

    async def _universe_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.recorder.universe_refresh_s)
            try:
                await self._universe_tick()
            except Exception as e:
                log.exception("universe tick failed: %r", e)

    async def _health_loop(self) -> None:
        rs = self.cfg.recorder
        while True:
            await asyncio.sleep(rs.health_interval_s)
            s = self.parser.stats
            sp = self.spot_parser.stats
            spot_reconn = self.spot_source.reconnects if self.spot_source is not None else 0
            tob_mm = sum(b.tob_mismatches for b in self.books.values())
            # Why aggregate: `health` keeps one row per interval; conn_id = newest
            # connection id (rises on every reconnect of any shard), reconnects summed.
            srcs = list(self.sources.values())
            conn_id = max(x.conn_id for x in srcs)
            n_sub = sum(len(x.subscribed) for x in srcs)
            reconn = sum(x.reconnects for x in srcs)
            lag_ms, self.loop_lag_max_ms = self.loop_lag_max_ms, 0.0
            row = (
                time.time_ns(), conn_id, time.time() - self.started_at, n_sub,
                s.msgs_total, s.msgs_book, s.msgs_delta, s.rows_delta, s.msgs_trade, s.msgs_tob, s.msgs_other,
                s.parse_errors, reconn, self.poller.gaps, tob_mm, self.poller.polls,
                self.queue.qsize(), self.queue_max_seen, self.last_write_latency_ms,
                self.raw.bytes_written, self.parquet.bytes_written, s.mirror_dropped, s.mirror_kept,
                sp.msgs, spot_reconn, lag_ms, self.poller.stale_rest,
            )
            self.parquet.buffers["health"].append(row)
            shards = " ".join(f"{x.name}:c{x.conn_id}/r{x.reconnects}/s{len(x.subscribed)}" for x in srcs)
            log.info(
                "conn=%d sub=%d msgs=%d (book=%d delta=%d/%drows trade=%d tob=%d other=%d err=%d) "
                "reconn=%d gaps=%d stale=%d races=%d tobmm=%d polls=%d q=%d/%d wlat=%.0fms lag=%.0fms raw=%.1fMB pq=%.1fMB "
                "mirror=%d dropped/%d kept spot=%d (q=%d t=%d err=%d reconn=%d q=%d/%d) ws[%s]",
                conn_id, n_sub, s.msgs_total, s.msgs_book, s.msgs_delta,
                s.rows_delta, s.msgs_trade, s.msgs_tob, s.msgs_other, s.parse_errors, reconn,
                self.poller.gaps, self.poller.stale_rest, self.poller.races, tob_mm, self.poller.polls, self.queue.qsize(), self.queue_max_seen,
                self.last_write_latency_ms, lag_ms, self.raw.bytes_written / 1e6, self.parquet.bytes_written / 1e6,
                s.mirror_dropped, s.mirror_kept,
                sp.msgs, sp.quotes, sp.trades, sp.parse_errors, spot_reconn, self.spot_queue.qsize(), self.spot_queue_max_seen,
                shards,
            )
            if self.parquet.pending_rows() >= rs.flush_max_rows:
                asyncio.create_task(self._flush())

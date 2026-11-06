"""Recorder: the composition root. Wires the stations and owns the event loop.

   WsSource ──► queue ──► consumer ──► RawSink.append
                                   └─► MarketParser ──► ParquetSink.buffers
   flusher (every N s): take() both sinks ──► to_thread(write)
   UniverseWatcher (60 s) ──► WsSource.set_subscriptions, `markets` rows
   RestPoller (60 s)      ──► gap check, `snapshot_rest` rows
   Health (10 s)          ──► `health` rows + log line
   SIGTERM/SIGINT         ──► stop source, drain queue, final flush, close
"""
from __future__ import annotations

import asyncio
import logging
import signal
import time

from .book import BookState
from .config import Config
from .parsers import MarketParser
from .poller import RestPoller
from .rest import PolyRest
from .sinks import ParquetSink, RawSink
from .sources import WsSource
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
        self.parser = MarketParser(self.books, self.parquet.buffers, self.universe.token_cond)
        self.source = WsSource(cfg.endpoints.ws_market, self.queue, rs.ping_interval_s, rs.custom_feature_enabled)
        self.poller = RestPoller(
            self.rest, self.books, self.parser, self.universe.token_cond, rs.rest_poll_interval_s, rs.rest_anchor_interval_s
        )
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

        tasks = [
            asyncio.create_task(self.source.run(), name="source"),
            asyncio.create_task(self._consumer(), name="consumer"),
            asyncio.create_task(self._flusher(), name="flusher"),
            asyncio.create_task(self.poller.run(), name="poller"),
            asyncio.create_task(self._universe_loop(), name="universe"),
            asyncio.create_task(self._health_loop(), name="health"),
        ]
        await self._stop.wait()
        log.info("stopping: draining queue (%d items)", self.queue.qsize())
        self.source.stop()
        self.poller.stop()
        tasks[0].cancel()
        tasks[3].cancel()
        tasks[4].cancel()
        tasks[5].cancel()
        # let the consumer drain what is already queued, then stop it
        while not self.queue.empty():
            await asyncio.sleep(0.05)
        tasks[1].cancel()
        tasks[2].cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
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

    async def _flusher(self) -> None:
        rs = self.cfg.recorder
        while True:
            await asyncio.sleep(rs.flush_interval_s)
            await self._flush()

    async def _flush(self, final: bool = False) -> None:
        async with self._flush_lock:
            batches = self.parquet.take()
            lines = self.raw.take()
            if not batches and not lines:
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
                self.parquet.write_tables(tables, first_ts)
                if final:
                    self.raw.close()

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
        await self.source.set_subscriptions(self.universe.desired_tokens())
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
            tob_mm = sum(b.tob_mismatches for b in self.books.values())
            row = (
                time.time_ns(), self.source.conn_id, time.time() - self.started_at, len(self.source.subscribed),
                s.msgs_total, s.msgs_book, s.msgs_delta, s.rows_delta, s.msgs_trade, s.msgs_tob, s.msgs_other,
                s.parse_errors, self.source.reconnects, self.poller.gaps, tob_mm, self.poller.polls,
                self.queue.qsize(), self.queue_max_seen, self.last_write_latency_ms,
                self.raw.bytes_written, self.parquet.bytes_written,
            )
            self.parquet.buffers["health"].append(row)
            log.info(
                "conn=%d sub=%d msgs=%d (book=%d delta=%d/%drows trade=%d tob=%d other=%d err=%d) "
                "reconn=%d gaps=%d tobmm=%d polls=%d q=%d/%d wlat=%.0fms raw=%.1fMB pq=%.1fMB",
                self.source.conn_id, len(self.source.subscribed), s.msgs_total, s.msgs_book, s.msgs_delta,
                s.rows_delta, s.msgs_trade, s.msgs_tob, s.msgs_other, s.parse_errors, self.source.reconnects,
                self.poller.gaps, tob_mm, self.poller.polls, self.queue.qsize(), self.queue_max_seen,
                self.last_write_latency_ms, self.raw.bytes_written / 1e6, self.parquet.bytes_written / 1e6,
            )
            if self.parquet.pending_rows() >= rs.flush_max_rows:
                asyncio.create_task(self._flush())

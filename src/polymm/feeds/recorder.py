"""Feeds recorder (D-042): every external feed, raw only, one process beside the main recorder.

   WsFeed × specs ──► queue ──► consumer ──► RawSink per feed   (flush every 30 s, worker thread)
   RestPoll × specs ─┘                                          health rows every 60 s

Why a separate process from `polymm record`: the Polymarket recorder is the one that must
never stop; a bug or a burst here cannot reach it. Why raw only: the VPS stores what the
venue sent, stamped on receive; `parse.py` builds the tables once a day from it (D-042), so a
parser bug costs a re-parse, never data.
"""
from __future__ import annotations

import asyncio
import itertools
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from websockets.asyncio.client import connect

from ..sinks import RawSink
from .spec import FEEDS, FeedSpec, RestPoll

log = logging.getLogger(__name__)

RECONNECT = "__RECONNECT__"
HEALTH_SCHEMA = pa.schema([
    ("ts", pa.int64()), ("feed", pa.string()), ("conn_id", pa.int32()), ("msgs", pa.int64()), ("bytes", pa.int64()),
    ("msgs_60s", pa.int64()), ("reconnects", pa.int32()), ("queue_max_60s", pa.int32()), ("polls", pa.int64()),
    ("poll_errors", pa.int64()), ("connected_s", pa.float64()),
])


class WsFeed:
    """One connection: connect, auth, subscribe, ping, stamp every frame, enqueue, reconnect."""

    def __init__(self, spec: FeedSpec, queue: asyncio.Queue, conn_ids):
        self.spec = spec
        self.queue = queue
        self._conn_ids = conn_ids
        self.conn_id = 0
        self.reconnects = 0
        self.idle_reconnects = 0
        self.msgs = 0
        self.bytes = 0
        self.connected_at: float | None = None
        self.on_connect: list = []  # coroutines to schedule after each connect (REST snapshots)
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        backoff = 1.0
        spec = self.spec
        while not self._stop.is_set():
            try:
                async with connect(spec.url, max_size=spec.max_size, open_timeout=20, max_queue=4096) as ws:
                    self.conn_id = next(self._conn_ids)
                    self.connected_at = time.time()
                    auth = spec.auth()
                    if auth:
                        await ws.send(auth)
                    for frame in spec.subscribe:
                        await ws.send(frame)
                    for cb in self.on_connect:
                        asyncio.create_task(cb())
                    log.info("connected [%s] conn_id=%d", spec.name, self.conn_id)
                    backoff = 1.0
                    pinger = asyncio.create_task(self._pinger(ws)) if spec.ping else None
                    try:
                        while True:
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=spec.idle_s)
                            except asyncio.TimeoutError:
                                # Why break and not continue: leaving the `async with` closes the
                                # socket; the loop below records the reconnect and opens a new one
                                log.warning("idle [%s] conn_id=%d: no frame for %.0f s, reconnecting", spec.name, self.conn_id, spec.idle_s)
                                self.idle_reconnects += 1
                                break
                            ts = time.time_ns()  # first: the stamp is the measurement
                            if isinstance(raw, bytes):
                                raw = raw.decode()
                            if not spec.keep(raw):
                                continue
                            self.msgs += 1
                            self.bytes += len(raw)
                            await self.queue.put((ts, spec.name, self.conn_id, raw))
                    finally:
                        if pinger:
                            pinger.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self._stop.is_set():
                    break
                log.warning("disconnected [%s] conn_id=%d: %r; retry in %.0fs", spec.name, self.conn_id, e, backoff)
            self.connected_at = None
            self.reconnects += 1
            await self.queue.put((time.time_ns(), spec.name, self.conn_id, RECONNECT))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    async def _pinger(self, ws) -> None:
        text, every = self.spec.ping
        try:
            while True:
                await asyncio.sleep(every)
                await ws.send(text)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass


class Poller:
    """REST polls of one feed, stored in its raw stream as conn_id 0."""

    def __init__(self, feed: str, poll: RestPoll, queue: asyncio.Queue, client: httpx.AsyncClient):
        self.feed, self.poll, self.queue, self.client = feed, poll, queue, client
        self.polls = 0
        self.errors = 0
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def once(self) -> None:
        try:
            r = await self.client.get(self.poll.url)
            ts = time.time_ns()
            r.raise_for_status()
            # Why wrap: the raw stream of a feed then holds both its WebSocket frames and its
            # REST answers, in receive order, and the parser tells them apart by `_rest`.
            line = orjson.dumps({"_rest": self.poll.name, "status": r.status_code, "body": r.json()}).decode()
            await self.queue.put((ts, self.feed, 0, line))
            self.polls += 1
        except Exception as e:
            self.errors += 1
            log.warning("poll %s/%s failed: %r", self.feed, self.poll.name, e)

    async def run(self) -> None:
        while not self._stop.is_set():
            await self.once()
            await asyncio.sleep(self.poll.every_s)


class FeedRecorder:
    def __init__(self, data_dir: Path, feeds: list[str] | None = None, flush_interval_s: float = 30, zstd_level: int = 12):
        names = feeds or list(FEEDS)
        self.specs = {n: FEEDS[n] for n in names}
        for s in self.specs.values():  # Why fail early: a missing secrets file is a setup error, not a reconnect loop
            if s.auth_env and not Path(os.path.expanduser(s.auth_env)).exists():
                raise SystemExit(f"feed {s.name}: secrets file {s.auth_env} not found")
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=200_000)
        self.root = data_dir / "feeds"
        conn_ids = itertools.count(1)
        self.feeds = {n: WsFeed(s, self.queue, conn_ids) for n, s in self.specs.items()}
        self.sinks = {n: RawSink(self.root, level=zstd_level, rotate_minutes=60, name=n) for n in self.specs}
        self.client = httpx.AsyncClient(timeout=20, headers={"User-Agent": "polymm-feeds/0.1"})
        self.pollers: list[Poller] = []
        for n, s in self.specs.items():
            for p in s.polls:
                po = Poller(n, p, self.queue, self.client)
                self.pollers.append(po)
                if p.on_connect:
                    self.feeds[n].on_connect.append(po.once)
        self.flush_interval_s = flush_interval_s
        self.queue_max = 0
        self._health_rows: list[tuple] = []
        self._health_hour = ""
        self._last_msgs: dict[str, int] = {n: 0 for n in self.specs}
        self._stop = asyncio.Event()
        self._flush_lock = asyncio.Lock()

    # ---- lifecycle --------------------------------------------------------------------

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._request_stop, sig)
        tasks = [asyncio.create_task(f.run(), name=f"ws:{n}") for n, f in self.feeds.items()]
        # Why only the periodic polls here: on_connect polls are fired by the feed itself
        tasks += [asyncio.create_task(p.run(), name=f"poll:{p.feed}") for p in self.pollers if not p.poll.on_connect]
        tasks += [asyncio.create_task(self._snapshot_loop(), name="snapshots")]
        consumer = asyncio.create_task(self._consumer(), name="consumer")
        flusher = asyncio.create_task(self._flusher(), name="flusher")
        health = asyncio.create_task(self._health_loop(), name="health")
        await self._stop.wait()
        log.info("stopping: %d items queued", self.queue.qsize())
        for f in self.feeds.values():
            f.stop()
        for p in self.pollers:
            p.stop()
        for t in tasks + [health]:
            t.cancel()
        while not self.queue.empty():
            await asyncio.sleep(0.05)
        consumer.cancel()
        flusher.cancel()
        await asyncio.gather(*tasks, consumer, flusher, health, return_exceptions=True)
        await self._flush(final=True)
        await self.client.aclose()
        log.info("stopped cleanly")

    def _request_stop(self, sig) -> None:
        log.info("signal %s", sig.name)
        self._stop.set()

    async def _snapshot_loop(self) -> None:
        """The periodic repeat of on_connect polls (the connect itself fires the first one)."""
        polls = [p for p in self.pollers if p.poll.on_connect]
        if not polls:
            return
        while True:
            await asyncio.sleep(min(p.poll.every_s for p in polls))
            for p in polls:
                await p.once()

    # ---- stations ---------------------------------------------------------------------

    async def _consumer(self) -> None:
        q = self.queue
        while True:
            ts, feed, conn, raw = await q.get()
            self.sinks[feed].append(ts, conn, raw)
            self.queue_max = max(self.queue_max, q.qsize())

    async def _flusher(self) -> None:
        while True:
            await asyncio.sleep(self.flush_interval_s)
            await self._flush()

    async def _flush(self, final: bool = False) -> None:
        async with self._flush_lock:
            pending = {n: s.take() for n, s in self.sinks.items()}
            loop = asyncio.get_running_loop()
            for n, lines in pending.items():
                if lines:
                    await loop.run_in_executor(None, self.sinks[n].write, lines)
            self._write_health(final)
            if final:
                for s in self.sinks.values():
                    await loop.run_in_executor(None, s.close)

    # ---- health -----------------------------------------------------------------------

    async def _health_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            ts = time.time_ns()
            parts = []
            for n, f in self.feeds.items():
                d = f.msgs - self._last_msgs[n]
                self._last_msgs[n] = f.msgs
                po = [p for p in self.pollers if p.feed == n]
                self._health_rows.append((ts, n, f.conn_id, f.msgs, f.bytes, d, f.reconnects, self.queue_max,
                                          sum(p.polls for p in po), sum(p.errors for p in po),
                                          round(time.time() - f.connected_at, 1) if f.connected_at else 0.0))
                parts.append(f"{n}:{d}/{f.reconnects}r")
            log.info("health q=%d/%d %s", self.queue.qsize(), self.queue_max, " ".join(parts))
            self.queue_max = 0

    def _write_health(self, final: bool) -> None:
        """One parquet per hour of health rows: data/feeds/tables/feeds_health/dt=…/hour=…"""
        if not self._health_rows:
            return
        hour = datetime.now(timezone.utc).strftime("%H")
        if hour == self._health_hour and not final:
            return
        rows, self._health_rows = self._health_rows, []
        ts0 = rows[0][0]
        d = datetime.fromtimestamp(ts0 / 1e9, tz=timezone.utc)
        out = self.root / "tables" / "feeds_health" / f"dt={d:%Y-%m-%d}" / f"hour={d:%H}"
        out.mkdir(parents=True, exist_ok=True)
        tbl = pa.Table.from_arrays([pa.array(c, type=f.type) for c, f in zip(zip(*rows), HEALTH_SCHEMA)], schema=HEALTH_SCHEMA)
        tmp = out / f"part-{d:%H%M%S}.parquet.tmp"
        pq.write_table(tbl, tmp, compression="zstd")
        os.replace(tmp, tmp.with_suffix(""))
        self._health_hour = hour

"""WsSource: the listener. Stamp, enqueue, nothing else.

Why so minimal: `ts_recv_ns` is the only measurement in the whole system that
must be precise. Anything the listener does besides stamping (parsing, disk)
delays the stamp on the *next* frame during a burst.

Owns the subscription set: on reconnect it resubscribes everything itself, so
the rest of the recorder never has to know a reconnect happened (except for
the RECONNECT marker it drops into the queue, which the parser records).
"""
from __future__ import annotations

import asyncio
import logging
import time

import orjson
import websockets
from websockets.asyncio.client import connect

log = logging.getLogger(__name__)

RECONNECT = "__RECONNECT__"  # marker frame; never a valid JSON message


class WsSource:
    def __init__(self, url: str, queue: asyncio.Queue, ping_interval_s: float, custom_feature: bool):
        self.url = url
        self.queue = queue
        self.ping_interval_s = ping_interval_s
        self.custom_feature = custom_feature
        self.subscribed: set[str] = set()
        self.conn_id = 0
        self.reconnects = 0
        self.connected_at: float | None = None
        self._ws = None
        self._stop = asyncio.Event()

    # ---- public ------------------------------------------------------------------

    async def set_subscriptions(self, desired: set[str]) -> None:
        add = desired - self.subscribed
        rem = self.subscribed - desired
        self.subscribed = set(desired)
        ws = self._ws
        if ws is None:
            return  # next connect() sends the full set
        try:
            if add:
                await ws.send(orjson.dumps({"assets_ids": sorted(add), "type": "market", "operation": "subscribe"}).decode())
            if rem:
                await ws.send(orjson.dumps({"assets_ids": sorted(rem), "type": "market", "operation": "unsubscribe"}).decode())
        except Exception as e:
            log.warning("subscription update failed (will resend on reconnect): %s", e)

    def stop(self) -> None:
        self._stop.set()

    # ---- main loop ---------------------------------------------------------------

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            if not self.subscribed:
                await asyncio.sleep(1.0)  # universe not resolved yet
                continue
            try:
                # Why max_size: a full `book` for a deep market can exceed the
                # 1 MiB default and would close the connection.
                # Why max_queue: websockets stops reading the socket when its own
                # frame queue is full (default 16 frames). During a burst that
                # propagates to TCP and Polymarket closes "slow consumers". A deep
                # queue absorbs the burst in memory instead.
                async with connect(self.url, max_size=16 * 2**20, open_timeout=20, max_queue=4096) as ws:
                    self._ws = ws
                    self.conn_id += 1
                    self.connected_at = time.time()
                    sub = {"assets_ids": sorted(self.subscribed), "type": "market"}
                    if self.custom_feature:
                        sub["custom_feature_enabled"] = True
                    await ws.send(orjson.dumps(sub).decode())
                    log.info("ws connected conn_id=%d subscribed=%d", self.conn_id, len(self.subscribed))
                    backoff = 1.0
                    pinger = asyncio.create_task(self._pinger(ws))
                    try:
                        async for raw in ws:
                            ts = time.time_ns()  # first thing, before anything else
                            if raw == "PONG":
                                continue
                            if isinstance(raw, bytes):
                                raw = raw.decode()
                            await self.queue.put((ts, self.conn_id, raw))
                    finally:
                        pinger.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self._stop.is_set():
                    break
                log.warning("ws disconnected (conn_id=%d): %r; reconnecting in %.0fs", self.conn_id, e, backoff)
            self._ws = None
            self.reconnects += 1
            await self.queue.put((time.time_ns(), self.conn_id, RECONNECT))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    async def _pinger(self, ws) -> None:
        # Why: Polymarket drops idle connections unless it sees a text "PING".
        try:
            while True:
                await asyncio.sleep(self.ping_interval_s)
                await ws.send("PING")
        except asyncio.CancelledError:
            pass
        except websockets.ConnectionClosed:
            pass

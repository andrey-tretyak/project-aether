from __future__ import annotations

import threading
import time
from collections.abc import Callable

from redis import Redis as SyncRedis

from src.common.logger import get_logger
from src.control_plane.heartbeat import HEARTBEAT_HASH, lease_key

log = get_logger("aether.worker.heartbeat")


class HeartbeatThread(threading.Thread):
    """Renews the worker's lease off the event loop.

    Blocking task code (a synchronous model client, a large JSON parse) can stall
    the asyncio loop for seconds. Renewing from a thread keeps a busy worker from
    being declared dead. To still catch a worker that is wedged rather than busy,
    renewal stops once the loop has not ticked for `max_loop_stall_s`.
    """

    def __init__(
        self,
        client: SyncRedis,
        worker_id: str,
        interval_s: float,
        lease_s: float,
        last_loop_tick: Callable[[], float],
        max_loop_stall_s: float,
    ):
        super().__init__(name=f"heartbeat-{worker_id}", daemon=True)
        self.client = client
        self.worker_id = worker_id
        self.interval_s = interval_s
        self.lease_ms = int(lease_s * 1000)
        self.last_loop_tick = last_loop_tick
        self.max_loop_stall_s = max_loop_stall_s
        self._halt = threading.Event()
        self._reported_stall = False

    def run(self) -> None:
        while not self._halt.is_set():
            stalled_for = time.monotonic() - self.last_loop_tick()
            if stalled_for > self.max_loop_stall_s:
                if not self._reported_stall:
                    log.info(
                        "worker.loop_wedged",
                        extra={"extra_fields": {"worker_id": self.worker_id, "stalled_s": stalled_for}},
                    )
                    self._reported_stall = True
            else:
                self._reported_stall = False
                try:
                    self.client.hset(HEARTBEAT_HASH, self.worker_id, str(time.time()))
                    self.client.set(lease_key(self.worker_id), "alive", px=self.lease_ms)
                except Exception:
                    log.exception("worker.heartbeat_failed")
            self._halt.wait(self.interval_s)

    def stop(self) -> None:
        self._halt.set()
        self.join(timeout=self.interval_s * 4)

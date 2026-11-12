"""A local Redis primary, its replicas, and the Sentinels that fail it over.

Used by the randomized fault test to kill and freeze Redis itself. Nodes run
without persistence, so a killed node is only restarted once the Sentinels have
promoted someone else, and then as a replica of the current primary: an empty
node coming back as primary would wipe every replica that synced from it.
"""

from __future__ import annotations

import signal
import subprocess
import time
from datetime import datetime
from pathlib import Path

from redis import Redis
from redis.sentinel import Sentinel

MASTER_NAME = "aether"
DOWN_AFTER_MS = 1000
FAILOVER_TIMEOUT_MS = 3000


class RedisTopology:
    def __init__(self, workdir: Path, base_port: int, replicas: int = 2, sentinels: int = 3):
        self.workdir = workdir
        self.node_ports = [base_port + i for i in range(1 + replicas)]
        self.sentinel_ports = [base_port + 100 + i for i in range(sentinels)]
        self.procs: dict[int, subprocess.Popen] = {}
        self.frozen: set[int] = set()

    @property
    def sentinel_addrs(self) -> str:
        return ",".join(f"127.0.0.1:{port}" for port in self.sentinel_ports)

    def _node(self, port: int, replica_of: int | None) -> None:
        # A replica keeps the snapshot it synced from in its working directory; a shared or reused
        # directory would hand a restarted node someone else's data.
        data = self.workdir / f"data-{port}-{time.monotonic_ns()}"
        data.mkdir()
        args = ["redis-server", "--port", str(port), "--save", "", "--appendonly", "no", "--bind", "127.0.0.1"]
        args += ["--dir", str(data), "--repl-diskless-sync-delay", "0"]
        if replica_of is not None:
            args += ["--replicaof", "127.0.0.1", str(replica_of)]
        log = open(self.workdir / f"redis-{port}.log", "w")
        self.procs[port] = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)

    def _sentinel(self, port: int, primary: int) -> None:
        conf = self.workdir / f"sentinel-{port}.conf"
        conf.write_text(
            f"port {port}\n"
            "bind 127.0.0.1\n"
            f"sentinel monitor {MASTER_NAME} 127.0.0.1 {primary} {len(self.sentinel_ports) // 2 + 1}\n"
            f"sentinel down-after-milliseconds {MASTER_NAME} {DOWN_AFTER_MS}\n"
            f"sentinel failover-timeout {MASTER_NAME} {FAILOVER_TIMEOUT_MS}\n"
            f"sentinel parallel-syncs {MASTER_NAME} 1\n"
        )
        log = open(self.workdir / f"sentinel-{port}.log", "w")
        self.procs[port] = subprocess.Popen(
            ["redis-server", str(conf), "--sentinel"], stdout=log, stderr=subprocess.STDOUT
        )

    def start(self, timeout_s: float = 15.0) -> None:
        primary, *replicas = self.node_ports
        self._node(primary, None)
        for port in replicas:
            self._node(port, primary)
        for port in self.sentinel_ports:
            self._sentinel(port, primary)
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                peers = [
                    Redis(port=port, socket_timeout=0.5).sentinel_master(MASTER_NAME)["num-other-sentinels"]
                    for port in self.sentinel_ports
                ]
                if all(n == len(self.sentinel_ports) - 1 for n in peers) and self._replicas_ready(replicas):
                    return
            except Exception:
                pass
            time.sleep(0.2)
        raise RuntimeError("Redis topology did not come up")

    def _replicas_ready(self, replicas: list[int]) -> bool:
        """Every replica has synced, and every Sentinel knows it as a healthy replica.

        Sentinels learn replicas only from the primary's INFO, which they poll every 10s.
        A primary that dies before that leaves them nobody to promote: each failover attempt
        aborts with no-good-slave, and Redis stays down."""
        if not all(self.synced(port) for port in replicas):
            return False
        for port in self.sentinel_ports:
            try:
                known = Redis(port=port, socket_timeout=0.5).sentinel_slaves(MASTER_NAME)
            except Exception:
                return False
            healthy = {int(r["port"]) for r in known if not r.get("is_sdown") and not r.get("is_disconnected")}
            if not set(replicas) <= healthy:
                return False
        return True

    def synced(self, port: int) -> bool:
        """Whether a replica has finished its initial sync. Until then it holds nothing, yet
        Sentinel will still promote it if the primary dies, losing everything."""
        try:
            return Redis(port=port, socket_timeout=0.5).info("replication").get("master_link_status") == "up"
        except Exception:
            return False

    def primary(self) -> int | None:
        """The primary the Sentinels currently agree on."""
        try:
            sentinels = Sentinel([("127.0.0.1", p) for p in self.sentinel_ports], socket_timeout=0.5)
            _, port = sentinels.discover_master(MASTER_NAME)
            return int(port)
        except Exception:
            return None

    def settled(self) -> bool:
        """One agreed primary, and every other running node a synced replica the Sentinels know."""
        primary = self.primary()
        if primary is None or not self.alive(primary):
            return False
        return self._replicas_ready([p for p in self.node_ports if p != primary and self.alive(p)])

    def replicas(self) -> list[int]:
        primary = self.primary()
        return [p for p in self.node_ports if p != primary]

    def alive(self, port: int) -> bool:
        return self.procs[port].poll() is None

    def kill(self, port: int) -> None:
        self.procs[port].kill()
        self.procs[port].wait()

    def freeze(self, port: int) -> None:
        self.frozen.add(port)
        self.procs[port].send_signal(signal.SIGSTOP)

    def thaw(self, port: int) -> None:
        self.frozen.discard(port)
        self.procs[port].send_signal(signal.SIGCONT)

    def cut_replication(self, primary: int) -> None:
        """Drop the primary's links to its replicas. A frozen replica cannot reconnect, so it
        misses every write from now on; freezing alone would not do, because the kernel keeps
        buffering the stream for it."""
        Redis(port=primary, socket_timeout=0.5).client_kill_filter(_type="slave")

    def restart_as_replica(self, port: int) -> bool:
        """Bring a killed node back under the current primary; False if none is elected yet."""
        primary = self.primary()
        if primary is None or primary == port:
            return False
        self._node(port, primary)
        return True

    def failover_windows(self) -> list[list[float]]:
        """Every failover the Sentinels ran, asked for or not, as [start, end] wall-clock times.

        A window opens one detection period before the switch and closes when the old primary
        is demoted. Until then, clients still connected to it are cut off from the real primary.
        With a 1s down-after, Sentinel occasionally fails over a primary it promoted moments ago.
        """
        switches: list[tuple[float, int, int]] = []
        demotions: list[tuple[float, int]] = []
        for port in self.sentinel_ports:
            for line in (self.workdir / f"sentinel-{port}.log").read_text().splitlines():
                parts = line.split()
                if "+switch-master" in parts:
                    i = parts.index("+switch-master")
                    switches.append((_log_time(parts), int(parts[i + 3]), int(parts[i + 5])))
                elif "+convert-to-slave" in parts:
                    node = int(parts[parts.index("+convert-to-slave") + 2].rsplit(":", 1)[1])
                    demotions.append((_log_time(parts), node))
        windows = []
        for at, old, _ in switches:
            # A killed primary comes back as a replica without being demoted; stop looking once it leads again.
            promoted_again = min((t for t, _, new in switches if new == old and t > at), default=float("inf"))
            demoted = min((t for t, node in demotions if node == old and at <= t < promoted_again), default=at)
            windows.append([at - DOWN_AFTER_MS / 1000 - 1.0, demoted])
        return windows

    def stop(self) -> None:
        for port in list(self.frozen):
            self.thaw(port)
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in self.procs.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


def _log_time(parts: list[str]) -> float:
    """Wall-clock time of a Redis log line: "<pid>:<role> 11 Nov 2026 20:41:40.888 ..."."""
    return datetime.strptime(" ".join(parts[1:5]), "%d %b %Y %H:%M:%S.%f").timestamp()

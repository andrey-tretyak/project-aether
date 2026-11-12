"""Randomized fault injection against a live cluster, checked from the log afterwards.

A seeded schedule kills, freezes (SIGSTOP) and drains (SIGTERM) random workers
and leaders while a workflow runs, then restarts or resumes each one after a
random delay. Freezes range from shorter than the lease to well past it, so some
processes ride it out and others come back as zombies. Worker faults prefer a
worker that is mid-task; leader faults land just after a task commits, when the
leader is most likely mid-dispatch.

With --redis-ha, Redis runs as a primary, a replica and 3 Sentinels, and the
schedule also kills or freezes the primary (forcing a failover), freezes the
replica, or fails over to a replica that is behind, the case that loses writes.
The external API's idempotency store lives on a separate Redis, as another
service's would.

Once the workflow is done and every frozen process has woken up and had a
chance to write, a run passes only if:
  - the WAL audit finds no violation: fences only advance, every worker write
    carries the current fence, tasks dispatch after their dependencies commit,
    nothing touches a committed task, no deposed leader writes after its
    successor, and every task commits exactly once;
  - every external effect was performed exactly once;
  - no two control planes were elected with the same epoch;
  - no worker was declared dead unless it was killed or frozen at the time, or
    a Redis fault was in progress (a worker still talking to a deposed primary
    is cut off, as in a partition);
  - every drained worker exited 0 and no process crashed.
The seed fixes the fault schedule, not the interleaving, which depends on process
timing; rerunning a failing seed is a starting point for reproducing the bug.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import signal
import subprocess
import sys
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from chaos_test import (
    ROOT,
    STALE_WRITE_EVENTS,
    ChaosFailure,
    _busy_workers,
    _check_effects,
    _clear_aether_keys,
    _log_events,
    _spawn,
    _wait_until,
)
from redis.asyncio import Redis
from redis.asyncio.sentinel import Sentinel as AsyncSentinel
from redis.exceptions import RedisError
from redis_topology import MASTER_NAME, RedisTopology

from src.common.config import load_settings
from src.common.models import TaskStatus
from src.control_plane.audit import audit_wal
from src.control_plane.heartbeat import HEARTBEAT_HASH
from src.control_plane.leader import LEADER_KEY
from src.control_plane.scheduler import RESULTS_CHANNEL
from src.control_plane.wal import WriteAheadLog

# Polling the WAL while Redis fails over logs a traceback per failed read.
logging.getLogger("aether.wal").setLevel(logging.CRITICAL)

FAULT_WEIGHTS = {
    "kill-worker": 3,
    "freeze-worker": 3,
    "drain-worker": 1,
    "kill-leader": 2,
    "freeze-leader": 2,
}
REDIS_FAULT_WEIGHTS = {
    "kill-redis": 1,
    "freeze-redis": 1,
    "freeze-replica": 1,
    "lagging-failover": 1,
}
FAULT_GAP_S = (0.5, 2.0)
RESTART_DELAY_S = (0.5, 3.0)
WORKER_FREEZE_S = (0.3, 4.0)
LEADER_FREEZE_S = (0.5, 5.0)
REDIS_FREEZE_S = (0.5, 4.0)
# How far the replica falls behind before the primary dies under it.
REPLICA_LAG_S = (0.3, 1.5)
FAILOVER_WAIT_S = 30.0
REDIS_WINDOW = "redis"
DRAIN_TIMEOUT_S = 10.0
# Time for woken processes to attempt their late writes before the audit reads the log.
SETTLE_S = 1.5
ROLES = {"worker": ("worker", "WORKER_ID"), "control-plane": ("control-plane", "CONTROL_PLANE_ID")}
STARTED = {"worker": "worker.started", "control-plane": "control_plane.standby"}


@dataclass
class Fault:
    at_s: float
    kind: str
    hold_s: float
    pick: float


def schedule(seed: int, window_s: float, redis_faults: bool = False) -> list[Fault]:
    rng = random.Random(seed)
    weights = {**FAULT_WEIGHTS, **(REDIS_FAULT_WEIGHTS if redis_faults else {})}
    faults, at = [], 0.0
    while True:
        at += rng.uniform(*FAULT_GAP_S)
        if at > window_s:
            return faults
        kind = rng.choices(list(weights), weights=list(weights.values()))[0]
        if kind == "freeze-leader":
            hold = rng.uniform(*LEADER_FREEZE_S)
        elif kind == "freeze-worker":
            hold = rng.uniform(*WORKER_FREEZE_S)
        elif kind in ("freeze-redis", "freeze-replica"):
            hold = rng.uniform(*REDIS_FREEZE_S)
        elif kind == "lagging-failover":
            hold = rng.uniform(*REPLICA_LAG_S)
        else:
            hold = rng.uniform(*RESTART_DELAY_S)
        faults.append(Fault(round(at, 2), kind, round(hold, 2), rng.random()))


class Cluster:
    def __init__(self, logdir: Path, env: dict[str, str], topo: Optional[RedisTopology] = None):
        self.logdir, self.env, self.topo = logdir, env, topo
        self.redis_busy = False
        self.procs: dict[str, subprocess.Popen] = {}
        self.logs: dict[str, Path] = {}
        self.counts: Counter[str] = Counter()
        self.started: set[str] = set()
        self.unhealed: set[str] = set()
        self.frozen: set[str] = set()
        self.signalled: set[str] = set()
        self.windows: dict[str, list[list[float]]] = {}
        self.problems: list[str] = []
        self.heals: list[asyncio.Task] = []

    def start(self, role: str) -> str:
        command, id_var = ROLES[role]
        self.counts[role] += 1
        name = f"{role}-{self.counts[role]}"
        self.logs[name] = self.logdir / f"{name}.log"
        self.procs[name] = _spawn([command], {**self.env, id_var: name}, self.logs[name])
        return name

    def members(self, role: str) -> list[str]:
        return [n for n in self.procs if n.startswith(f"{role}-")]

    def healthy(self, role: str) -> list[str]:
        """Running, not under a fault, and past startup (a process signalled while importing just dies)."""
        for name in self.members(role):
            if name not in self.started and any(e.get("msg") == STARTED[role] for e in _log_events(self.logs[name])):
                self.started.add(name)
        return [
            n
            for n in self.members(role)
            if n in self.started and self.procs[n].poll() is None and n not in self.unhealed
        ]

    def logs_of(self, role: str) -> list[Path]:
        return [self.logs[n] for n in self.members(role)]

    def _open_window(self, name: str) -> None:
        self.windows.setdefault(name, []).append([time.time(), float("inf")])

    def kill(self, name: str, restart_after: float) -> None:
        self.unhealed.add(name)
        self.signalled.add(name)
        self._open_window(name)
        self.procs[name].kill()
        self.procs[name].wait()
        self.heals.append(asyncio.create_task(self._replace(name, restart_after)))

    def freeze(self, name: str, hold: float) -> None:
        self.unhealed.add(name)
        self.frozen.add(name)
        self._open_window(name)
        self.procs[name].send_signal(signal.SIGSTOP)
        self.heals.append(asyncio.create_task(self._thaw(name, hold)))

    def drain(self, name: str, restart_after: float) -> None:
        self.unhealed.add(name)
        self.signalled.add(name)
        self.procs[name].terminate()
        self.heals.append(asyncio.create_task(self._replace(name, restart_after, drained=True)))

    async def _thaw(self, name: str, hold: float) -> None:
        await asyncio.sleep(hold)
        self.procs[name].send_signal(signal.SIGCONT)
        self.frozen.discard(name)
        self.windows[name][-1][1] = time.time()
        self.unhealed.discard(name)

    async def _replace(self, name: str, delay: float, drained: bool = False) -> None:
        await asyncio.sleep(delay)
        if drained:
            deadline = time.monotonic() + DRAIN_TIMEOUT_S
            while self.procs[name].poll() is None and time.monotonic() < deadline:
                await asyncio.sleep(0.1)
            code = self.procs[name].poll()
            if code != 0:
                self.problems.append(f"{name} did not drain cleanly after SIGTERM (exit {code})")
        self.start(name.rsplit("-", 1)[0])
        self.unhealed.discard(name)

    def redis_fault(self, kind: str, hold: float) -> Optional[str]:
        """Start one Redis fault, unless one is already under way; returns what was done."""
        if self.topo is None or self.redis_busy:
            return None
        primary = self.topo.primary()
        replicas = [p for p in self.topo.replicas() if self.topo.alive(p) and p not in self.topo.frozen]
        if primary is None or not self.topo.alive(primary) or primary in self.topo.frozen:
            return None
        if kind in ("freeze-replica", "lagging-failover") and not replicas:
            return None
        self.redis_busy = True
        # Until Sentinel demotes a deposed primary, workers still connected to it are cut off from
        # the real one, as in a partition, so any of them may be declared dead.
        self._open_window(REDIS_WINDOW)
        if kind == "kill-redis":
            self.topo.kill(primary)
            heal = self._rejoin(primary, hold)
            done = f"kill redis primary :{primary}, rejoins as a replica {hold:.1f}s after failover"
        elif kind == "freeze-redis":
            self.topo.freeze(primary)
            heal = self._thaw_redis(primary, hold)
            done = f"freeze redis primary :{primary} for {hold:.1f}s"
        elif kind == "freeze-replica":
            self.topo.freeze(replicas[0])
            heal = self._thaw_redis(replicas[0], hold)
            done = f"freeze redis replica :{replicas[0]} for {hold:.1f}s"
        else:
            heal = self._lagging_failover(primary, replicas[0], hold)
            done = f"cut off redis replica :{replicas[0]}, kill primary :{primary} {hold:.1f}s later"
        self.heals.append(asyncio.create_task(heal))
        return done

    async def _thaw_redis(self, port: int, hold: float) -> None:
        await asyncio.sleep(hold)
        self.topo.thaw(port)
        await self._settle(time.monotonic() + FAILOVER_WAIT_S)

    async def _rejoin(self, port: int, delay: float) -> None:
        deadline = time.monotonic() + FAILOVER_WAIT_S + delay
        while self.topo.primary() in (None, port):
            if time.monotonic() > deadline:
                self.problems.append(f"Sentinel never promoted a replica after :{port} died")
                return
            await asyncio.sleep(0.2)
        await asyncio.sleep(delay)
        self.topo.restart_as_replica(port)
        await self._settle(deadline)

    async def _settle(self, deadline: float) -> None:
        # Faulting the primary while a replica is still syncing would make Sentinel promote an empty node.
        while not self.topo.settled():
            if time.monotonic() > deadline:
                self.problems.append("Redis never settled into a primary with synced replicas")
                return
            await asyncio.sleep(0.2)
        self.windows[REDIS_WINDOW][-1][1] = time.time()
        self.redis_busy = False

    async def _lagging_failover(self, primary: int, replica: int, lag: float) -> None:
        self.topo.freeze(replica)
        self.topo.cut_replication(primary)
        await asyncio.sleep(lag)
        self.topo.kill(primary)
        await asyncio.sleep(0.2)
        self.topo.thaw(replica)
        await self._rejoin(primary, 0.5)

    def crashed(self) -> list[str]:
        return [
            f"{n} (exit {p.returncode})" for n, p in self.procs.items() if p.poll() is not None and n not in self.signalled
        ]

    def shutdown(self) -> None:
        if self.topo is not None:
            for port in list(self.topo.frozen):
                self.topo.thaw(port)
        for name in self.frozen:
            self.procs[name].send_signal(signal.SIGCONT)
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in self.procs.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


async def _next_commit(redis: Redis, timeout_s: float) -> None:
    """Wait for a task to commit: the leader is then most likely mid-dispatch, the riskiest moment to lose it."""
    pubsub = redis.pubsub()
    await pubsub.subscribe(RESULTS_CHANNEL)
    try:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1):
                return
    finally:
        await pubsub.unsubscribe()
        await pubsub.aclose()


async def _inject(cluster: Cluster, redis: Redis, wal: WriteAheadLog, workflow_id: str, fault: Fault) -> Optional[str]:
    """Apply one fault if the cluster can take it; returns what was done, or None if skipped."""
    if fault.kind in REDIS_FAULT_WEIGHTS:
        return cluster.redis_fault(fault.kind, fault.hold_s)
    role = "worker" if fault.kind.endswith("worker") else "control-plane"
    healthy = cluster.healthy(role)
    if len(healthy) < 2:
        return None
    if role == "worker":
        try:
            state = await wal.recover_latest_state(workflow_id)
            done = {t.id for t in state.workflow.tasks if t.status == TaskStatus.COMPLETED} if state else set()
            busy = {w: [t for t in held if t not in done] for w, held in (await _busy_workers(redis)).items()}
        except RedisError:
            busy = {}
        targets = sorted(w for w in healthy if busy.get(w)) or sorted(healthy)
        target = targets[int(fault.pick * len(targets))]
        detail = f" (holding {', '.join(busy[target])})" if busy.get(target) else " (idle)"
    else:
        try:
            await _next_commit(redis, timeout_s=2.0)
            token = await redis.get(LEADER_KEY)
        except RedisError:
            return None
        target = token.rpartition(":")[0] if token else None
        if target not in healthy:
            return None
        detail = f" (epoch {token.rpartition(':')[2]})"

    action = fault.kind.split("-")[0]
    if action == "kill":
        cluster.kill(target, fault.hold_s)
        detail += f", replaced after {fault.hold_s:.1f}s"
    elif action == "freeze":
        cluster.freeze(target, fault.hold_s)
        detail += f" for {fault.hold_s:.1f}s"
    else:
        cluster.drain(target, fault.hold_s)
    return f"{action} {target}{detail}"


def _answers(client: Redis):
    async def ping() -> bool:
        try:
            return bool(await client.ping())
        except RedisError:
            return False

    return ping


async def _why_stuck(redis: Redis, workflow_id: str, dependencies: dict[str, list[str]]) -> str:
    """The WAL audit of a workflow that never finished, minus the tasks it never got to."""
    try:
        deltas = [json.loads(d) for d in await redis.lrange(f"aether:wal:{workflow_id}", 0, -1)]
    except RedisError:
        return ""
    violations = [v for v in audit_wal(deltas, dependencies) if "committed 0 times" not in v]
    return f"; WAL audit found {len(violations)} violations: {violations[:5]}" if violations else ""


def _count(n: int, noun: str) -> str:
    return f"{n} {noun}{'' if n == 1 else 's'}"


def _check_leaders(events: list[dict]) -> str:
    elected = [(int(e["epoch"]), e["node_id"]) for e in events if e.get("msg") == "control_plane.elected"]
    per_epoch = Counter(epoch for epoch, _ in elected)
    shared = sorted(epoch for epoch, n in per_epoch.items() if n > 1)
    if shared:
        raise ChaosFailure(f"more than one control plane elected in epochs {shared}: {elected}")
    return " -> ".join(f"{node} (epoch {epoch})" for epoch, node in sorted(elected))


def _check_deaths(cluster: Cluster, events: list[dict], detection_budget: float) -> int:
    deaths = [e for e in events if e.get("msg") == "worker.dead"]
    for death in deaths:
        worker = death.get("worker_id")
        at = datetime.fromisoformat(death["ts"]).timestamp()
        windows = cluster.windows.get(worker, []) + cluster.windows.get(REDIS_WINDOW, [])
        if not any(start <= at <= end + detection_budget for start, end in windows):
            raise ChaosFailure(
                f"{worker} was declared dead at {death['ts']} while it was neither killed nor frozen "
                "and Redis was healthy"
            )
    return len(deaths)


async def run_once(args: argparse.Namespace, seed: int) -> dict[str, Any]:
    settings = load_settings()
    detection_budget = settings.lease_s + settings.monitor_poll_s + 0.5
    faults = schedule(seed, args.window, redis_faults=args.redis_ha)
    logdir = Path(tempfile.mkdtemp(prefix=f"aether-nemesis-{seed}-"))
    env = {**os.environ, "REDIS_URL": args.redis_url, "PYTHONPATH": str(ROOT)}
    topo: Optional[RedisTopology] = None
    outside: Optional[subprocess.Popen] = None
    if args.redis_ha:
        (logdir / "redis").mkdir()
        topo = RedisTopology(logdir / "redis", args.redis_port, replicas=1)
        topo.start()
        # The external API is someone else's service: Aether's Redis failing over must not touch it.
        outside_port = args.redis_port + 50
        outside = subprocess.Popen(
            ["redis-server", "--port", str(outside_port), "--save", "", "--appendonly", "no",
             "--bind", "127.0.0.1", "--dir", str(logdir / "redis")],
            stdout=subprocess.DEVNULL,
        )
        env.update(
            REDIS_SENTINELS=topo.sentinel_addrs,
            REDIS_SENTINEL_MASTER=MASTER_NAME,
            EXTERNAL_API_REDIS_URL=f"redis://127.0.0.1:{outside_port}/0",
        )
        timeouts = {"socket_timeout": 2.0, "socket_connect_timeout": 1.0}
        sentinels = [("127.0.0.1", port) for port in topo.sentinel_ports]
        redis = AsyncSentinel(sentinels, **timeouts).master_for(MASTER_NAME, decode_responses=True, **timeouts)
        effects_redis = Redis(port=outside_port, decode_responses=True)
        await _wait_until(_answers(effects_redis), 10, "the external API's Redis to start")
    else:
        redis = effects_redis = Redis.from_url(args.redis_url, decode_responses=True)
    await redis.ping()
    await _clear_aether_keys(redis)
    wal = WriteAheadLog(redis)
    dag_path = Path(args.dag).resolve()
    dag = json.loads(dag_path.read_text())
    workflow_id = dag["workflow_id"]

    cluster = Cluster(logdir, env, topo)
    for _ in range(args.control_planes):
        cluster.start("control-plane")
    for _ in range(args.workers):
        cluster.start("worker")
    history: list[str] = []

    try:
        async def cluster_ready() -> bool:
            events = _log_events(*cluster.logs_of("control-plane"))
            standing = {e["_log"] for e in events if e.get("msg") == "control_plane.standby"}
            return (
                any(e.get("msg") == "control_plane.started" for e in events)
                and len(standing) == args.control_planes
                and await redis.hlen(HEARTBEAT_HASH) >= args.workers
            )

        await _wait_until(cluster_ready, 20, "control planes and workers to start")
        subprocess.run(
            [sys.executable, "-m", "src.main", "submit-workflow", str(dag_path)],
            cwd=ROOT,
            env=env,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        submitted = time.monotonic()

        async def finished() -> bool:
            try:
                state = await wal.recover_latest_state(workflow_id)
            except RedisError:
                return False
            if state is not None and state.workflow.has_failed():
                failed = [t.id for t in state.workflow.tasks if t.status == TaskStatus.FAILED]
                raise ChaosFailure(f"tasks failed: {failed}")
            return state is not None and state.workflow.is_complete()

        for fault in faults:
            await asyncio.sleep(max(0.0, fault.at_s - (time.monotonic() - submitted)))
            if await finished():
                break
            done = await _inject(cluster, redis, wal, workflow_id, fault)
            history.append(f"+{time.monotonic() - submitted:5.2f}s  {done or f'skip {fault.kind} (nothing spare to fault)'}")

        dependencies = {t["id"]: t.get("dependencies", []) for t in dag["tasks"]}
        try:
            await _wait_until(finished, args.timeout, f"the workflow to finish within {args.timeout}s")
        except ChaosFailure as exc:
            raise ChaosFailure(f"{exc}{await _why_stuck(redis, workflow_id, dependencies)}") from None
        elapsed = time.monotonic() - submitted
        await asyncio.gather(*cluster.heals)
        await asyncio.sleep(SETTLE_S)

        injected = [h for h in history if " skip " not in h]
        print(f"seed {seed}: {len(injected)} faults injected, {len(history) - len(injected)} skipped")
        for line in history:
            print(f"  {line}")

        deltas = [json.loads(d) for d in await redis.lrange(f"aether:wal:{workflow_id}", 0, -1)]
        violations = audit_wal(deltas, dependencies)
        if violations:
            raise ChaosFailure(f"WAL audit found {len(violations)} violations: {violations[:5]}")
        worker_events = _log_events(*cluster.logs_of("worker"))
        cp_events = _log_events(*cluster.logs_of("control-plane"))
        claims = Counter(e.get("task_id") for e in worker_events if e.get("msg") == "worker.claimed")
        re_executed = sum(n - 1 for n in claims.values())
        print(
            f"PASS  {len(dag['tasks'])} tasks completed in {elapsed:.2f}s; WAL audit of {len(deltas)} entries "
            f"found no violations ({_count(re_executed, 'task')} re-executed)"
        )
        zombie_writes = sum(1 for e in worker_events if e.get("msg") in STALE_WRITE_EVENTS)
        leader_writes = sum(1 for e in cp_events if e.get("msg") == "control_plane.write_rejected")
        print(
            f"      late writes rejected: {zombie_writes} from zombie workers (fence), "
            f"{leader_writes} from deposed leaders (leader check)"
        )
        await _check_effects(effects_redis, dag, expect_duplicates=False)
        print(f"PASS  one leader per epoch: {_check_leaders(cp_events)}")
        if topo is not None:
            cluster.windows.setdefault(REDIS_WINDOW, []).extend(topo.failover_windows())
        deaths = _check_deaths(cluster, cp_events, detection_budget)
        cause = "killed, frozen, or cut off by a Redis fault" if args.redis_ha else "killed or frozen"
        print(f"PASS  {_count(deaths, 'worker')} declared dead, each while {cause}")
        problems = cluster.problems + [f"{c} crashed" for c in cluster.crashed()]
        if problems:
            raise ChaosFailure("; ".join(problems))
        drains = sum(1 for h in injected if " drain " in h)
        print(f"PASS  no process crashed; {_count(drains, 'drained worker')} exited 0")
        return {"seed": seed, "passed": True, "faults": len(injected), "elapsed_s": round(elapsed, 2)}
    except Exception:
        print(f"seed {seed} history:")
        for line in history:
            print(f"  {line}")
        print(f"logs: {logdir}")
        raise
    finally:
        for heal in cluster.heals:
            heal.cancel()
        cluster.shutdown()
        await redis.aclose()
        if topo is not None:
            await effects_redis.aclose()
            topo.stop()
            outside.terminate()
            outside.wait()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dag", default=str(ROOT / "failover_dag.json"))
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--control-planes", type=int, default=3)
    parser.add_argument("--seed", type=int, default=None, help="first seed; random if omitted")
    parser.add_argument("--runs", type=int, default=1, help="consecutive seeds to run, stopping at the first failure")
    parser.add_argument("--window", type=float, default=12.0, help="seconds after submit during which faults start")
    parser.add_argument("--timeout", type=float, default=90.0)
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument(
        "--redis-ha",
        action="store_true",
        help="run Redis as a primary, a replica, and 3 Sentinels, and fault Redis too",
    )
    parser.add_argument("--redis-port", type=int, default=7400, help="first port of the --redis-ha topology")
    parser.add_argument("--json", type=Path, default=None, help="write per-run results to this file")
    args = parser.parse_args()
    if args.workers < 2 or args.control_planes < 2:
        parser.error("needs at least 2 workers and 2 control planes so a fault never takes out a whole role")

    first = args.seed if args.seed is not None else random.randrange(1_000_000)
    results: list[dict[str, Any]] = []
    code = 0
    for seed in range(first, first + args.runs):
        try:
            results.append(asyncio.run(run_once(args, seed)))
        except ChaosFailure as exc:
            print(f"FAIL  seed {seed}: {exc}")
            print(f"rerun: python scripts/nemesis.py --seed {seed}{' --redis-ha' if args.redis_ha else ''}")
            results.append({"seed": seed, "passed": False, "error": str(exc)})
            code = 1
            break
    if args.json:
        args.json.write_text(json.dumps(results))
    return code


if __name__ == "__main__":
    sys.exit(main())

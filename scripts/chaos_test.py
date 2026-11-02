"""End-to-end chaos test: a real control plane and workers against a live Redis.

Exits non-zero unless every task completes exactly once.
  --kill-after S   SIGKILL a worker that is mid-task; its work must finish on a
                   survivor within the recovery budget derived from the lease.
  --stop-after S   SIGTERM a worker that is mid-task; it must drain (finish its
                   task, exit cleanly) without ever being declared dead.
  --pause-after S  SIGSTOP a worker that is mid-task, wait until its work is
                   reassigned, then SIGCONT it; its late write must be rejected
                   by the fence and the task committed once, by a survivor.
  --kill-leader-after S
                   SIGKILL the leading control plane (needs --control-planes 2+);
                   a standby must take over and finish the workflow.
  --pause-leader-after S
                   SIGSTOP the leader until a standby takes over, then SIGCONT it;
                   it must step down, and no write from its epoch may land after
                   the new leader's first.
Unless --allow-false-deaths is passed, any live worker declared dead fails the run.
Detection and recovery budgets are derived from the configured lease.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from redis.asyncio import Redis  # noqa: E402

from src.common.config import load_settings  # noqa: E402
from src.common.models import TaskStatus  # noqa: E402
from src.control_plane.heartbeat import HEARTBEAT_HASH  # noqa: E402
from src.control_plane.leader import LEADER_KEY  # noqa: E402
from src.control_plane.wal import WriteAheadLog  # noqa: E402

logging.getLogger("aether.wal").setLevel(logging.WARNING)

DRAIN_TIMEOUT_S = 10.0
ZOMBIE_TIMEOUT_S = 10.0
PAUSE_ATTEMPTS = 5
STALE_WRITE_EVENTS = {"worker.stale_start", "worker.stale_commit", "worker.stale_fail_commit"}


class ChaosFailure(Exception):
    pass


def _spawn(args: list[str], env: dict[str, str], log_path: Path) -> subprocess.Popen:
    log = open(log_path, "w")
    return subprocess.Popen(
        [sys.executable, "-m", "src.main", *args],
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )


def _log_events(*log_paths: Path) -> list[dict]:
    events = []
    for log_path in log_paths:
        for line in log_path.read_text().splitlines():
            if line.startswith("{"):
                try:
                    events.append({**json.loads(line), "_log": log_path.stem})
                except json.JSONDecodeError:
                    continue
    return sorted(events, key=lambda e: e.get("ts", ""))


async def _clear_aether_keys(redis: Redis) -> None:
    async for key in redis.scan_iter(match="aether:*"):
        await redis.delete(key)


async def _wait_until(predicate, timeout_s: float, what: str) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.1)
    raise ChaosFailure(f"timed out waiting for {what}")


async def _busy_workers(redis: Redis) -> dict[str, list[str]]:
    busy: dict[str, list[str]] = {}
    async for key in redis.scan_iter(match="aether:lock:*"):
        holder = await redis.get(key)
        if holder:
            worker_id = holder.rsplit(":", 1)[0]
            busy.setdefault(worker_id, []).append(key[len("aether:lock:"):])
    return busy


def _liveness_stats(cp_logs: list[Path], worker_logs: dict[str, Path], expected_dead: Optional[str]) -> dict[str, Any]:
    dead = [e.get("worker_id") for e in _log_events(*cp_logs) if e.get("msg") == "worker.dead"]
    runs_per_task = Counter(
        e.get("task_id") for path in worker_logs.values() for e in _log_events(path) if e.get("msg") == "worker.claimed"
    )
    false_deaths = [w for w in dead if w != expected_dead]
    return {
        "false_deaths": len(false_deaths),
        "false_dead_workers": false_deaths,
        "extra_executions": sum(n - 1 for n in runs_per_task.values()),
    }


async def run(args: argparse.Namespace, metrics: dict[str, Any]) -> None:
    settings = load_settings()
    detection_budget = settings.lease_s + settings.monitor_poll_s + 0.5
    recovery_budget = detection_budget + 1.0
    # Lease expiry, one campaign poll, then one reconcile pass to pick the work back up.
    failover_budget = settings.leader_lease_s * 1.25 + 1.0
    metrics.update(lease_s=settings.lease_s, heartbeat_mode=settings.heartbeat_mode)

    redis = Redis.from_url(args.redis_url, decode_responses=True)
    await redis.ping()
    await _clear_aether_keys(redis)
    wal = WriteAheadLog(redis)

    dag_path = Path(args.dag).resolve()
    dag = json.loads(dag_path.read_text())
    workflow_id = dag["workflow_id"]
    killed: Optional[str] = None
    paused: Optional[str] = None

    logdir = Path(tempfile.mkdtemp(prefix="aether-chaos-"))
    env = {**os.environ, "REDIS_URL": args.redis_url, "PYTHONPATH": str(ROOT)}
    procs: dict[str, subprocess.Popen] = {}
    cp_named = {f"control-plane-{i}": logdir / f"control-plane-{i}.log" for i in range(1, args.control_planes + 1)}
    cp_logs = list(cp_named.values())
    worker_logs = {f"worker-{i}": logdir / f"worker-{i}.log" for i in range(1, args.workers + 1)}
    for node_id, log_path in cp_named.items():
        procs[node_id] = _spawn(["control-plane"], {**env, "CONTROL_PLANE_ID": node_id}, log_path)
    for worker_id, log_path in worker_logs.items():
        procs[worker_id] = _spawn(["worker"], {**env, "WORKER_ID": worker_id}, log_path)
    old_leader: Optional[str] = None

    try:
        async def cluster_ready() -> bool:
            events = _log_events(*cp_logs)
            started = any(e.get("msg") == "control_plane.started" for e in events)
            campaigning = {e["_log"] for e in events if e.get("msg") == "control_plane.standby"}
            return (
                started
                and len(campaigning) == args.control_planes
                and await redis.hlen(HEARTBEAT_HASH) >= args.workers
            )

        await _wait_until(cluster_ready, 20, "control plane and workers to start")

        subprocess.run(
            [sys.executable, "-m", "src.main", "submit-workflow", str(dag_path)],
            cwd=ROOT,
            env=env,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        submitted_at = time.monotonic()

        victim: Optional[str] = None
        victim_tasks: list[str] = []
        paused_fences: dict[str, int] = {}
        action_wall = action_mono = 0.0
        action_after = next((s for s in (args.kill_after, args.stop_after, args.pause_after) if s is not None), None)
        if action_after is not None:
            await asyncio.sleep(action_after)
            for attempt in range(PAUSE_ATTEMPTS):
                state = await wal.recover_latest_state(workflow_id)
                status = {t.id: t.status for t in state.workflow.tasks}
                # A worker keeps its lock briefly after committing; only target uncommitted work.
                candidates = {
                    worker: held
                    for worker, tasks_held in (await _busy_workers(redis)).items()
                    if (held := [t for t in tasks_held if status.get(t) != TaskStatus.COMPLETED])
                }
                if not candidates:
                    raise ChaosFailure("no worker was mid-task at that point; use a larger DAG or an earlier time")
                victim = sorted(candidates)[0]
                if args.kill_after is not None:
                    sig = signal.SIGKILL
                elif args.stop_after is not None:
                    sig = signal.SIGTERM
                else:
                    sig = signal.SIGSTOP
                procs[victim].send_signal(sig)
                action_wall, action_mono = time.time(), time.monotonic()
                victim_tasks = candidates[victim]
                if sig == signal.SIGKILL:
                    killed = victim
                    procs[victim].wait()
                elif sig == signal.SIGSTOP:
                    paused = victim
                    paused_fences = {t: await wal.current_fence(t) for t in victim_tasks}
                    # A commit already on the wire when the signal landed can still reach Redis.
                    await asyncio.sleep(0.2)
                if sig in (signal.SIGKILL, signal.SIGSTOP):
                    state = await wal.recover_latest_state(workflow_id)
                    victim_tasks = [
                        t for t in victim_tasks if state.workflow.task_map()[t].status != TaskStatus.COMPLETED
                    ]
                if sig != signal.SIGSTOP or victim_tasks or attempt == PAUSE_ATTEMPTS - 1:
                    break
                # The pause landed after the commit, so there would be no late write to reject.
                procs[victim].send_signal(signal.SIGCONT)
                paused = None
                await asyncio.sleep(0.3)
            print(f"sent {sig.name} to {victim} while it held {', '.join(candidates[victim])}")
            if paused and not victim_tasks:
                raise ChaosFailure(f"every paused worker committed before SIGSTOP landed ({PAUSE_ATTEMPTS} attempts)")

            if paused:
                async def reassigned() -> bool:
                    declared = any(
                        e.get("msg") == "worker.dead" and e.get("worker_id") == paused for e in _log_events(*cp_logs)
                    )
                    if not declared:
                        return False
                    for task_id in victim_tasks:
                        if await wal.current_fence(task_id) <= paused_fences[task_id]:
                            return False
                    return True

                await _wait_until(reassigned, detection_budget + 2.0, f"{paused}'s work to be reassigned")
                procs[paused].send_signal(signal.SIGCONT)
                print(f"sent SIGCONT to {paused} {time.monotonic() - action_mono:.2f}s later, after its work was reassigned")

        leader_after = args.kill_leader_after if args.kill_leader_after is not None else args.pause_leader_after
        leader_sig = signal.SIGKILL if args.kill_leader_after is not None else signal.SIGSTOP
        new_leader: Optional[str] = None
        old_epoch = new_epoch = 0
        failover_after = 0.0
        if leader_after is not None:
            await asyncio.sleep(leader_after)
            token = await redis.get(LEADER_KEY)
            if not token:
                raise ChaosFailure("no control plane held the leader lease")
            old_leader, _, epoch = token.rpartition(":")
            old_epoch = int(epoch)
            procs[old_leader].send_signal(leader_sig)
            action_wall = time.time()
            if leader_sig == signal.SIGKILL:
                procs[old_leader].wait()
            print(f"sent {leader_sig.name} to {old_leader}, the leader (epoch {old_epoch})")

            def elections() -> list[dict]:
                return [
                    e
                    for e in _log_events(*cp_logs)
                    if e.get("msg") == "control_plane.elected" and int(e.get("epoch", 0)) > old_epoch
                ]

            async def taken_over() -> bool:
                return bool(elections())

            await _wait_until(taken_over, failover_budget + 5.0, "a standby to take over")
            first = elections()[0]
            new_leader, new_epoch = first["node_id"], int(first["epoch"])
            failover_after = datetime.fromisoformat(first["ts"]).timestamp() - action_wall
            if leader_sig == signal.SIGSTOP:
                procs[old_leader].send_signal(signal.SIGCONT)
                print(f"sent SIGCONT to {old_leader} after {new_leader} took over")

        recovered_after: Optional[float] = None
        deadline = time.monotonic() + args.timeout
        while True:
            state = await wal.recover_latest_state(workflow_id)
            if state is None:
                raise ChaosFailure("workflow state not found in the WAL")
            tasks = state.workflow.task_map()
            if victim and recovered_after is None and all(
                tasks[t].status == TaskStatus.COMPLETED for t in victim_tasks
            ):
                recovered_after = time.monotonic() - action_mono
            if state.workflow.has_failed():
                failed = [t.id for t in state.workflow.tasks if t.status == TaskStatus.FAILED]
                raise ChaosFailure(f"tasks failed: {failed}")
            if state.workflow.is_complete():
                break
            if time.monotonic() > deadline:
                stuck = {t.id: t.status.value for t in state.workflow.tasks if t.status != TaskStatus.COMPLETED}
                raise ChaosFailure(f"workflow did not complete within {args.timeout}s; unfinished: {stuck}")
            await asyncio.sleep(0.1)
        elapsed = time.monotonic() - submitted_at

        raw = await redis.lrange(f"aether:wal:{workflow_id}", 0, -1)
        commits = Counter(
            d["task_id"] for d in map(json.loads, raw) if d.get("type") == "task_completed"
        )
        duplicates = {t: n for t, n in commits.items() if n != 1}
        if duplicates or len(commits) != len(tasks):
            raise ChaosFailure(f"expected exactly one commit per task; got {dict(commits)}")

        cp_events = _log_events(*cp_logs)
        dead_events = [e for e in cp_events if e.get("msg") == "worker.dead"]
        stats = _liveness_stats(cp_logs, worker_logs, killed or paused)
        metrics.update(
            tasks=len(tasks),
            elapsed_s=round(elapsed, 2),
            recovered_after_s=round(recovered_after, 2) if recovered_after is not None else None,
        )

        print(
            f"PASS  {len(tasks)} tasks, {args.workers} workers, completed in {elapsed:.2f}s, "
            f"one commit per task, {stats['extra_executions']} re-executed"
        )
        if stats["false_deaths"] and not args.allow_false_deaths:
            raise ChaosFailure(f"live workers were declared dead: {stats['false_dead_workers']}")

        if old_leader:
            metrics["failover_after_s"] = round(failover_after, 2)
            if failover_after > failover_budget and not args.measure_only:
                raise ChaosFailure(f"failover took {failover_after:.2f}s (budget {failover_budget:.1f}s)")
            print(
                f"PASS  {new_leader} took over as epoch {new_epoch} {failover_after:.2f}s after "
                f"{leader_sig.name} (budget {failover_budget:.1f}s)"
            )
            deltas = [json.loads(d) for d in raw]
            first_new = next((i for i, d in enumerate(deltas) if d.get("leader_epoch") == new_epoch), None)
            if first_new is None:
                raise ChaosFailure(f"the new leader (epoch {new_epoch}) never wrote to the WAL")
            late = [d for d in deltas[first_new:] if d.get("leader_epoch") is not None and d["leader_epoch"] < new_epoch]
            if late:
                raise ChaosFailure(f"{len(late)} writes from a deposed leader landed after the new leader's: {late[:3]}")
            old_writes = sum(1 for d in deltas[:first_new] if d.get("leader_epoch") == old_epoch)
            print(
                f"PASS  WAL: {old_writes} writes from epoch {old_epoch}, then only epoch {new_epoch}; "
                "nothing from the old leader landed after the takeover"
            )

            if leader_sig == signal.SIGSTOP:
                old_log = cp_named[old_leader]

                def after_takeover(msg: str) -> list[dict]:
                    return [
                        e
                        for e in _log_events(old_log)
                        if e.get("msg") == msg and datetime.fromisoformat(e["ts"]).timestamp() > action_wall
                    ]

                async def back_on_standby() -> bool:
                    return bool(after_takeover("control_plane.lost_leadership")) and bool(
                        after_takeover("control_plane.standby")
                    )

                await _wait_until(back_on_standby, ZOMBIE_TIMEOUT_S, f"{old_leader} to step down after SIGCONT")
                if procs[old_leader].poll() is not None:
                    raise ChaosFailure(f"{old_leader} exited instead of standing by")
                reason = after_takeover("control_plane.lost_leadership")[0].get("reason")
                rejected = len(after_takeover("control_plane.write_rejected"))
                print(
                    f"PASS  {old_leader} woke up, stepped down ({reason}, late writes rejected: {rejected}) "
                    "and is standing by"
                )

        if killed or paused:
            lost = killed or paused
            how = "SIGKILL" if killed else "SIGSTOP"
            dead = [e for e in dead_events if e.get("worker_id") == lost]
            if not dead:
                raise ChaosFailure(f"control plane never declared {lost} dead")
            detected_after = datetime.fromisoformat(dead[0]["ts"]).timestamp() - action_wall
            metrics["detected_after_s"] = round(detected_after, 2)
            moved = [t for t in victim_tasks if tasks[t].assigned_worker == killed]
            if moved:
                raise ChaosFailure(f"tasks still attributed to the killed worker: {moved}")
            if detected_after > detection_budget and not args.measure_only:
                raise ChaosFailure(f"failure detection took {detected_after:.2f}s (budget {detection_budget:.1f}s)")
            print(f"PASS  {lost} detected dead {detected_after:.2f}s after {how} (budget {detection_budget:.1f}s)")
            if not victim_tasks:
                print(f"NOTE  {lost} committed its task before the signal landed; nothing to recover")
            elif recovered_after is None or (recovered_after > recovery_budget and not args.measure_only):
                raise ChaosFailure(f"in-flight work took {recovered_after}s to recover (budget {recovery_budget:.1f}s)")
            else:
                where = "on a survivor" if killed else "under a new fence"
                print(
                    f"PASS  its in-flight work finished {where} {recovered_after:.2f}s after {how} "
                    f"(budget {recovery_budget:.1f}s)"
                )

        if paused and victim_tasks:
            def stale_writes() -> list[dict]:
                return [
                    e
                    for e in _log_events(worker_logs[paused])
                    if e.get("msg") in STALE_WRITE_EVENTS and e.get("task_id") in victim_tasks
                ]

            async def zombie_rejected() -> bool:
                return {e["task_id"] for e in stale_writes()} >= set(victim_tasks)

            await _wait_until(zombie_rejected, ZOMBIE_TIMEOUT_S, f"{paused} to attempt its late write after SIGCONT")
            if procs[paused].poll() is not None:
                raise ChaosFailure(f"{paused} exited after its write was rejected (code {procs[paused].returncode})")
            committed = {
                d["task_id"]: d for d in map(json.loads, raw) if d.get("type") == "task_completed"
            }
            for task_id in victim_tasks:
                commit = committed[task_id]
                if commit["fence_token"] <= paused_fences[task_id]:
                    raise ChaosFailure(f"{task_id} was committed with the paused worker's stale fence: {commit}")
                by = commit["assigned_worker"]
                if by == paused:
                    by = f"{paused} itself after it rejoined"
                print(
                    f"PASS  {paused} woke up and tried to write {task_id} with fence {paused_fences[task_id]}; "
                    f"rejected. Committed once, with fence {commit['fence_token']}, by {by}"
                )

        if args.stop_after is not None and victim:
            try:
                code = procs[victim].wait(timeout=DRAIN_TIMEOUT_S)
            except subprocess.TimeoutExpired:
                raise ChaosFailure(f"{victim} did not exit within {DRAIN_TIMEOUT_S}s of SIGTERM")
            if code != 0:
                raise ChaosFailure(f"{victim} exited with code {code} after SIGTERM")
            reassigned = [e for e in cp_events if e.get("msg") == "task.reassigned"]
            if reassigned:
                raise ChaosFailure(f"draining should not reassign work; saw {len(reassigned)} reassignments")
            finished_elsewhere = [t for t in victim_tasks if tasks[t].assigned_worker != victim]
            if finished_elsewhere:
                raise ChaosFailure(f"drained worker did not finish its own tasks: {finished_elsewhere}")
            print(f"PASS  {victim} drained: finished {', '.join(victim_tasks)} itself, exited 0, nothing reassigned")
    except Exception:
        print(f"logs: {logdir}")
        for log_path in cp_logs:
            print(f"  [{log_path.stem}]")
            for line in log_path.read_text().splitlines()[-30 // len(cp_logs):]:
                print(f"  {line}")
        raise
    finally:
        for name in (paused, old_leader):
            if name and procs[name].poll() is None:
                procs[name].send_signal(signal.SIGCONT)
        for proc in procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in procs.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        await redis.aclose()
        stats = _liveness_stats(cp_logs, worker_logs, killed or paused)
        metrics.update(false_deaths=stats["false_deaths"], extra_executions=stats["extra_executions"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dag", default=str(ROOT / "chaos_dag.json"))
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--control-planes", type=int, default=1, help="one leads, the rest stand by")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--kill-after", type=float, default=None, help="seconds after submit to SIGKILL a busy worker")
    action.add_argument("--stop-after", type=float, default=None, help="seconds after submit to SIGTERM a busy worker")
    action.add_argument(
        "--pause-after",
        type=float,
        default=None,
        help="seconds after submit to SIGSTOP a busy worker, resuming it once its work is reassigned",
    )
    action.add_argument("--kill-leader-after", type=float, default=None, help="seconds after submit to SIGKILL the leader")
    action.add_argument(
        "--pause-leader-after",
        type=float,
        default=None,
        help="seconds after submit to SIGSTOP the leader, resuming it once a standby takes over",
    )
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="environment override for every process, e.g. AETHER_LEASE_S=1.5",
    )
    parser.add_argument("--allow-false-deaths", action="store_true", help="report false deaths instead of failing")
    parser.add_argument(
        "--measure-only",
        action="store_true",
        help="report detection and recovery times without enforcing the budgets (correctness is still checked)",
    )
    parser.add_argument("--json", type=Path, default=None, help="write run metrics to this file")
    args = parser.parse_args()
    if (args.kill_leader_after is not None or args.pause_leader_after is not None) and args.control_planes < 2:
        parser.error("leader failover needs --control-planes 2 or more")

    for item in args.set:
        key, _, value = item.partition("=")
        os.environ[key] = value

    metrics: dict[str, Any] = {"passed": False}
    code = 0
    try:
        asyncio.run(run(args, metrics))
        metrics["passed"] = True
    except ChaosFailure as exc:
        print(f"FAIL  {exc}")
        metrics["error"] = str(exc)
        code = 1
    if args.json:
        args.json.write_text(json.dumps(metrics))
    return code


if __name__ == "__main__":
    sys.exit(main())

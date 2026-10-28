"""End-to-end chaos test: a real control plane and workers against a live Redis.

Exits non-zero unless every task completes exactly once. With --kill-after, a
worker that is mid-task is SIGKILLed and its work must finish on a survivor
within the recovery budget.
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
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from redis.asyncio import Redis  # noqa: E402

from src.common.models import TaskStatus  # noqa: E402
from src.control_plane.heartbeat import HEARTBEAT_HASH  # noqa: E402
from src.control_plane.wal import WriteAheadLog  # noqa: E402

# Lease TTL (3s) + monitor poll (0.5s), then up to one task (0.5s) waiting for a free worker
# and one task (0.5s) of re-execution.
DETECTION_BUDGET_S = 4.0
RECOVERY_BUDGET_S = 5.0

logging.getLogger("aether.wal").setLevel(logging.WARNING)


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


def _log_events(log_path: Path) -> list[dict]:
    events = []
    for line in log_path.read_text().splitlines():
        if line.startswith("{"):
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


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


async def run(args: argparse.Namespace) -> None:
    redis = Redis.from_url(args.redis_url, decode_responses=True)
    await redis.ping()
    await _clear_aether_keys(redis)
    wal = WriteAheadLog(redis)

    dag_path = Path(args.dag).resolve()
    workflow_id = json.loads(dag_path.read_text())["workflow_id"]

    logdir = Path(tempfile.mkdtemp(prefix="aether-chaos-"))
    env = {**os.environ, "REDIS_URL": args.redis_url, "PYTHONPATH": str(ROOT)}
    procs: dict[str, subprocess.Popen] = {}
    cp_log = logdir / "control-plane.log"
    procs["control-plane"] = _spawn(["control-plane"], env, cp_log)
    for i in range(1, args.workers + 1):
        worker_id = f"worker-{i}"
        procs[worker_id] = _spawn(["worker"], {**env, "WORKER_ID": worker_id}, logdir / f"{worker_id}.log")

    try:
        async def cluster_ready() -> bool:
            started = any(e.get("msg") == "control_plane.started" for e in _log_events(cp_log))
            return started and await redis.hlen(HEARTBEAT_HASH) >= args.workers

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
        kill_wall = 0.0
        kill_mono = 0.0
        if args.kill_after is not None:
            await asyncio.sleep(args.kill_after)
            busy = await _busy_workers(redis)
            if not busy:
                raise ChaosFailure("no worker was mid-task at kill time; use a larger DAG or an earlier --kill-after")
            victim, victim_tasks = next(iter(sorted(busy.items())))
            procs[victim].send_signal(signal.SIGKILL)
            kill_wall, kill_mono = time.time(), time.monotonic()
            print(f"killed {victim} while it held {', '.join(victim_tasks)}")

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
                recovered_after = time.monotonic() - kill_mono
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

        print(f"PASS  {len(tasks)} tasks, {args.workers} workers, completed in {elapsed:.2f}s, one commit per task")

        if victim:
            dead = [
                e for e in _log_events(cp_log)
                if e.get("msg") == "worker.dead" and e.get("worker_id") == victim
            ]
            if not dead:
                raise ChaosFailure(f"control plane never declared {victim} dead")
            detected_after = datetime.fromisoformat(dead[0]["ts"]).timestamp() - kill_wall
            moved = [t for t in victim_tasks if tasks[t].assigned_worker == victim]
            if moved:
                raise ChaosFailure(f"tasks still attributed to the killed worker: {moved}")
            if detected_after > DETECTION_BUDGET_S:
                raise ChaosFailure(f"failure detection took {detected_after:.2f}s (budget {DETECTION_BUDGET_S}s)")
            if recovered_after is None or recovered_after > RECOVERY_BUDGET_S:
                raise ChaosFailure(f"in-flight work took {recovered_after}s to recover (budget {RECOVERY_BUDGET_S}s)")
            print(f"PASS  {victim} detected dead {detected_after:.2f}s after SIGKILL")
            print(f"PASS  its in-flight work finished on a survivor {recovered_after:.2f}s after SIGKILL")
    except Exception:
        print(f"logs: {logdir}")
        for line in cp_log.read_text().splitlines()[-30:]:
            print(f"  {line}")
        raise
    finally:
        for proc in procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in procs.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        await redis.aclose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dag", default=str(ROOT / "chaos_dag.json"))
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--kill-after", type=float, default=None, help="seconds after submit to SIGKILL a busy worker")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except ChaosFailure as exc:
        print(f"FAIL  {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

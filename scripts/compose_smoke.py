"""Docker Compose smoke test: the README quick start, plus a worker kill and a leader kill.

Brings up Redis, two control planes, and three workers with `docker compose`, submits
failover_dag.json the way the README does, `docker kill`s a busy worker and then the
leading control plane, and exits non-zero unless the workflow completes with exactly one
commit per task. Tears the stack down afterwards.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from redis.asyncio import Redis  # noqa: E402

from src.common.models import TaskStatus  # noqa: E402
from src.control_plane.heartbeat import HEARTBEAT_HASH  # noqa: E402
from src.control_plane.leader import LEADER_KEY  # noqa: E402
from src.control_plane.wal import WriteAheadLog  # noqa: E402

DAG = "failover_dag.json"


class SmokeFailure(Exception):
    pass


def compose(*args: str, capture: bool = False) -> str:
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
    )
    return result.stdout or ""


def container_for(service: str, hostname: str) -> str:
    """Compose sets each container's hostname to its short container ID."""
    for container_id in compose("ps", "-q", service, capture=True).split():
        if container_id.startswith(hostname):
            return container_id
    raise SmokeFailure(f"no {service} container with hostname {hostname}")


async def wait_until(predicate, timeout_s: float, what: str) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.25)
    raise SmokeFailure(f"timed out waiting for {what}")


async def run(args: argparse.Namespace) -> None:
    redis = Redis.from_url(args.redis_url, decode_responses=True)
    wal = WriteAheadLog(redis)
    workflow_id = json.loads((ROOT / DAG).read_text())["workflow_id"]
    try:
        async def cluster_ready() -> bool:
            try:
                return bool(await redis.get(LEADER_KEY)) and await redis.hlen(HEARTBEAT_HASH) >= 3
            except OSError:
                return False

        await wait_until(cluster_ready, 60, "a leader and three worker heartbeats")
        print("cluster up: 2 control planes, 3 workers")

        compose("run", "--rm", "control-plane", "python", "-m", "src.main", "submit-workflow", DAG)
        submitted = time.monotonic()

        await asyncio.sleep(args.kill_after)
        busy = [await redis.get(key) async for key in redis.scan_iter(match="aether:lock:*")]
        busy = [holder for holder in busy if holder]
        if not busy:
            raise SmokeFailure("no worker was mid-task to kill")
        worker_id = sorted(busy)[0].rsplit(":", 1)[0]
        worker_host = worker_id.rsplit("-", 2)[0]
        subprocess.run(["docker", "kill", container_for("worker", worker_host)], check=True, stdout=subprocess.DEVNULL)
        print(f"docker kill worker {worker_id}")

        await asyncio.sleep(args.leader_after)
        token = await redis.get(LEADER_KEY)
        if not token:
            raise SmokeFailure("no leader to kill")
        node_id, _, epoch = token.rpartition(":")
        leader_host = node_id.rsplit("-", 1)[0]
        subprocess.run(
            ["docker", "kill", container_for("control-plane", leader_host)], check=True, stdout=subprocess.DEVNULL
        )
        print(f"docker kill leader {node_id} (epoch {epoch})")

        async def new_leader() -> bool:
            current = await redis.get(LEADER_KEY)
            return bool(current) and int(current.rpartition(":")[2]) > int(epoch)

        await wait_until(new_leader, 15, "a standby to take over")
        print(f"standby took over: {await redis.get(LEADER_KEY)}")

        async def complete() -> bool:
            state = await wal.recover_latest_state(workflow_id)
            if state and state.workflow.has_failed():
                failed = [t.id for t in state.workflow.tasks if t.status == TaskStatus.FAILED]
                raise SmokeFailure(f"tasks failed: {failed}")
            return bool(state) and state.workflow.is_complete()

        await wait_until(complete, args.timeout, f"{workflow_id} to complete")
        elapsed = time.monotonic() - submitted

        raw = await redis.lrange(f"aether:wal:{workflow_id}", 0, -1)
        commits = Counter(d["task_id"] for d in map(json.loads, raw) if d.get("type") == "task_completed")
        tasks = json.loads((ROOT / DAG).read_text())["tasks"]
        duplicates = {t: n for t, n in commits.items() if n != 1}
        if duplicates or len(commits) != len(tasks):
            raise SmokeFailure(f"expected exactly one commit per task; got {len(commits)} tasks, duplicates {duplicates}")
        print(f"PASS  {len(tasks)} tasks completed in {elapsed:.1f}s with one commit each, after a worker kill and a leader kill")
    finally:
        await redis.aclose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--kill-after", type=float, default=3.0, help="seconds after submit to kill a busy worker")
    parser.add_argument("--leader-after", type=float, default=2.0, help="seconds after that to kill the leader")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--keep", action="store_true", help="leave the stack running afterwards")
    args = parser.parse_args()

    compose("up", "--build", "-d", "--scale", "control-plane=2", "--scale", "worker=3")
    code = 0
    try:
        asyncio.run(run(args))
    except SmokeFailure as exc:
        print(f"FAIL  {exc}")
        compose("logs", "--tail", "40", "control-plane")
        code = 1
    finally:
        if not args.keep:
            compose("down", "-v", "--remove-orphans")
    return code


if __name__ == "__main__":
    sys.exit(main())

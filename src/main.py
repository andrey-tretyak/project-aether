from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
import sys
from pathlib import Path

from src.common.config import load_settings
from src.common.logger import get_logger
from src.common.models import DAGWorkflow
from src.common.redis_client import create_redis, create_sync_redis
from src.control_plane.leader import LeaderElection
from src.control_plane.scheduler import Scheduler
from src.worker.executor import WorkerExecutor

log = get_logger("aether.main")


async def _first(*aws: asyncio.Future | asyncio.Task) -> None:
    """Wait for the first of `aws` to finish and cancel the rest."""
    tasks = [asyncio.ensure_future(a) for a in aws]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def run_control_plane() -> None:
    """Stand by until elected, lead until the lease is lost, then stand by again."""
    redis = create_redis()
    settings = load_settings()
    node_id = os.environ.get("CONTROL_PLANE_ID") or f"{socket.gethostname()}-{os.getpid()}"
    election = LeaderElection(redis, node_id, settings)
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stopping.set)
    try:
        while not stopping.is_set():
            await _first(election.campaign(), stopping.wait())
            if stopping.is_set():
                break
            scheduler = Scheduler(redis, settings, leader=election)
            await scheduler.start()
            try:
                await _first(scheduler.consume_results(), election.hold(), stopping.wait())
            finally:
                await scheduler.stop()
    finally:
        if election.token is not None:
            await election.resign()
        await redis.aclose()


async def run_worker() -> None:
    redis = create_redis()
    heartbeat_client = create_sync_redis()
    worker = WorkerExecutor(redis, heartbeat_client=heartbeat_client)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.request_stop)
    try:
        await worker.run()
    finally:
        heartbeat_client.close()
        await redis.aclose()


async def submit_workflow(path: str) -> None:
    payload = json.loads(Path(path).read_text())
    workflow = DAGWorkflow.model_validate(payload)
    redis = create_redis()
    scheduler = Scheduler(redis)
    await scheduler.submit_workflow(workflow, dispatch=False)
    log.info(
        "workflow.submitted",
        extra={"extra_fields": {"workflow_id": workflow.workflow_id, "tasks": len(workflow.tasks)}},
    )
    await redis.aclose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aether", description="Project Aether cluster CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("control-plane", help="Run the scheduler, WAL, and heartbeat monitor")
    sub.add_parser("worker", help="Run a worker executor")
    submit = sub.add_parser("submit-workflow", help="Submit a DAG JSON file")
    submit.add_argument("path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "control-plane":
        asyncio.run(run_control_plane())
    elif args.command == "worker":
        asyncio.run(run_worker())
    elif args.command == "submit-workflow":
        asyncio.run(submit_workflow(args.path))
    else:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

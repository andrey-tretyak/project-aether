"""Check a finished workflow's write-ahead log against the guarantees Aether makes.

The log records the fence on every task write and the leader epoch on every
control-plane write, so a run can be audited from the log alone, whatever
faults it went through.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

FENCE_ISSUING = {"task_dispatched", "task_reassigned"}


def _issued_by_control_plane(delta: dict[str, Any]) -> bool:
    kind = delta.get("type")
    # Retry exhaustion is the control plane's task_failed; a worker's always names the worker.
    return kind in FENCE_ISSUING or (kind == "task_failed" and delta.get("assigned_worker") is None)


def audit_wal(deltas: list[dict[str, Any]], dependencies: dict[str, list[str]]) -> list[str]:
    """Return every violation found; an empty list means the run kept its guarantees.

    `dependencies` maps each task in the workflow to the tasks it depends on.
    """
    violations: list[str] = []
    issued: dict[str, int] = {}
    committed: set[str] = set()
    commits: Counter[str] = Counter()
    newest_epoch = 0

    for index, delta in enumerate(deltas):
        kind = delta.get("type")
        task_id = delta.get("task_id")
        where = f"WAL #{index} {kind} {task_id or ''}".rstrip()

        epoch = delta.get("leader_epoch")
        if epoch is not None:
            if epoch < newest_epoch:
                violations.append(f"{where}: written by leader epoch {epoch} after epoch {newest_epoch} had written")
            newest_epoch = max(newest_epoch, epoch)

        if task_id is None:
            continue
        fence = delta.get("fence_token")

        if _issued_by_control_plane(delta):
            if task_id in committed:
                violations.append(f"{where}: the control plane touched a task that had already committed")
            if fence is None or fence <= issued.get(task_id, 0):
                violations.append(f"{where}: fence {fence} does not advance past {issued.get(task_id, 0)}")
            else:
                issued[task_id] = fence
            if kind == "task_dispatched":
                waiting = [dep for dep in dependencies.get(task_id, []) if dep not in committed]
                if waiting:
                    violations.append(f"{where}: dispatched before its dependencies committed: {waiting}")
            continue

        if task_id in committed:
            violations.append(f"{where}: a worker wrote to a task that had already committed")
        if fence != issued.get(task_id):
            violations.append(f"{where}: fence {fence} is not the current fence {issued.get(task_id)}")
        if kind == "task_completed":
            committed.add(task_id)
            commits[task_id] += 1

    for task_id in dependencies:
        if commits[task_id] != 1:
            violations.append(f"{task_id}: committed {commits[task_id]} times, expected exactly once")
    return violations

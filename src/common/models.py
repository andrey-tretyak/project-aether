from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskNode(BaseModel):
    id: str
    agent_prompt: str
    dependencies: list[str] = Field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    fence_token: int = 0
    payload: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] | None = None
    assigned_worker: str | None = None
    error: str | None = None

    model_config = {"use_enum_values": False}


class DAGWorkflow(BaseModel):
    workflow_id: str
    tasks: list[TaskNode] = Field(default_factory=list)

    def task_map(self) -> dict[str, TaskNode]:
        return {task.id: task for task in self.tasks}

    def ready_tasks(self) -> list[TaskNode]:
        """Tasks whose dependencies are COMPLETED and that are still PENDING."""
        by_id = self.task_map()
        ready: list[TaskNode] = []
        for task in self.tasks:
            if task.status != TaskStatus.PENDING:
                continue
            deps_done = all(
                by_id[dep].status == TaskStatus.COMPLETED
                for dep in task.dependencies
                if dep in by_id
            )
            if deps_done:
                ready.append(task)
        return ready

    def is_complete(self) -> bool:
        return all(t.status == TaskStatus.COMPLETED for t in self.tasks)

    def has_failed(self) -> bool:
        return any(t.status == TaskStatus.FAILED for t in self.tasks)

    def missing_dependencies(self) -> list[str]:
        ids = {t.id for t in self.tasks}
        missing: list[str] = []
        for task in self.tasks:
            for dep in task.dependencies:
                if dep not in ids:
                    missing.append(f"{task.id}->{dep}")
        return missing

    def has_cycle(self) -> bool:
        by_id = self.task_map()
        visiting: set[str] = set()
        visited: set[str] = set()

        def dfs(node_id: str) -> bool:
            if node_id in visiting:
                return True
            if node_id in visited or node_id not in by_id:
                return False
            visiting.add(node_id)
            for dep in by_id[node_id].dependencies:
                if dfs(dep):
                    return True
            visiting.remove(node_id)
            visited.add(node_id)
            return False

        return any(dfs(task.id) for task in self.tasks)


class WorkflowState(BaseModel):
    """Serializable cluster snapshot used by WAL checkpoints."""

    workflow: DAGWorkflow
    wal_seq: int = 0

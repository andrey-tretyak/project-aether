from src.common.models import DAGWorkflow, TaskNode, TaskStatus


def _workflow() -> DAGWorkflow:
    return DAGWorkflow(
        workflow_id="w1",
        tasks=[
            TaskNode(id="a", agent_prompt="A"),
            TaskNode(id="b", agent_prompt="B", dependencies=["a"]),
            TaskNode(id="c", agent_prompt="C", dependencies=["a"]),
            TaskNode(id="d", agent_prompt="D", dependencies=["b", "c"]),
        ],
    )


def test_initially_only_roots_are_ready():
    dag = _workflow()
    assert [t.id for t in dag.ready_tasks()] == ["a"]


def test_downstream_unlocks_after_dependencies_complete():
    dag = _workflow()
    dag.task_map()["a"].status = TaskStatus.COMPLETED
    assert {t.id for t in dag.ready_tasks()} == {"b", "c"}


def test_join_node_waits_for_all_parents():
    dag = _workflow()
    dag.task_map()["a"].status = TaskStatus.COMPLETED
    dag.task_map()["b"].status = TaskStatus.COMPLETED
    assert [t.id for t in dag.ready_tasks()] == ["c"]
    dag.task_map()["c"].status = TaskStatus.COMPLETED
    assert [t.id for t in dag.ready_tasks()] == ["d"]


def test_complete_and_failed_flags():
    dag = _workflow()
    assert not dag.is_complete()
    assert not dag.has_failed()
    for task in dag.tasks:
        task.status = TaskStatus.COMPLETED
    assert dag.is_complete()
    dag.tasks[-1].status = TaskStatus.FAILED
    assert dag.has_failed()
    assert not dag.is_complete()


def test_cycle_detection():
    dag = DAGWorkflow(
        workflow_id="cyclic",
        tasks=[
            TaskNode(id="a", agent_prompt="A", dependencies=["b"]),
            TaskNode(id="b", agent_prompt="B", dependencies=["a"]),
        ],
    )
    assert dag.has_cycle()
    assert not _workflow().has_cycle()


def test_missing_dependencies():
    dag = DAGWorkflow(
        workflow_id="broken",
        tasks=[TaskNode(id="a", agent_prompt="A", dependencies=["ghost"])],
    )
    assert dag.missing_dependencies() == ["a->ghost"]

# Project Aether

Local, fault-tolerant DAG orchestration for multi-agent workflows. One control plane, Redis WAL + fence tokens, and mock workers so you can chaos-test recovery without LLM API spend.

## What it guarantees

- **At-least-once task execution** across worker crashes
- **WAL snapshot + replay** so state is recovered from Redis, not by re-prompting
- **Fence tokens** so a zombie worker cannot commit after its task was reassigned
- **Sub-3s worker failure detection** (500ms heartbeat, 3s lease timeout)

## Layout

```
src/common/           models, logging, Redis client
src/control_plane/    scheduler, WAL, heartbeat monitor
src/worker/           executor + deterministic mock agent
tests/                DAG resolution and chaos/recovery tests
```

## Local cluster

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=. pytest
```

```sh
docker compose up --build --scale worker=3 -d
docker compose run --rm control-plane python -m src.main submit-workflow sample_dag.json
docker compose logs -f control-plane worker
```

## Chaos test

```sh
docker compose up --build --scale worker=3 -d
docker compose run --rm control-plane python -m src.main submit-workflow sample_dag.json
sleep 2 && docker kill $(docker compose ps -q worker | head -n 1)
docker compose logs control-plane | grep -E "Recovering state|task.reassigned|worker.dead"
```

Expect the control plane to mark the worker dead, bump the fence token, requeue the in-flight task, and finish the DAG on remaining workers.

## Interview framing

WAL + fences give *effective exactly-once state transitions* even though execution is at-least-once: replay reads cached mock/LLM outputs from the log, and stale writers are rejected when a newer fence exists.

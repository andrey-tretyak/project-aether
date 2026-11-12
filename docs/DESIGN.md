# Aether design notes

The README says what Aether survives. This document says how, and why each mechanism looks the way it does. Every claim here is checked by a test; the relevant one is named in each section.

- [State: log first](#state-log-first)
- [Workers: leases, fences, and zombies](#workers-leases-fences-and-zombies)
- [Choosing the lease](#choosing-the-lease)
- [Control plane: one dispatcher, then failover](#control-plane-one-dispatcher-then-failover)
- [Side effects: idempotency keys](#side-effects-idempotency-keys)
- [Trade-offs](#trade-offs)
- [Testing](#testing)

---

## State: log first

Every dispatch, claim, result, and reassignment is an append-only delta in a Redis list (`aether:wal:{workflow_id}`). Workflow state is never stored anywhere else: it is a snapshot plus the deltas after it, and `recover_latest_state` rebuilds it by replaying.

**Snapshots record exactly how many WAL entries they reflect** (`wal_index`). Recovery replays from that index, so a commit that lands while the control plane is checkpointing is replayed on the next recovery instead of being skipped. An earlier version derived the index after the fact and lost updates under load; the 100-task real-process run found it. (`test_checkpoint_keeps_commit_that_lands_after_recover`)

**Recovery never re-prompts a model.** LLM and tool output is data in the log, so replay is deterministic even when the original step was not.

## Workers: leases, fences, and zombies

**Liveness.** Each worker renews a lease key (`aether:lease:{worker}`, 1.5s TTL) every 500ms. The control plane scans for expired leases every 500ms and declares those workers dead.

**Reassignment.** When a worker is declared dead, the control plane bumps the fence of each task it held, sets the task back to `PENDING`, deletes its lock, and dispatches it at the *front* of the queue, so recovered work does not wait behind the backlog. Each reassignment counts as a retry; after 3 the task is marked `FAILED` with the reason in the WAL, instead of a poison task taking down the fleet one worker at a time. (`test_worker_failure_reassigns_running_task`, `test_retried_task_jumps_the_queue`, `test_task_fails_after_max_retries`)

**Fencing.** Every task has a monotonic fence (`aether:fence:{task_id}`), and every envelope carries the fence it was dispatched with. `WriteAheadLog.append_delta` checks the delta's fence against the current one inside a `WATCH`/`MULTI` transaction and raises `StaleFenceError` if it is behind, so the check and the append are atomic. A worker that was declared dead and then wakes up (a GC pause, a frozen VM, a partition) cannot commit. (`test_stale_fence_is_rejected`)

The fence goes up twice on reassignment: once when the old holder is declared dead, invalidating it immediately, and once when the task is dispatched again.

The real-process version of this test freezes a busy worker with `SIGSTOP`, waits until the control plane has declared it dead and reassigned its task, then resumes it with `SIGCONT`:

```
sent SIGSTOP to worker-1 while it held step-18
sent SIGCONT to worker-1 2.04s later, after its work was reassigned
PASS  61 tasks, 3 workers, completed in 7.05s, one commit per task, 1 re-executed
PASS  worker-1 detected dead 1.99s after SIGSTOP (budget 2.5s)
PASS  its in-flight work finished under a new fence 2.05s after SIGSTOP (budget 3.5s)
PASS  worker-1 woke up and tried to write step-18 with fence 1; rejected. Committed once, with fence 3, by worker-3
```

Sometimes the resumed worker pops the reassigned envelope itself and commits it legitimately under the new fence; the test checks the fence on the commit, not who made it. If the worker commits in the milliseconds between being chosen and being frozen, the harness resumes it and tries another, and fails rather than passing without exercising the case.

**Orphans.** A task that is `RUNNING` in the log but neither waiting in the queue nor held by a live worker (for example, a worker died between `BRPOP` and taking the lock) is requeued after one lease. Tasks simply waiting behind a backlog are left alone. (`test_orphan_reclaim_ignores_tasks_still_in_queue`, `test_orphan_reclaim_requeues_lost_envelope`)

**Planned shutdown.** `SIGTERM` drains: the worker stops taking work, finishes and commits what it holds (refreshing its locks meanwhile), and deregisters, so nothing is reassigned.

## Choosing the lease

A short lease recovers faster, but a worker whose event loop is blocked (a synchronous tool call, a long JSON parse) stops renewing and gets declared dead while still working. So Aether renews the lease from a dedicated thread with its own Redis connection, and a busy loop no longer looks like a dead process. The thread stops renewing if the loop has made no progress for 30s, so a truly wedged worker is still reaped. (`test_thread_renews_lease_while_the_loop_is_healthy`, `test_thread_lets_the_lease_lapse_when_the_loop_is_wedged`)

`scripts/lease_sweep.py` measures the trade-off: 15% of tasks block the event loop for 1.2s, and one worker is `SIGKILL`ed 2s into a 61-task run. Five runs per cell:

| Lease | Heartbeat | Workflows completed | Recovery after kill (median / worst) | Live workers declared dead | Repeated executions |
| --- | --- | --- | --- | --- | --- |
| 1s | event loop | 0/5 | n/a | 25 | 25 |
| 1s | thread | 5/5 | 2.87s / 3.44s | 0 | 5 |
| 1.5s | event loop | 5/5 | 2.17s / 3.42s | 12 | 20 |
| **1.5s** | **thread** | **5/5** | **2.23s / 3.62s** | **0** | **5** |
| 2s | event loop | 5/5 | 2.70s / 3.92s | 0 | 5 |
| 2s | thread | 5/5 | 3.50s / 3.93s | 0 | 5 |
| 3s | event loop | 5/5 | 4.68s / 5.30s | 0 | 5 |
| 3s | thread | 5/5 | 4.67s / 4.93s | 0 | 5 |

The five repeated executions in the thread rows are the killed worker's tasks, one per run. With heartbeats on the event loop and a 1s lease, busy workers were declared dead so often that one task hit the retry cap and every workflow failed. With heartbeats on a thread, no live worker was ever declared dead. Below 2s, recovery time is mostly spent re-running the task, so a 1s lease buys nothing over 1.5s. The default is 1.5s with a thread: three heartbeats, so a worker can miss two renewals before it is declared dead.

All timings live in `config/cluster.yaml` and can be overridden with `AETHER_*` environment variables.

## Control plane: one dispatcher, then failover

**One dispatcher.** Four paths can move a task: a result arriving, a worker dying, an orphan sweep, and a new submission. They all run inside the control plane under one `asyncio.Lock`, so each transition starts from the latest WAL state and two never race to bump the same fence. A 1s reconcile loop re-checks every active workflow, so a missed wake-up or a restart costs at most a second rather than a stuck DAG. Clients only append the submission; they never dispatch. The lease sweep found the race this replaced: concurrent paths crashed the control plane with `StaleFenceError` and left tasks stuck in `PENDING`. (`test_concurrent_recovery_paths_reassign_a_task_once`, which fails without the lock)

**Failover.** Control planes compete for one Redis key, `aether:leader`, set with `NX` and a 2s TTL; the winner renews it four times per lease, and the others poll. All scheduler state is in the WAL, so a new leader needs no handoff: its reconcile loop replays each active workflow and dispatches whatever is ready. Workers only talk to Redis, so they keep running through the gap. On `SIGTERM` the leader deletes its own key, so a standby takes over immediately.

**The deposed leader.** The hard case is a leader that was paused rather than killed: it wakes up still believing it leads. Every election increments an epoch, and the key holds `node:epoch`. Every control-plane write (fence bump, WAL append, snapshot) runs in a `WATCH`/`MULTI` transaction that first checks the key still holds this node's token, so a deposed leader's write fails with `NotLeaderError` and the node steps down to standby. This is the fence-token idea applied to the leader. A dispatch's WAL entry and its queue push share one transaction, so a deposed leader cannot enqueue a duplicate either. (`test_deposed_leader_cannot_write`, `test_leader_dispatch_is_atomic_and_records_the_epoch`)

Every control-plane delta records its epoch, which makes a takeover auditable from the log alone. The chaos test kills or freezes the leader partway through `failover_dag.json` (12 chains of 8 dependent steps, so the leader keeps dispatching for ~10s) and checks that no write from the old epoch lands after the new leader's first:

```
sent SIGKILL to control-plane-2, the leader (epoch 1)
PASS  97 tasks, 3 workers, completed in 10.39s, one commit per task, 0 re-executed
PASS  control-plane-1 took over as epoch 2 1.53s after SIGKILL (budget 3.5s)
PASS  WAL: 42 writes from epoch 1, then only epoch 2; nothing from the old leader landed after the takeover
```

The same DAG takes about 9.8s with no failure, so a takeover costs roughly one lease. In the frozen-leader variant, the resumed leader usually notices on its first renewal and steps down before attempting a write; when it does write first, the write is rejected.

## Side effects: idempotency keys

Execution is at-least-once, so a task that sends a message or charges a card can run twice: once on the worker that was declared dead, once on the worker that took over. Fences keep committed *state* correct, but they cannot recall an effect that already reached the outside world.

Tasks declare effects in their payload (`{"effects": [{"kind": "notify", ...}]}`), and the worker sends each one after the agent step and before committing, with a key derived from `(workflow, task, effect index)`. The key never includes the fence or the attempt, so every execution of the same effect presents the same key. The receiving API (`MockExternalAPI`, which follows Stripe's rules) performs the first request it sees for a key, answers repeats with the stored receipt, and rejects the key if it comes back with a different request. Both executions get the same receipt, and only one is committed. (`tests/test_effects.py`)

Checking the fence before calling the API would narrow the window but not close it: the worker can be paused between the check and the call. The key closes it. The chaos test checks the API's ledger after every run, and a control run in CI turns the keys off and requires a duplicate to appear, which proves the check can catch the bug:

```
PASS  61 external effects, each performed exactly once; 1 repeated calls answered from the idempotency store

# same scenario, AETHER_IDEMPOTENCY_KEYS=false
PASS  control: without idempotency keys, effects ran more than once: step-18#0 x2
```

This is exactly-once only where the far side honors idempotency keys. An API that does not would need an outbox or after-the-fact reconciliation.

## Trade-offs

| Problem | What Aether does | What it does not do |
| --- | --- | --- |
| Split-brain control plane | Leader lease in Redis with epoch-checked writes; hot standbys take over in ~2s. | Raft or quorum. Leadership is only as available as the one Redis it lives in. |
| Zombie workers | Monotonic fence tokens checked atomically with each WAL append. | Redlock across several Redis primaries: one Redis, short `NX` locks. |
| Repeated side effects | Idempotency key per (workflow, task, effect), stable across attempts. | Outbox or reconciliation for APIs that take no key. |
| Non-deterministic replay | Results are data in the log. | Re-prompting a model during recovery. |
| Queue vs. bus | Redis list plus pub/sub wake-ups, backed by a reconcile loop. | NATS or RabbitMQ. The envelope would swap easily; the state machine is the point. |
| Cost of testing failure | Mock agent, `SIGKILL`/`SIGSTOP`, and `docker kill`. | Staging spend or GPUs. |

## Testing

```
tests/test_dag_resolution.py    ready set, joins, cycles, missing edges
tests/test_chaos_recovery.py    WAL replay, stale fence, reassignment, checkpoint/commit race,
                                orphan vs. backlog, concurrent recovery paths, retry priority,
                                retry cap
tests/test_liveness.py          config and env overrides, heartbeat thread vs. wedged loop
tests/test_failover.py          one leader at a time, epochs, deposed-leader writes rejected,
                                atomic dispatch
tests/test_effects.py           idempotency keys: replay, conflict, concurrency, retry under a
                                new fence, keys off
tests/test_audit.py             the log auditor passes a real run through a worker failure and
                                reports each kind of violation planted in it
scripts/chaos_test.py           real control planes and workers against a live Redis
scripts/nemesis.py              randomized faults across workers and leaders, audited from the log
scripts/compose_smoke.py        the Docker quick start, plus a worker kill and a leader kill
scripts/lease_sweep.py          lease length x heartbeat mode under event-loop stalls
```

Unit tests use `fakeredis`. `chaos_test.py` starts real processes and fails unless every task commits exactly once, no live worker is declared dead, every declared effect runs exactly once, and detection, recovery, and takeover land within budgets derived from the configured leases. Its modes:

| Flag | Failure injected |
| --- | --- |
| *(none)* | No failure; 61-task backlog |
| `--kill-after S` | `SIGKILL` a worker that is mid-task |
| `--stop-after S` | `SIGTERM` a worker that is mid-task (drain) |
| `--pause-after S` | `SIGSTOP` a worker until its task is reassigned, then `SIGCONT` |
| `--kill-leader-after S` | `SIGKILL` the leading control plane (`--control-planes 2`) |
| `--pause-leader-after S` | `SIGSTOP` the leader until a standby takes over, then `SIGCONT` |
| `--set K=V` | Environment override for every process, e.g. `AETHER_IDEMPOTENCY_KEYS=false` |
| `--expect-duplicate-effects` | Invert the effect check for the control run |

### Randomized faults

Each chaos mode tests one failure at a time, at a moment chosen by hand. `nemesis.py` runs 3 control planes and 3 workers through `failover_dag.json` while a seeded schedule picks a fault every 0.5–2s:

- kill, freeze, or drain a worker, preferring one that is mid-task;
- kill or freeze the leader, just after a task commits, when it has dispatch work to do.

Killed and drained processes are replaced, and frozen ones resumed, after a random delay. Freezes run from 0.3s to 5s, so some end inside the lease and others outlive it and come back as zombies. A fault is skipped rather than take out the last healthy member of a role.

Afterwards, once every frozen process has woken up and had time to attempt its late writes, `src/control_plane/audit.py` replays the WAL and checks that:

- each task's fences only advance;
- every worker write carries the task's current fence;
- no task is dispatched before its dependencies commit, and nothing touches a task after it commits;
- leader epochs never go backwards;
- every task commits exactly once.

The run also requires that every effect was performed once, that each epoch was won by one node, that no worker was declared dead outside a kill or freeze, and that no process crashed. `chaos_test.py` runs the same audit.

Twenty consecutive seeds (100–119) passed. They covered 150 faults: 46 worker kills, 45 worker freezes, 17 drains, 20 leader kills, and 22 leader freezes. They produced 37 leader changes and 77 workers declared dead, and fences and leader checks rejected 31 and 16 late writes. CI runs 5 new seeds on every push; a failure prints its seed and keeps the process logs.

A test that always passes proves little, so two checks were disabled on purpose to see whether the run notices:

- **Worker fence check disabled.** The first seed failed. Zombie workers committed with fence 1 after the task had already committed under fence 3.
- **Leader check disabled.** The audit found no violation in 16 seeds. A woken deposed leader's first write is the snapshot checkpoint, which is harmless when stale because recovery replays the WAL from the snapshot's own index. That checkpoint is where all 16 rejections above happened. By then the renewal loop has seen the lease is gone and stops the scheduler before it dispatches. The leader check on the dispatch path is covered by `test_deposed_leader_cannot_write` and `test_deposed_scheduler_steps_down_without_dispatching`, not by process-level faults.

Epochs in the elected sequence can skip numbers (`epoch 1 -> epoch 3`): every campaign increments the counter before trying the lease, so a candidate that loses the race uses up an epoch. They stay unique and increasing, which is all the fencing needs.

The seed fixes the schedule, not the interleaving, so rerunning a failing seed is where reproducing a bug starts, not a guarantee of seeing it again.

To try the same failures by hand with Docker:

```sh
docker compose up --build -d --scale control-plane=2 --scale worker=3
docker compose run --rm control-plane python -m src.main submit-workflow failover_dag.json
sleep 2 && docker kill "$(docker compose ps -q worker | head -n 1)"
docker compose logs control-plane | grep -E "worker.dead|task.reassigned|control_plane.elected|workflow.terminal"
```

To kill the leader instead, `docker kill` whichever control-plane container logged `control_plane.elected` most recently; the other logs `control_plane.elected` with the next epoch about 2s later and finishes the workflow.

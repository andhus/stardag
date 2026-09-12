# Build & Execution

How Stardag turns a task graph into work — independent of where that work
runs. Everything on this page holds for a laptop, a CI runner, a cluster
or Modal. The Modal integration adds two things on top (detached
execution and a scheduler with no resident process); those have their
[own page](modal-orchestration.md).

## The model

Stardag builds bottom-up, Makefile-style:

1. Start at the requested task.
2. Is it complete? If its output exists, stop.
3. Otherwise make sure every dependency is complete, recursively.
4. Run it, persist its output.

Two properties follow and hold in every execution mode:

- **Completeness is target existence.** A task is done when its output
  exists in storage — not when a scheduler says so. Storage is the ground
  truth, which is what makes resumption, retries and de-duplication safe:
  re-running a build never re-executes work whose outputs exist.
- **Re-execution is idempotent.** Tasks (including those with dynamic
  dependencies) are written so that running them from scratch is safe.
  The engine relies on this whenever an execution crosses a process or
  machine boundary.

## The build functions

`sd.build(task)` / `await sd.build_aio(task)` is how you run a build. It
discovers the graph, then runs a scheduling loop: submit every _ready_ task
(all dependencies complete) to a **task executor**, process results as they
arrive, repeat until the roots are complete.

`sd.build_sequential` / `sd.build_sequential_aio` run one task at a time
with no executor — for tests and debugging.

## Executors: where a task runs

The scheduling loop never runs a task itself. It hands the task to a
`TaskExecutorABC`, and the executor decides where and how:

- **`HybridConcurrentTaskExecutor`** (the default) runs each task in one of
  four local modes, chosen per task: `ASYNC_MAIN_LOOP` (async-native tasks
  on the event loop), `SYNC_THREAD` (the default for sync tasks),
  `SYNC_PROCESS` (CPU-bound work in a process pool), `SYNC_BLOCKING`
  (inline, for debugging).
- **A remote executor** submits the task to other infrastructure.
  `ModalTaskExecutor` is the first-class one; the seam is public, so you
  can implement your own.
- **`RoutedTaskExecutor`** mixes executors — GPU tasks to Modal, the rest
  locally, say.

Two shapes of executor exist, and the difference matters for everything
below:

|                            | attached                             | detached                                                                                            |
| -------------------------- | ------------------------------------ | --------------------------------------------------------------------------------------------------- |
| the executor…              | runs the task and returns its result | starts the task and returns a **handle** (a backend reference)                                      |
| if the build process dies… | the task's fate is unknown to anyone | the task keeps running; the handle is recorded in the registry, and a later build re-attaches to it |
| who reports the outcome    | the build process                    | the **worker itself**, from inside the execution                                                    |

Local executors are attached. The Modal executor is detached, and that is
what the [Modal page](modal-orchestration.md) is about.

### Build-local concurrency limits

`ConcurrencyConfig` caps how much a single build submits at once: an
overall limit plus named limits mapped to tasks by a `key_selector`,
enforced with asyncio semaphores around executor submission. They are
scoped to the one build process; limits that hold _across_ builds are a
registry feature, below.

## The registry as the execution ledger

Without a registry, a build is a process with a graph in memory. With one,
every build records what it does — tasks and edges, starts, completions,
failures — and that record is what lets separate builds, and builds with
no resident process, coordinate.

Three facts about the ledger shape everything else:

- **Task state is per environment, not per build.** A task id is a
  deterministic hash of its parameters, so the same task in two builds is
  the same row, with one status. That is what makes "don't re-run what
  another build completed" work, and equally what lets one build's
  in-flight task hold another build's downstream tasks back.
- **Statuses are derived from an append-only event log**, denormalised
  onto the task row for fast reads. `COMPLETED` is sticky: no later event
  downgrades it, matching target-as-ground-truth.
- **Registry writes are best-effort.** A registry hiccup never fails a
  task whose work succeeded; a lost completion heals from target
  existence on the next look.

### Builds collaborate; the claim is the only coordination

> A build is a **request for a set of root tasks to be materialised**, not
> an owner of the tasks that materialise them.

Which build runs a task cannot change its result — tasks are
content-addressed — so the only thing that must not happen is two builds
running the same task at once. The **execution claim** prevents exactly
that, and nothing else does.

- The claim is the task's `RUNNING` status plus an expiry, taken atomically
  in the start transaction. At most one claimant wins; the loser learns
  the winner's executor reference and re-attaches, or waits, or — if the
  claim has **lapsed** — takes it over. A lapsed claim is not a distinct
  state: it is simply not a claim anymore, so an abandoned execution heals
  without a reaper or an operator.
- Every start carries a claim TTL. Where the executor knows how long an
  execution may run (Modal's worker `timeout`), the TTL is derived from it
  plus a grace margin, so a live execution's claim cannot be taken while
  the backend would still let it run.
- **A build's plan is closed under dependencies.** Discovery registers
  every incomplete dependency of the roots, so a build is never gated by a
  task it could not run itself. **A build acts on everything in its
  plan**, whichever build last touched it — a shared task another build
  _cancelled_ is reset and run; a shared task another build _failed_ is a
  result, and the build's `fail_mode` decides.
- **Authority to revoke is build-scoped.** Cancelling build B releases
  only the claims B's own executions hold, never build C's.
- **A dependency edge stops counting when the act that asserted it is
  withdrawn.** A static edge is declared by every build that registers the
  task. A dynamic edge is discovered — one execution attempt yielded it and
  suspended — so abandoning that attempt abandons the edges with it: a task
  reset to `PENDING`, or a suspension taken over by another build, retracts
  the generation the previous attempt yielded. Without that, an attempt
  abandoned part-way left its incomplete children gating their own parent
  forever, and the next build had to re-run a whole generation of work the
  task was no longer going to ask for.

!!! warning "A fan-out's scheme belongs in its children's identity"

    Changing how a task partitions its work internally does not change what
    it promises, so its own id should not change. But the children it
    yields **must** be distinguishable: if a partition task's parameters are
    just `index=3`, then partition 3 of a ten-way split and partition 3 of a
    fifty-way split share an id while promising different content, and
    stardag will reuse the stale target. Put whatever the scheme is — the
    partition count, the chunk boundaries — into the children's parameters.
    Nothing can detect this for you; it is the content-addressing contract
    applied one level down.

Control it with `build(..., claim=...)`: `None` (default) claims probeable
executions; `True` always claims; `False` disables. Without a registry
(`NoOpRegistry`) there is nothing to arbitrate against and every claim is
granted.

Design record: [`docs/design/execution-claims-and-liveness.md`](https://github.com/stardag-dev/stardag/blob/main/docs/design/execution-claims-and-liveness.md).

### Cross-build blocking

Because task state is environment-global, a build can have nothing to run
and nothing running _of its own_ and still be healthy: an upstream some
other build is executing gates it. A build in that state asks the registry
which upstreams gate it and who owns them, then decides per blocker:

| the blocker is…                                                           | the build…                                                           |
| ------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `RUNNING` under a **live** claim                                          | waits — the blocker's completion wakes it                            |
| `RUNNING` under a **lapsed** claim                                        | fails, naming the abandoned claim                                    |
| `PENDING` / `SUSPENDED` / `INTERRUPTED` under a build that is still live  | waits — that build will move it                                      |
| `PENDING` / `SUSPENDED` / `INTERRUPTED` under a terminal or unknown build | fails, naming the task, its owner and why the owner will not move it |
| `CANCELLED`, with attempt budget left                                     | resets it and runs it — a revocation, not a verdict                  |
| `FAILED` / `SKIPPED`                                                      | leaves it; `fail_mode` owns results                                  |

`stardag builds frontier <build-id>` shows this directly. To recover a
build blocked on an abandoned task: `stardag tasks retry <owning-build-id>
<task-id>` (or `cancel` for one stuck `RUNNING`), then re-trigger your
build; `stardag builds cleanup` is the bulk recovery for abandoned builds.

### Concurrency limits across builds

Named limits configured per environment in the registry
(`stardag concurrency-limits set <key> <n>`) hold **across all builds** —
processes, machines and scheduling modes. A task occupies a slot by being
`RUNNING` under a live claim with the key recorded; the slot frees on any
transition out of `RUNNING`, with no leases to renew. Enforcement is
atomic with the claim, so a denied task never occupies a worker.

A resident build enforces them by passing
`concurrency_limiter=RegistryConcurrencyLimiter(key_selector=...)`, which
blocks-and-retries submission. Note that a resident build killed while
holding a slot leaves its task `RUNNING` until the claim lapses; reactive
scheduling on Modal heals this itself, which is one reason to prefer it
for unattended limited runs.

Infrastructure-level limits (Modal's per-function `max_containers`, say)
apply independently underneath.

### Global concurrency lock (deprecated)

The lease-based `GlobalLockConfig` predates execution claims and is
deprecated in their favour. It remains for one case claims do not cover:
executions with **no probeable liveness** (local executors shared across
machines), where its TTL lease is what recovers a crashed holder.

## Where this leaves off

Everything above is executor-agnostic. The one thing it cannot give you is
a build that survives its own process: an attached executor's task dies
with the build, and a resident scheduling loop has to stay alive for as
long as the longest task. Stardag's answer is a **detached** executor with
**self-reporting workers**, and on top of it a scheduler made of
short-lived ticks with no resident process at all. Modal is where that is
implemented and the recommended way to run Stardag at scale — continue
with [Orchestration on Modal](modal-orchestration.md), or the
[Modal how-to](../how-to/integrate-modal.md) to set it up.

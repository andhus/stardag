# Changelog

All notable changes to the Stardag project (SDK, Registry API, and UI).

For detailed SDK migration guides, see [RELEASE_NOTES.md](RELEASE_NOTES.md).

## [Unreleased]

### SDK

- **Breaking, for anyone implementing `RegistryABC` outside this repo:**
  `task_cancel_aio` takes a keyword-only `only_if_held`, and
  `build_get_executions` / `build_get_executions_aio` take a keyword-only
  `cursor`. Subclasses that override the old signatures raise `TypeError`
  when the reactive tick calls them; the call sites log and continue, so the
  symptom is a cancel that is never recorded rather than a crash.

- **A cancelled or failing build no longer stops other builds' executions.**
  The tick's cancel pass read the frontier's `running` list, which is every
  RUNNING task in the build's _plan_ — and after plan closure that includes
  tasks another build has claimed and is executing. A cancelled build
  cancelled them: killing live containers, releasing claims it never held,
  and doing it again on every tick a neighbour's status write earned it. It
  now asks the registry which executions it started and has not seen end
  (`GET /builds/{id}/executions`, `RegistryABC.build_get_executions`) and
  stops only those. Against a server predating the route it falls back to
  the frontier filtered on the new `latest_status_build_id`; against one
  predating that field too, it behaves exactly as before.
- **`stardag builds cancel --cascade` now actually stops the executions it
  releases.** The cascade writes TASK_CANCELLED for the claims the build
  held — which is what lets the next build take those tasks over — but a
  cascaded task is CANCELLED and therefore in neither `running` nor
  `actionable`, so the one caller of `cancel_detached` could not see it.
  The claim was released and the container kept running, and the next
  claimant started a second execution of the same task. The executions
  route reports them, so the one tick a cancel asks for now stops them.
- **A tick that resets a blocked upstream now runs it, instead of lingering
  for a wake-up it never sent.** Resetting a cancelled blocker is the one
  thing terminal handling does that changes the frontier, and the pass
  treated it as no action at all: it fell through to the linger poll, which
  waits on the registry's wake-up flag — and that flag deliberately skips
  the build whose own event caused the change, since it is the one that
  already knows. So the tick waited for news it had already heard, exited on
  its deadline, and left the build with nothing running, nothing scheduled
  and no flag to be handed out on, until the watchdog. Reachable whenever a
  shared task is genuinely left cancelled, which is exactly what the cancel
  fixes above make the common outcome.
- **A worker no longer spawns a tick for a build that cannot use one.** A
  cancelled build's workers keep running until a tick stops them, and each
  one re-flagged the build on its way out, so every drain in the
  environment handed it out again. `POST /builds/{id}/notify` now answers
  `needs_tick` truthfully and the worker skips the spawn when it is false.
  The one tick a cancel wants is unaffected: the cancel sets that flag
  itself, and it survives until a tick drains it.

### Registry API

- `POST /builds/{build}/tasks/{task}/cancel` refuses with 409
  `not_claim_holder` when the task is RUNNING, SUSPENDED or INTERRUPTED
  under a different build. Authority to revoke is build-scoped — the
  cascade already enforced it, and `stardag tasks cancel` has documented it
  since it shipped ("pass the build from `latest_status_build_id`"). PENDING
  and terminal statuses stay cancellable by any build; they hold no claim.
- `GET /builds/{build_id}/executions`: the detached executions this build
  started and has not seen end, with the backend and ref to stop them by.
  Paged with a keyset `cursor` — stopping an execution records nothing, so
  the answer does not shrink as a caller works through it, and a bare cap
  would hand back the same page forever.
  Answered from the event log rather than from the task rows, because the
  question is about the past: releasing a claim is what lets the next build
  take the task over, so by the time a cancelled build ticks, the row may
  already name somebody else's execution. A ref is not a claim — cancelling
  the one this build recorded cannot reach another build's container.
- `FrontierTaskRef.latest_status_build_id`: who holds each task in the
  frontier, so a scheduler can tell its own executions from a neighbour's.
- `POST /builds/{id}/notify` flags only a RUNNING build, and reports
  `needs_tick` accordingly.
- `POST /builds/{b}/tasks/{t}/cancel?only_if_held=true` records nothing
  unless this build still holds the task in RUNNING or INTERRUPTED —
  evaluated on the locked row, so an engine cleaning up after itself from a
  listing it read a moment ago cannot stamp a task another build has since
  reset and is about to run. A no-op rather than an error: losing that race
  is a normal outcome, not a fault.

## [0.23.0] — 2026-09-01

### SDK

- **Scheduler ticks share a container.** The deployed Modal `tick` is now an
  `async def` and is registered with `@modal.concurrent(max_inputs=10)`, so up
  to ten concurrent reactive builds are served by one container instead of one
  each — stardag's default for this function, overridable through
  `FunctionSettings(max_concurrent_inputs=...)`.
  A tick is almost entirely I/O wait — read the frontier, spawn, then poll on
  a sleep until the linger deadline — so this is close to free, and it is what
  makes `linger_seconds` cheap: a resident tick driving level after level no
  longer costs a container of its own. Async is load-bearing rather than
  stylistic: Modal serves concurrent inputs to a sync function on _threads_,
  each running its own `asyncio.run`, and `APIRegistry`'s async client is
  cached per event loop and closed when the loop changes — so threaded ticks
  would tear down each other's in-flight HTTP client. Workers are deliberately
  not packed (they run user code and may be CPU- or GPU-bound), nor is
  `tick_watchdog` (its own function, one input per period), which shares
  `tick_settings` with the tick and so is held out explicitly rather than by
  having no default. Supporting
  changes: the async client's connection pool is sized for a shared process
  rather than for one caller, and the tick's two remaining blocking calls —
  the foreign-app forward and the successor hand-off's spawn — moved off the
  event loop, where they would have stalled every co-resident tick. The
  tick's completion log line now carries `MODAL_TASK_ID`, since packing is
  otherwise unobservable: `modal app logs` has no per-container attribution
  and `modal container list` names the app but not the function.
- **`FunctionSettings` speaks Modal's current vocabulary, and translates the
  old one.** `max_containers`, `min_containers`, `buffer_containers`,
  `scaledown_window`, `max_concurrent_inputs` and `target_concurrent_inputs`
  are now accepted; the last two are applied via `@modal.concurrent` rather
  than passed to `App.function()`, and validated in stardag's own vocabulary
  rather than left to fail at registration with a `TypeError` naming a
  parameter nobody wrote. The four legacy names Modal renamed in 2025
  — `concurrency_limit`, `keep_warm`, `container_idle_timeout` and
  `allow_concurrent_inputs` — are translated with a warning instead of being
  forwarded. This is a **bug fix, not a deprecation**: Modal's client raises
  `DeprecationError` on all four, and `FunctionSettings` passed them straight
  through, so any app that set one failed to deploy.

- **`require_pickle_free=True` now binds the scheduler tick, not just the
  trigger.** The flag was a trigger-time gate: the bootstrap refused to start
  a build whose tasks would need pickles, and nothing carried the intent any
  further. But a tick is a writer of the same store — it wrote back every task
  it rehydrated from registry data — so on a writable target root a build that
  declared it writes no pickles quietly left them there anyway, one per task,
  the first time each was rehydrated. Where the class could not be pickled the
  write merely failed, warning once per rehydration per tick. `BuildTaskStore`
  now takes `pickle_free`, which makes every write a no-op, and the deployed
  tick builds its store from the app's flag. Nothing is lost by skipping: the
  write-back is a cache over an object the caller already holds, and on such a
  build every task is rehydratable by construction. Unchanged: the trigger-time
  gate, and the documented carve-out for an uncovered _dynamic_ dependency
  registered from inside a worker, which still gets its pickle rather than
  failing the bookkeeping of a task that has already run.

- **`BuildTaskStore` gains async variants** — `save_task_aio` /
  `save_tasks_aio` / `load_task_aio`, built on the targets' existing
  `exists_aio` / `open_aio`. The reactive tick's `_load_task` and the async
  trigger path called the blocking methods straight from the event loop, which
  stalled every other coroutine in the tick for a network round-trip and drew
  Modal's `AsyncUsageWarning` on volume-backed roots. The sync methods stay for
  the sync callers. Also: a corrupt or truncated store pickle now reads as a
  **miss**, so the tick falls back to registry rehydration instead of raising.

- **Compatible with modal 1.5.5**, which moved two symbols in a patch release:
  `modal.volume.FileEntryType` → `modal.types.FileEntryType` (still re-exported
  at runtime, but dropped from the stub, so it broke type checking rather than
  execution) and `_utils.function_utils.FunctionInfo` → `FunctionSourceInfo`.
  Both are handled with an import fallback rather than a version floor bump,
  since the declared `modal>=1.0.0` spans both spellings — verified against
  1.5.0 and 1.5.5 alike.

- **`--server-version latest` is resolved at deploy time, not deployed as a
  tag.** `stardag self-host up/upgrade --server-version latest` now asks the
  GitHub Releases API for the newest `server-vX.Y.Z`, prints what it resolved
  to, and uses that concrete version for the image reference, the recorded
  deployment meta and `self-host status`. Previously the literal string
  reached `modal.Image.from_registry`, where a mutable tag is cached like any
  other image definition — so an upgrade could silently redeploy the image
  already built for `:latest`, and nothing afterwards could say which release
  was running (`status` echoed `latest`). Releases rather than git tags are
  the source of truth because the release job runs `needs: publish-image`, so
  a release exists only if the image push succeeded. A deployment recorded as
  `latest` by an older SDK resolves once on its next upgrade and converts to a
  pin. GHCR's `:latest` tag is unchanged and still usable directly.
- **A plain `stardag self-host upgrade` rolls the server version forward
  again.** `_resolve_upgrade_server_version` returned the recorded deployed
  version unconditionally, so once a deployment had recorded a version only an
  explicit `--server-version` could ever move it — a newer SDK's pin was
  ignored, contrary to what the self-host docs said. It now deploys the newer
  of the recorded version and `DEFAULT_SERVER_VERSION`, which keeps the
  anti-downgrade property that motivated the original behaviour (a deployment
  ahead of this SDK's pin stays where it is) while letting the pin move a
  deployment that is behind.

- **One arbitrated start method on `RegistryABC`.**
  `task_start_with_limits_aio` is removed; `task_start_claim_aio` gains a
  `claim: bool = True` parameter, so the two orthogonal flags the registry's
  single `/start` endpoint carries — `claim` and `enforce_limits` — are
  reached through one method. The resident build's
  `RegistryConcurrencyLimiter` acquires its slots with `claim=False` (the
  engine has already claimed the task before entering the slot, so a
  claiming acquire would be denied `already_running` by its own build) and
  now logs the keys the server actually held back on. No wire change and nothing to do for a
  build driven through the engines. **Breaking for a custom `RegistryABC`
  implementation in two ways**, both loud: any direct caller of
  `task_start_with_limits_aio` (a subclass that implemented it, or code
  calling it on `APIRegistry`, where it is also removed) migrates to
  `task_start_claim_aio(..., claim=False)`; and an existing
  `task_start_claim_aio` **override must accept `claim`** or it raises
  `TypeError` the first time the limiter calls it. A backend that
  implemented only `task_start_with_limits_aio` and relied on its no-op
  default now raises `NotImplementedError` from
  `RegistryConcurrencyLimiter` instead of silently skipping enforcement.
- The Modal worker's wake-up reads `BuildNotifyResult.scheduler_live` off
  the result directly instead of through `getattr`. Unknown still spawns —
  an older registry leaves the field `None`, and a notify that raised
  returns nothing at all — but the tolerance for a third-party backend
  answering with some other shape is gone, matching the v0.18.0 decision
  that custom-backend compatibility was never real.
- **The watchdog sweep spawns instead of running ticks inline.**
  `tick_watchdog` now lists the running reactive builds the app owns, spawns
  one `tick` per build and returns, rather than running every build's tick
  body sequentially inside the sweep's own container. Three things stop being
  a function of how many builds the environment happens to be running: each
  build's spawn cap (that one container's timeout was divided across the
  sweep), whether the sweep finished at all, and — to within a spawn RPC
  rather than a whole tick — how long the last build in the list waits. Each
  build now gets a container of its own, its full timeout and its normal
  linger; a duplicate spawn still starts a container, but that tick finds the
  scheduler lease held and exits without acting. The `linger_seconds=0` and
  share-of-timeout overrides the inline form needed are gone with it.

  A swept build's tick still does **one pass and exits**: `spawn_tick` grew
  an optional `tick_kwargs`, and the sweep uses it to ask for
  `linger_seconds=0`. The inline version forced that to survive sharing one
  container; it is kept for a better reason. A wake-up's tick lingers
  because something just happened and more is likely to, whereas a sweep
  looks at builds where nothing is known to have happened. Lingering there
  would hold container time every period on exactly the builds least likely
  to have anything to do — and since a container lives as long as its
  longest live input, a couple of stale `RUNNING` builds would be enough to
  keep the tick function warm, however few they are.

- Default prebuilt server image bumped to `0.3.0`
  (`DEFAULT_SERVER_VERSION = "0.3.0"`, from the `server-v0.3.0` release) —
  the server version this SDK release is tested against, and the one that
  carries the new server surface this release's reactive changes call:
  `GET /builds/{build_id}/notify` and the scheduler-lease routes. The
  image tag
  and `--server-version` take the bare `X.Y.Z`; `server-v` prefixes the git
  tag only.

- **The linger poll no longer reads the frontier.**
  `RegistryABC.build_get_notify[_aio]` reads the wake-up flag, and the tick's
  linger poll, its pre-release re-check and its exit hand-off all use it. The
  frontier is fetched only when there is something to act on. Against a
  server without the route the poll falls back to the frontier read it did
  before, and an unparseable answer reads as _unset_ rather than set —
  fabricating a change would spin the tick without releasing its lease.
  Latched once per process, since re-probing on every poll is the cost the
  endpoint removes. The default implementation on `RegistryABC`
  delegates to the frontier, so a custom backend needs no changes to keep
  working, and can override for the cheap read.

- **The tick no longer uses the global concurrency lock at all.**
  `run_tick_aio` **drops its `lock_manager` parameter** — the lease was its
  only use — and takes the lease through the registry instead, renewing it
  in the background while it lingers and stopping
  (`TickSummary.outcome == "lease_lost"`) if a renewal reports it was taken
  over. `RegistryABC` gains
  `build_acquire_scheduler_lease_aio` / `build_renew_scheduler_lease_aio` /
  `build_release_scheduler_lease_aio`, defaulting to granting, and the
  duplicated `SCHEDULER_LOCK_PREFIX` is gone from both sides.

  Against a server without the routes the tick runs unleased and says so
  once: duplicate ticks become possible (idempotent, and task starts stay
  arbitrated by the execution claim). That server also reports
  `scheduler_live=False` to every worker, because it reads a lock table this
  SDK no longer writes — so wake-ups spawn unconditionally too, which is the
  pre-lease behaviour end to end rather than a half-broken one.

  **The other direction is the one a real deployment takes**, since the API
  upgrades before the Modal apps that bake in their SDK: a new server does
  not read the legacy lock an old tick takes, so it reports
  `scheduler_live=False` for every completion and every worker spawns a tick
  that immediately no-ops. Correct, but it costs containers until each app
  is redeployed — worth doing promptly rather than leaving.

  The lease TTL is 60 s, unchanged from the lock-table lease it replaces,
  and it is the dead-tick recovery window: until it lapses, the build is
  hidden from drainers _and_ workers skip their spawn. Renewal moved from
  half the TTL to a third, so two consecutive renewal failures are
  survivable — and a third is too, because a refused renewal re-acquires
  rather than abandoning a build nothing else is driving.

- _Internal:_ `stardag/build/_reactive.py` (3,412 lines) is now the
  `_reactive/` package — `_discovery`, `_tick`, `_frontier_actions`,
  `_terminal`, `_budgets` — split along the section banners the module already
  had. No behaviour change and no import change: `stardag.build`'s re-exports
  are untouched and the package `__init__` re-exports what the module did, with
  one deliberate exception. The **mutable module globals** (the lease timing
  knobs, the tick-summary route latch, the successor-spawner warning latch) are
  _not_ re-exported: rebinding an alias leaves the reading module untouched, so
  a monkeypatch against the package would go green while patching nothing.
  Reach into the owning submodule instead.

### Registry API

- **`GET /builds/{build_id}/notify`** reads a build's scheduler wake-up flag
  from its own row, with nothing derived. The reactive tick's linger poll
  asks one question every few seconds per lingering build — "has anything
  changed?" — and used to ask it by fetching the whole frontier: seven
  statements, one of them a window-function aggregate over the event log,
  of which it read a single boolean.

- **The reactive scheduler's lease lives on the build row.** New
  `POST`/`PUT`/`DELETE /builds/{build_id}/scheduler-lease` acquire, renew and
  release a build's single-flight lease, recorded as
  `builds.scheduler_lease_until` / `scheduler_lease_owner` (migration, no
  backfill — a lease is transient). It used to ride on the deprecated global
  concurrency lock, so both readers had to assemble a lock name from a build
  id and query `distributed_locks`: `select_wake_candidates` is now one query
  instead of two, and "is a scheduler live?" is a column comparison. Renew
  and release are owner-checked, so a tick whose lease lapsed and was taken
  over cannot extend or clear its successor's. The global lock is untouched
  for its remaining use (executions without probeable liveness).

### Deployment

- **Server image `0.3.0`**, carrying the two Registry API changes above:
  `GET /builds/{build_id}/notify` (the linger poll's one-row read) and the
  scheduler lease on the build row —
  `POST`/`PUT`/`DELETE /builds/{build_id}/scheduler-lease` plus the
  `builds.scheduler_lease_until` / `builds.scheduler_lease_owner` migration
  (`b41c7d9e2f08`, no backfill). Minor rather than patch for the same
  reason as `0.2.0`: the HTTP surface grew and there is a schema migration.
  Self-hosters upgrade with `stardag self-host upgrade`; redeploy Modal
  apps promptly after — until each app is redeployed, its old ticks take a
  legacy lock this server no longer reads, so every completion reports no
  live scheduler and spawns a tick that immediately no-ops.

- **Server image `0.2.0`**, the first server release since `0.1.2`
  (2026-08-12). It carries the Registry API and UI changes recorded under
  0.19.0 — apart from the SQL-injection fix, which shipped in `0.1.2` itself
  — plus those under 0.21.0 and 0.22.0: `INTERRUPTED` as a first-class task
  status and `POST /builds/{id}/tasks/{task_id}/interrupt`,
  `POST /builds/{id}/notify` reporting `scheduler_live`,
  `POST /builds/wake-candidates`, and the `builds.tick_requested_at`
  migration (`97ce4e3cbf32`). It also carries the `python-jose` → `PyJWT`
  swap for token auth and raised security floors on the API's transitive
  dependencies and the UI's build tooling. Minor rather than patch: the HTTP
  surface grew and there is a schema migration. Self-hosters upgrade with
  `stardag self-host upgrade`.

  A self-hosted `0.1.2` predates `wake-candidates`, so an SDK on the v0.22.0
  line talking to it degrades to the previous behaviour — cross-build
  wake-ups arrive only via the watchdog — rather than failing.

## [0.22.0] — 2026-08-30

### SDK

- **A status change reaches every build it concerns.** A reactive build
  used to be woken only by its own Modal workers; a shared task finished,
  failed, cancelled or retried by another build's worker or tick, by a
  resident build, or by an operator in the UI or CLI — and a concurrency
  slot freed by another build — reached it only through the watchdog,
  which is off by default. The registry now flags every live reactive
  build holding a task whenever that task's status changes (and the
  builds queued on a key when a slot frees, and a build itself when it is
  cancelled), and every scheduler pass — a tick after acting and on exit,
  a resident build with Modal workers after each result — asks the
  registry for the flagged builds nobody is serving and spawns one tick
  each. The registry hands each build out once per window, so N schedulers
  asking at once produce one tick per flagged build, not N. `TickSummary`
  gains `neighbour_ticks_spawned`. Both halves degrade to the previous
  behaviour across version skew: an older registry answers nothing, an
  older SDK never asks.
- **Concurrency-limit keys are registered at plan time.**
  `discover_and_register_aio(limit_key_selector=...)` sends each task's
  keys with its registration (the bootstrap passes the app's selector; the
  worker wrapper publishes it for dynamically yielded dependencies), so the
  registry knows which pending tasks want a key and can wake the builds
  queued on it when a slot frees. Plan-time rows never count towards
  occupancy — only a `RUNNING` task under a live claim does.
- **`tick_watchdog` is deployed on every app**, scheduled only when
  `watchdog_period_minutes` is set, so a full sweep is one click away on an
  app that runs no cron. `build_trigger(reactive=True)` no longer warns
  when no period is configured. `TaskExecutorABC` gains
  `can_spawn_scheduler_ticks` / `spawn_scheduler_tick`; the Modal executor
  implements them and a `RoutedTaskExecutor` delegates.
- **`TickConfig.spawn_successor_tick(build_id)` is now
  `TickConfig.spawn_tick(build_id, app_name)`.** One callable serves the
  exit hand-off and the cross-build drain. Only callers driving
  `run_tick_aio` by hand are affected; the Modal integration supplies it.
- **Docs:** `concepts/build-execution.md` is rewritten top-down and
  executor-agnostic; the Modal-specific model (detached execution,
  resident vs reactive, wake-ups, the watchdog) moves to a new
  `concepts/modal-orchestration.md`, and the Modal how-to's reactive
  section is consolidated around configuration.

### Registry API

- `POST /builds/wake-candidates` — hands out RUNNING reactive builds that
  are flagged, hold no live scheduler lease and were not handed out within
  the last window, stamping `builds.tick_requested_at` (new column,
  migration `97ce4e3cbf32`) in the same transaction. `POST
/builds/{id}/notify` stamps it too when it reports no live scheduler.
- Every path that changes a task's status — the event routes,
  `skip-blocked`, `cancel?cascade=true`, bulk cancel and the reaper, the
  lock release-with-completion — flags the other live reactive builds
  holding the task, transition-gated. A build cancel flags the build.
- `POST /builds/{id}/tasks/bulk` accepts an optional `limit_keys` per task
  and records them; `null` leaves recorded keys alone, and a `RUNNING`
  task keeps the keys it was started under.
- `PUT /builds/{id}/reactive-meta` rejects an empty `app_name`.
- The reaper's idleness signal no longer counts `needs_tick_at`: the flag
  is written by other builds' transitions now, and was redundant with the
  event stream before.

## [0.21.0] — 2026-08-29

### SDK

- **The reactive scheduler no longer logs an ERROR for a task-store miss it
  recovers from.** `BuildTaskStore.load_task` logged
  `... not found in the build task store — cannot (re)schedule it` on every
  miss, but its only caller rehydrates the task from registry data and
  succeeds. Declaring `task_modules` _is_ the opt-in to pickle elision, so on
  the recommended configuration every lookup misses by design: a healthy
  seven-task build emitted seven errors claiming its tasks could not be
  scheduled, immediately after which all seven were. The miss is now DEBUG.
  A store entry that is not a `BaseTask` remains an ERROR, and `_load_task`
  still logs at ERROR when _both_ stages fail — with the failed-import
  annotation that makes it actionable.

- **`with_stardag_on_image` no longer pins a Modal image to a stale PyPI
  release when stardag is installed editable.** The choice between "ship
  the local working tree" and "install the pinned release" was inferred
  from `stardag.__version__` — but that is
  `importlib.metadata.version("stardag")`, and an **editable install's
  metadata version is a snapshot taken when the install ran**. hatch-vcs
  computes it from the git tag reachable at that moment and nothing
  recomputes it as the working tree moves on, so a checkout installed at
  v0.17.0 keeps reporting `0.17.0` while its source is v0.20.x. That string
  carries no `dev` and no `+`, so it read as a plain released version and
  the image was pinned to a real PyPI release **older than the code being
  serialized into it**.

  The result is the failure shape v0.20.1's placement guardrail was built
  for, from a different cause: the app deploys cleanly and then every
  container dies at hydration with `ModuleNotFoundError` for a stardag
  module the deploying process could see and the container cannot —
  observed as `No module named 'stardag.integration.modal._builder'`,
  before any of the app's own code runs.

  `local_stardag_source="auto"` now asks the installer whether this is a
  working tree (an editable install, a bare `sys.path` entry, or a dev
  version) instead of guessing from the version string. The two routes that
  can still pin a version older than the running source — an explicit
  `local_stardag_source="no"` from a working tree, and an explicit
  `version=` older than the running one — warn rather than refuse, since
  both are something the caller asked for.

  Note that a plain `uv sync` does not refresh a stale editable version:
  the install is already present, so nothing rebuilds its metadata.
  `uv sync --reinstall-package stardag` does.

- **A worker no longer spawns a scheduler tick when one is already
  running.** Every task completion used to spawn a tick unconditionally. On
  a build whose tasks are short relative to a tick container's startup,
  none of those ticks scheduled anything — the resident scheduler's own
  linger loop did all the work, and each spawned tick started only after it
  had finished, took the lease or found the build terminal, and exited. A
  measured seven-task build paid seven cold starts for zero scheduling.
  `build_notify` now reports whether a scheduler holds the build's lease
  (`BuildNotifyResult.scheduler_live`) and the worker skips the spawn when
  it does — one working tick instead of N+1. An older registry does not
  report it, and every wake-up then spawns a tick exactly as before.

- **The scheduler tick's exit path no longer has a lost-wakeup window.**
  Nothing re-read the wake-up flag between the linger loop's final poll and
  the release of the scheduler lease, so a flag set in that window was
  served by nobody — it stayed set until the next completion or the
  watchdog (off by default), and with the last task in flight there may be
  no next completion. Harmless while every wake-up spawned its own tick;
  the load-bearing prerequisite for skipping that spawn. The tick now
  re-reads the flag once **before** releasing the lease (set → keep the
  lease and re-act) and once **after** (set → spawn a successor tick), and
  reports both on its `TickSummary` as `linger_extended` and
  `successor_spawned`. This closes the release window; it is not crash
  recovery — a tick that clears the flag and then dies still leaves that
  wake-up to the next completion or the watchdog, as before.

  `RegistryABC.build_notify` returns a `BuildNotifyResult` rather than
  `None`. A custom registry backend that overrides it and returns `None` is
  read as "scheduler state unknown", which keeps today's behaviour.

### Registry API

- `POST /builds/{id}/notify` reports `scheduler_live` — whether a reactive
  scheduler held the build's lease when the response was produced. The read
  happens after the flag is committed rather than atomically with it, and
  that ordering is the whole guarantee: a `true` means the lease was still
  held once the flag was already durable, so its holder cannot exit without
  seeing it. That is what makes skipping the tick spawn safe.

## [0.20.1] — 2026-08-28

### SDK

- **`StardagApp(...)` now refuses a callable no container could unpickle.**
  Everything an app passes — `container_setup`, `worker_selector`,
  `limit_key_selector`, `build_function`, `run_function` — is cloudpickled
  into the `serialized=True` functions `finalize()` registers, and
  cloudpickle stores a module-level callable (or the class of a callable
  instance) as a _reference_ to its defining module.
  `stardag modal deploy path/to/app.py` loads the entry point under a module
  name taken from the file name, so a `def` written in `app.py` pickles as
  `app.<name>` — a module that exists only in the deploying process. The app
  deployed cleanly and then every function carrying that callable died at
  hydration with `ModuleNotFoundError: No module named 'app'`, before
  reaching any of the app's own code. The damage was partial and delayed:
  `build` and `worker_*` often survived, because their closures reach the
  app's package modules anyway, while the scheduled reactive functions did
  not — so an app could look healthy with its scheduler dead.

  This is now a `SerializedCallablePlacementError` at `StardagApp(...)`,
  naming the callable, the module and the fix. It raises rather than warns:
  unlike the `task_modules` coverage warning there is no degraded-but-working
  path on the other side of it.

  Lambdas, closures and anything defined in `__main__` are **not** rejected —
  cloudpickle writes those out by value, so they need no import in the
  container and work today. A `functools.partial` or a bound method is
  itself by value but carries a reference to what it wraps, so the check
  looks through both.

  Docs: the placement rule was stated only on `ContainerSetup`, which read as
  though it were specific to the hook. It now covers all five parameters, and
  the Modal how-to gained a section stating the deploy CLI's import naming
  explicitly — the part no app author can infer from their own code.

## [0.20.0] — 2026-08-14

### SDK

- **`StardagApp(container_setup=...)` — one declared place for setup that
  every Modal container of an app needs.** A zero-argument callable, run once
  per container at the top of **all five** registered functions: `build`, each
  `worker_*`, and the reactive `tick`, `bootstrap` and `tick_watchdog`.

  It closes a real gap rather than adding sugar. `finalize()` registers every
  function with `serialized=True`, so a container unpickles a closure instead
  of importing the module the app was declared in — and which of the app's
  modules _do_ get imported was decided by what each closure happened to
  reference. `build` and `worker_*` close over the app's `build_function` /
  `run_function`, so their modules are imported and their module-level setup
  runs; that is the behaviour `StardagApp.__init__` has always documented. But
  a `bootstrap` container closes over nothing of the app's at all (just the app
  name, the task-module patterns and two flags), and `tick` / `tick_watchdog`
  import app code only as a side effect of a supplied `worker_selector` /
  `limit_key_selector` or of the expanded `task_modules`. Setup that appears to
  "run everywhere" because it runs in the workers could therefore be silently
  absent from exactly the containers that drive a reactive build — reported
  from a deployed app whose storage credentials were prepared by such a
  routine, whose reactive builds then failed in `bootstrap` at the first
  completion check.

  Semantics:

  - **Once per container, not once per input** — a worker serves many tasks and
    a tick container may be reused; the guard lives in stardag so apps do not
    each write one.
  - **A hook that raises propagates and is retried on the next input.** It is
    deliberately not remembered as done on failure, so a container's remaining
    inputs cannot run silently un-set-up; a deterministic failure fails every
    input, loudly.
  - **Runs before stardag's own `logging.basicConfig` default.** `basicConfig`
    no-ops once the root logger has handlers, so an app that configures root
    logging in the hook owns log formatting in these containers, and an app
    that does not still gets the default.
  - Pickled by reference like `worker_selector`, so **define it in a module
    importable inside the container** — which is also what makes module-level
    code in the hook's own module run in every container.

  It complements, and does not replace, `Builder.setup(tasks)` (per build,
  `build` container only) and `Runner.setup(task)` (per task). For the reactive
  functions there is no choice to make: a `tick` / `bootstrap` /
  `tick_watchdog` container holds neither a `Builder` nor a `Runner`, so this
  is the only hook that reaches them.

  Additive — an app that passes nothing behaves exactly as before. The new
  `ContainerSetup` type alias is exported from `stardag.integration.modal`.

- **`finalize()` now fails an app with no `"default"` worker and no
  `worker_selector`.** Every task would route to a `worker_default` function
  the app does not deploy, so the deployment is dead on arrival — previously
  it deployed cleanly and failed at the first task. Scoped to the
  no-selector case: an app that declares a selector may omit `"default"` and
  route everything to its own tiers, which works today and keeps working.

- **`finalize()` now warns about workers nothing can route to.** An app that
  declares several `worker_settings` but no `worker_selector` sends every task
  to `"default"`, so its other tiers are deployed and never reached — and the
  symptom is indistinguishable from a healthy deployment, because the build
  succeeds, just entirely on the wrong worker. The warning names the
  unreachable workers and fires at deploy, not on the app object the
  triggering process constructs. Passing a selector explicitly — even one that
  always returns `"default"` — silences it. Per-trigger overrides
  (`build_spawn`/`build_trigger(worker_selector=...)`) remain a valid way to
  route a **resident** build; reactive builds reject them, since later ticks
  could not honour them, which is why the app-level selector is what the
  warning points at.

## [0.19.1] — 2026-08-14

### Fixed

- **A user package named `modal` no longer breaks target resolution.**
  Reported from a deployment running `0.19.0`, where `get_directory_target()`
  failed at import with

  ```
  AttributeError: module 'modal' has no attribute 'exception'
  ```

  in a service that did not use Modal at all. Two independent defects had to
  line up.

  First, the trigger. The service's entrypoint was launched as a script path
  (`python pkg/service/main.py`, not `python -m`), which puts the script's own
  directory on `sys.path[0]`. That directory contained a first-party
  `modal/` subpackage, so **every** `import modal` in the process — stardag's
  included — resolved to it instead of the installed distribution. `import
modal` therefore _succeeded_ and returned the wrong module; the failure
  surfaced only on first attribute access. Nothing on that service's code
  path had ever imported `stardag.integration.modal` before, which is why the
  shadowing had been latent and `0.19.0` appeared to cause it.

  Second, why it escalated. `get_default_prefix_to_target_prototype()` imports
  the optional `stardag.integration.*` backends to register the `s3://` and
  `modalvol://` prefixes, and guarded those imports against `ImportError` —
  the "not installed" case — and nothing else. A shadowed `modal` does not
  raise `ImportError`, so the `AttributeError` propagated out of the factory
  and killed target resolution for **every** prefix, local paths included.

  Both are fixed. `_runner.py` and `_app.py` no longer resolve
  `modal.exception` off the parent package — `exception` is not in modal's
  `__all__`, and the attribute is bound only as a side effect of modal's own
  `__init__` — so a shadowed or partial `modal` now fails as a plain
  `ImportError`, which is both accurate and catchable. And the factory's
  guards now also catch and log an unexpected failure: the affected prefix
  drops out of the mapping, using it gives the ordinary unsupported-prefix
  error, and a warning names the cause. A genuinely absent optional dependency
  stays silent, as before.

  Note the attribute form worked on every modal release stardag supports
  (`>=1.0.0`); it was the shadowing, not a modal version, that exposed it.

## [0.19.0] — 2026-08-13

### Registry API

- **Security (server image `0.1.2`): fixed a SQL injection in the task search
  endpoint.** `GET /api/v1/tasks/search` builds JSONB accessor chains as SQL
  text (Postgres has no bind-parameter form for a `->'key'` step). The path
  segments of a `filter` key, and the artifact name in a `sort` field, were
  interpolated without validation, so a crafted key or sort value could inject
  SQL. The endpoint requires authentication, and the injection lands inside the
  environment-scoped `WHERE`; a malformed value was read-only (no statement
  stacking on the parameterised path) but could read across environments. The
  exposure is limited to **registry metadata** — task identities, parameters,
  build history, registry-stored artifacts, and user/credential records (keys
  and any local passwords are bcrypt hashes). Task `target()` output is not
  stored by the service — it lives in the user's own filesystem/S3/Modal
  storage — and is not exposed by this issue. All released server images before
  `0.1.2` are affected. Fixed by validating every path segment against an
  identifier character class and binding the sort artifact name as a parameter;
  filter/sort inputs are now length-bounded. Self-hosters should upgrade to
  server image `0.1.2`. See advisory
  [GHSA-47m3-4ppr-cfh4](https://github.com/stardag-dev/stardag/security/advisories/GHSA-47m3-4ppr-cfh4)
  (CVSS 7.7 High; read-only, authenticated).

- **`TASK_INTERRUPTED` / `TaskStatus.INTERRUPTED`**, and
  `POST /builds/{id}/tasks/{task_id}/interrupt`. Modelled on `SUSPENDED`:
  non-terminal, non-running, holds no execution claim, listed as
  actionable by the frontier, and reset by a re-trigger. **No migration** —
  both columns are already `String(32)`.

  Deliberately a status rather than a `retryable` flag on `/fail`: a
  worker-recorded _failure_ sits in the next frontier snapshot and, under
  `FAIL_FAST`, kills the build before anything can retry it. A tick avoids
  that only because it records and retries inside one pass.

- `FrontierTaskRef.interrupt_count`, counted over the same build round as
  `attempt_count`. An interruption between two starts does not open a new
  attempt.

- Old SDKs are unaffected (nothing emits the new event). A **new SDK
  against an older server** logs a warning and records nothing, which is
  its pre-existing behaviour — a version skew degrades to the old recovery
  path, never to a failed build.

### UI

- **A build whose work is done no longer says it needs intervention.** A
  build that failed waiting on a shared task, which another build later
  completed, kept showing "Nothing runnable, and no wake-up pending — needs
  intervention" alongside a status count taken when it failed. The panel now
  recognises that every root is complete.

- `INTERRUPTED` renders as its own status (orange, beside skipped's amber
  rather than failed's red), is filterable, and offers Retry but not
  Release — an interrupted task holds no claim to release.

### SDK

- **A `StopAsyncIteration` raised by an async task's loop body is no longer
  swallowed.** `_drive_async_generator` wrapped its `async for` in
  `except StopAsyncIteration: pass`, mirroring the sync driver — but `async
for` consumes the generator's own exhaustion, so the handler could only
  ever catch one raised by the _loop body_ (a `complete()` check, say).
  Swallowing that returned `None`, which the caller reads as "task
  completed" and reports as such. It now propagates.

- **A task can survive preemption and function timeouts** (closes
  [#245](https://github.com/stardag-dev/stardag/issues/245)). The two ways
  a container routinely dies without the task being broken had no
  representation in the SDK, and a task that caught the interrupt to
  checkpoint — which is what you must do — and re-raised anything derived
  from `Exception` recorded a permanent `TASK_FAILED`, killing a
  `FAIL_FAST` build.

  New: **`sd.ResumableInterruption`**, and
  **`stardag.integration.modal.MODAL_INTERRUPTIONS`** — the exact pair the
  platform raises (`KeyboardInterrupt` for preemption,
  `modal.exception.InputCancellation` for a timeout, which is _not_ a
  `KeyboardInterrupt`).

  ```python
  try:
      train(resume_from=checkpoint)
  except MODAL_INTERRUPTIONS:
      save_checkpoint(checkpoint)
      raise sd.ResumableInterruption("checkpointed") from None
  ```

  **The task decides, not configuration.** Raising `ResumableInterruption`
  is the only way a task gets resumed, bounded by the new
  `TickConfig.max_interruptions` (default 20) — a budget separate from
  `max_attempts`, or a trainer designed to be killed and resumed would
  exhaust one meant for genuine failures. An interruption a task does
  **not** catch stays a failure under `max_attempts`: it means the task had
  no plan for one, so either it hung or the worker's `timeout` is too
  small, and neither is improved by resuming it.

  Catch `MODAL_INTERRUPTIONS`, never `BaseException` — a `NameError` is a
  `BaseException` too, and resuming one would run a deterministic failure
  until the budget is gone.

  The Modal runner reports the interruption inside the grace window the
  platform allows, which releases the execution claim and its
  concurrency-limit slots immediately and wakes the scheduler directly — so
  recovery does not depend on the (opt-in) watchdog. It reports only when
  nothing else will recover the execution: before the function timeout
  fires an escaping `BaseException` gets the input restarted by Modal on
  the same call id, which is faster and keeps the claim.

  **Reactive builds only.** `sd.build`/`build_aio` have no resumption path;
  a timed-out execution fails the task there as it always did.

- **`FunctionSettings` gains `nonpreemptible` and `startup_timeout`**, the
  two Modal knobs this topic needs that were not previously expressible
  through `StardagApp`. `nonpreemptible=True` is the direct answer to
  "this task must not be preempted" (3× CPU/memory price, not supported
  for GPU functions).

### Deployment

- **Cognito self-signup now defaults to off** (AWS CDK templates). The user
  pool was created with `selfSignUpEnabled: true`, so a deployment reachable
  from the public internet let anyone register — and the Registry API
  auto-provisions an internal user and personal workspace on first login.
  It is now a config flag, `COGNITO_ALLOW_SELF_SIGNUP`, defaulting to
  `false`. **Self-hosters who rely on open registration must set it
  explicitly.** Note this governs _native_ registration only: with a
  federated IdP, Cognito still auto-provisions on first login regardless —
  the README covers the pre-sign-up-Lambda and IdP-side options.

- **Dependency security floors raised** across the API's transitive
  dependencies, the UI's dev/build tooling, and `aws-cdk-lib`
  (2.215 → 2.264, picking up a critical handlebars advisory and CDK
  tooling CVEs). Dev lockfiles relocked and Dependabot configured.

- **`aws-cdk` CLI 2.1030.0 → 2.1136.0**, to match the library above. 2.264
  emits cloud assembly schema 54.0.0 and the old CLI reads at most 48.x.x,
  so `cdk synth` refused the manifest and every deployment from
  `infra/aws-cdk` failed at step one. It fails closed — nothing is
  deployed, no stack is left half-updated — but **self-hosters deploying
  the CDK templates need this bump**, not just the library one. Bootstrap
  stack version 6 is still all the templates require; no re-bootstrap.

## [0.18.0] — 2026-08-11

### Registry API

- **The API now knows which SDK is calling it, and can say so when that
  stops being good enough.** The SDK reports its own version in a dedicated
  header, `X-Stardag-SDK-Version`; the server parses it, records it (one log
  line per distinct version per process, so "which SDK versions are actually
  calling us?" is answerable), and exposes it on `request.state.sdk_version`.
  A descriptive `User-Agent` rides along for logs, and the server
  deliberately never parses it — a policy decision must not depend on a
  free-form string that proxies rewrite.

  Alongside it, a compatibility floor: `STARDAG_API_SDK_MINIMUM_VERSION`.
  **It is unset by default, and unset means no request is ever rejected** —
  which is the state this release ships in, because the API remains wire
  compatible with every SDK released so far (changes have been additive
  fields, parameters and endpoints; no response has changed shape or
  meaning). Nothing about any client's behaviour changes here. When it _is_
  set, an SDK below it gets `426 Upgrade Required` with a body naming the
  version it is on, the version required, and the `pip install --upgrade`
  command to get there.

  The rules that make the switch safe to flip later: a **missing** header is
  never fatal (every SDK released before the header existed sends nothing,
  and must keep working), a **malformed** value is treated identically to a
  missing one and never 500s, and comparison is real PEP 440 — with
  pre-release, dev and local builds counted as their base release, so a
  `0.18.0.dev1` local build is not told to upgrade to `0.18.0`. `/health`
  and `GET /api/v1/version` are never gated, so a refused client can always
  fetch the policy that refused it.

  `GET /api/v1/version` gains `minimum_sdk_version` (`null` by default), so
  the SDK, the docs and support read one number from one place.

  **Process rule:** an API change that raises `minimum_sdk_version` must
  state it in `CHANGELOG.md`, in `RELEASE_NOTES.md`, and in the error the
  server returns. See "Releasing the Server" in `DEV_README.md`.

- **Task refs now carry `attempt_count`, so a scheduler can run a retry
  policy** ([#208](https://github.com/stardag-dev/stardag/issues/208)).
  Reactive scheduling records a failed execution and never respawns it —
  retries are the execution backend's job — but a backend's function-level
  retries only cover exceptions raised _inside_ a container that started.
  A spawn that failed before the container existed, an OOM kill, a
  preemption, or a worker that died after writing partial output are
  invisible to them, so under fail-fast a single transient failure killed
  the whole build. Deciding otherwise needs a _durable_ attempt count: a
  tick is short-lived and cannot remember what it already tried.

  `attempt_count` is how many times execution has been started for a task
  **since its build's most recent `BUILD_RESUMED` event** — since the
  build began, if it never was. Exposed on `FrontierTaskRef` (all of
  `actionable`, `running` and `roots`), on `GET /builds/{id}/tasks`, and on
  every task lifecycle event response so a caller that has just recorded a
  failure or won a claiming start can apply its budget without a second
  round-trip. Always populated wherever it is declared.

  Three things it is easy to assume and get wrong:

  - **It counts attempts, not `TASK_STARTED` events.** Engines emit
    several starts per execution — an acquiring start for the claim /
    limit slot before the spawn (no executor ref), a second carrying the
    ref, plus the worker's own start when the executor self-reports
    lifecycle. A run of consecutive starts collapses to the one execution
    it describes, which makes the number the same whichever engine and
    executor ran the task.
  - **The scope is the build, not the environment** — unlike the
    `latest_*` fields it travels with. A task that spent two attempts in
    an earlier build arrives in a fresh trigger with a full budget.
  - **A resume resets it; a retry does not.** `build_trigger(...,
build_id=<existing>, reactive=True)` is the recommended way to pick a
    failed reactive build back up, and it does _not_ mint a new build: it
    resumes this one and retries the failed tasks in it. Counting over a
    build's whole history would therefore leave the budget spent the
    moment the user asked for another go, and resuming would mean also
    raising `max_attempts`. `BUILD_RESUMED` is the durable marker of
    "another round was asked for" — and the server already skips it for a
    build with no activity beyond `BUILD_STARTED`, so a first trigger is
    unaffected. `TASK_RETRIED` on its own does not reset, because a
    scheduler retries _through_ that endpoint: a counter cleared by it
    would be cleared by every enforcement of the budget it defines. So a
    bare retry against a spent budget is a real "this round is out of
    attempts", and resuming is the answer to it.

  Derived from the event log rather than denormalised — attempts are
  per-build, per-round, and there is no per-(build, task) row to hang a
  column on — as one grouped query bounded to the tasks being reported,
  with the round cutoff riding along as a correlated subquery. No
  migration; the frontier costs exactly one extra query, and none at all
  when it has no tasks to report. Purely additive: every existing field
  keeps its exact semantics.

- **Reactive scheduler tick summaries are now persisted per build**
  (`POST`/`GET /builds/{build_id}/tick-summaries`). A reactive build is
  driven by many short-lived scheduler ticks, each in its own container,
  and each tick's `TickSummary` — the scheduler's own account of what it
  did and why it did nothing — previously reached nothing but that
  container's log. Reconstructing why a build stalled meant correlating
  logs across dozens of containers; it is now a single request. The
  summary is stored verbatim in a JSONB blob (with `outcome` promoted to
  an indexed column), so the SDK can add fields without a server release
  or a migration. Retention is bounded per build (newest 50 by default,
  `STARDAG_API_MAX_TICK_SUMMARIES_PER_BUILD`), pruned on insert.
  Additive: nothing writes to these endpoints yet.
- **A failed build now reports _why_, on the build itself.**
  `BuildResponse.latest_error_message` carries the reason recorded on the
  build's newest `BUILD_FAILED` event, so "why did this fail" no longer costs a
  `GET /builds/{id}/events` per build — which is why no listing ever showed it.
  Reported while the build is `failed` and not afterwards: a build resumed after
  failing is running again, and a current status paired with a previous round's
  reason misleads worse than no reason. A blank reason is normalised to `null`,
  so "none recorded" has one representation. Derived from the event rather than
  denormalised onto the row (unlike `Task.latest_error_message`), batched into
  one grouped query per page on the listing path.

- **`GET /builds/{id}/frontier` now reports why a build is waiting on work
  it does not own** ([#208](https://github.com/stardag-dev/stardag/issues/208)).
  Dependency gating is environment-global (task rows and edges are shared
  across builds) while `running`/`status_counts` cover only the tasks a
  build has events for, so an upstream left non-COMPLETED by another build
  could gate a build's tasks while contributing nothing it could see — a
  scheduler then read "nothing actionable, nothing running" as "cannot
  progress" and failed the build. The new `blocked_by_external` list pairs
  each such blocked task with its blocker (identity, status, status
  timestamp, the owning build id, the claim's expiry where the blocker holds
  one, the attempts the blocker has spent in this build's round, and whether
  the blocker is in this build's task set), capped with an explicit
  `blocked_by_external_truncated` flag. Purely additive: every existing field
  keeps its exact semantics.

  `blocking_in_build` is reported for diagnostics and is not the field a
  scheduler should branch on: what happens next follows from the blocker's
  status. Plan closure (below) makes `true` the normal case, but `false` stays
  reachable — closure runs once, at registration, so an edge written afterwards
  is outside the plan, which is what happens whenever a concurrent build's
  worker yields dynamic dependencies into its own plan.
  `blocking_attempt_count` is `null` for a blocker outside the plan, which is
  also what keeps a scheduler from resetting one: it has no budget to spend
  there.

- **Registration closes a build's plan over every recorded dependency edge**
  ([#208](https://github.com/stardag-dev/stardag/issues/208)). A build's plan
  is every dependency of its roots that was not complete at discovery time,
  pruned at complete tasks. Discovery enforces that by walking
  `requires()` — **static** edges. But gating consults every recorded edge,
  and _dynamic_ edges are written by whichever build first ran the task and
  then outlive it: environment-global and permanent. So a later build that
  statically discovers the same task inherited the dependency without
  inheriting the task, and was gated on an upstream **no build containing it
  could schedule** — the only thing that would produce it being the very task
  being gated. Permanent deadlock.

  Both registration endpoints now admit incomplete upstreams into the plan,
  transitively, stopping at complete tasks. Admission is a status-neutral
  `TASK_REFERENCED`: the upstream's own state is untouched, it simply becomes
  part of this build's plan, which is what makes it schedulable here.
  Over-approximating is safe and under-approximating is not — a stale edge
  costs one unnecessary upstream, a missing one deadlocks — so no attempt is
  made to judge whether a recorded edge is current.

  RUNNING upstreams are admitted too. Closure runs **once**, at registration,
  while RUNNING is transient: excluding them would leave a permanent hole in
  the plan the moment the task stopped running, and the likeliest way for it
  to stop is an operator releasing a stale claim — the documented remedy
  stranding every build that inherited the dependency. Safety comes from the
  claim, not from which build started the task.

  **Visible consequence, and it is the intended one:** an incomplete upstream
  now belongs to the build that depends on it, so `GET /builds/{id}/graph`
  reports it as a **primary** node rather than as greyed-out context, and
  `upstream_depth` reveals only _complete_ upstreams. It also joins the
  build's own `actionable`/`running` and its status counts. Nothing to change
  on your side, but the counts and the graph will look different.

- **Frontier task refs now actually carry `latest_status_at`.** The field
  was declared and documented as the input to scheduler staleness bounds
  but never populated, so it always serialised as `null` and those guards
  silently did nothing.
- **`POST /builds/{id}/tasks/{task_id}/retry` accepts `suspended`.** A task
  suspended for dynamic dependencies and then abandoned (orchestrator died,
  build cancelled) was permanently unschedulable — the only escape was an
  undocumented cancel-then-retry. `running` remains non-retryable on
  purpose: it holds a live execution claim, and releasing that is
  cancellation, not retry.
- **`GET /tasks` can enumerate claim holders**
  ([#208](https://github.com/stardag-dev/stardag/issues/208)). New
  `status` filter (repeatable, so `?status=running&status=suspended`
  works) and `status_older_than` (an absolute ISO-8601 cutoff) answer
  "which tasks in this environment are holding an execution claim, and for
  how long?". `latest_status` is environment-global, so a task left RUNNING
  by a build whose orchestrator died denies the claim to every future build
  that needs it. When either filter is applied the list is ordered oldest
  claim first; unfiltered ordering is unchanged. Backed by a new
  `(environment_id, latest_status, latest_status_at)` index.
- **`TaskResponse` now carries `latest_status`, `latest_status_at` and
  `latest_status_build_id`.** The last is the claim holder — "running under
  build Y since T" — and was previously unavailable outside the build
  frontier. Purely additive.
- **`POST /builds/{id}/cancel` accepts `cascade=true`.** Cancelling a build
  wrote a single build-level event and nothing else, so the claims and
  concurrency-limit slots its tasks held survived it indefinitely. With
  `cascade` the build's RUNNING/SUSPENDED tasks are cancelled in the same
  transaction. Scoped to tasks whose current status _this_ build produced,
  so it can never declare another build's live execution dead; PENDING tasks
  are left alone for the same reason. Default off — it is a behaviour change,
  and the SDK's fail-fast path cancels its own running tasks.
  The response gains `cascaded_task_ids` / `cascaded_task_count`.
- **New `POST /builds/bulk-cancel`: bulk cleanup and a stale-build reaper.**
  Nothing terminated abandoned builds: build status is derived from events,
  so a build whose orchestrator died without emitting a terminal one stays
  RUNNING forever, and interrupted local runs, crashed CI jobs and failed
  triggers accumulate permanently. One endpoint serves both shapes —
  `build_ids` for an explicit set, `idle_for_seconds` for staleness — with
  `dry_run`, `cascade` (on by default here), and reactive builds excluded
  unless asked for. Only builds whose derived status is RUNNING are ever
  touched, so it is idempotent.

  **Idleness is measured on activity, not on `last_active_at`.** That column
  is bumped by build-level lifecycle transitions only — task events skip it
  so worker traffic doesn't contend on the build row — so a build running
  tasks for three days still shows its BUILD_STARTED timestamp there, and
  reaping on it would cancel live work. The signal used is the newest of the
  build's entire event stream, its `last_active_at`, and any pending
  scheduler wake-up (`needs_tick_at`).

- **`BuildResponse` exposes `last_active_at` and `last_activity_at`** — the
  ordering column and the reaper's idleness signal respectively — so a UI can
  show operators the same number the reaper acts on.
- **`GET /builds` accepts `idle_for_seconds`**, using the same idleness
  definition and the same 60s floor as `bulk-cancel` (one shared SQL
  predicate, not a second implementation) — so a client can list what the
  reaper would cancel before cancelling it. Because it is a real SQL
  predicate, `total` is an exact `COUNT(*)` and pagination is server-side
  and unbounded. Ordering flips to stalest-first when it is given. It
  combines with any `status` value — see the denormalisation entry below,
  which made that true for the non-`running` ones too.
- **Optional unattended sweep** (`STARDAG_API_REAPER_ENABLED`, off by
  default) runs the same operation on a timer inside the API process. Note
  that every replica runs its own timer with no leader election; cancellation
  is idempotent, so concurrent sweeps are wasteful rather than wrong.
- **The execution claim now has an expiry, so an abandoned claim heals
  itself** ([#208](https://github.com/stardag-dev/stardag/issues/208)).
  `Task.latest_status == RUNNING` _is_ the claim, and it recorded no
  liveness evidence a third party could evaluate: a holder that vanished
  denied the task to every future build indefinitely and leaked its
  concurrency-limit slots with it. New nullable column
  `tasks.latest_status_expires_at`, written once when a start grants the
  claim — **not** a lease, nothing heartbeats it. A claim past its expiry
  simply is not a claim: the next claiming start takes the task over,
  replacing the dead holder's build, executor fields and expiry together.
  No reaper, no release call, no new status a user has to understand.
  - `POST /builds/{id}/tasks/{task_id}/start` accepts
    `claim_ttl_seconds` (60 s … 30 days, 422 outside that). Set it from the
    executor's own timeout plus a small grace — the caller is the only
    party that knows how long the execution may legitimately take. Omitted,
    it falls back to `STARDAG_API_CLAIM_DEFAULT_TTL_SECONDS` (default
    7 days). That fallback is deliberately generous because it is what a
    caller that does _not_ derive a TTL gets — every SDK predating this
    change, and any newer one whose executor declares no timeout — so it
    must be a bound no realistic task reaches. Expiring late only delays a
    heal that today never happens; expiring early hands a live task to a
    second claimant. It is a backstop, not the cleanup path: a claim held
    by an abandoned _build_ is released within a day by the reaper's
    cascade, and an operator can release any claim immediately.
  - **The concurrency-limit count uses the same predicate**, so an expired
    claim releases its slots — otherwise the leak would survive in the one
    place nobody reads. `GET /concurrency-limits/{key}/holders` matches
    (eviction deliberately still reaches an expired holder, whose task is
    RUNNING to every status reader until an event says otherwise).
  - Surfaced as `latest_status_expires_at` on frontier task refs and on
    `ConcurrencyLimitHolder`, as `blocking_status_expires_at` on
    `blocked_by_external` entries, and in the `task_already_running` 409
    detail — so a scheduler can act on evidence instead of inferring death
    from elapsed time.
  - **The migration backfills the claims that are already RUNNING**, as
    `latest_status_at + <default TTL>`. Those rows are the population this
    feature exists to heal — a task RUNNING since three months ago is an
    abandoned claim, not one that "never lapses" — and leaving them null
    would have shipped the fix while excluding every case that motivated
    it. The value is correct in both directions without guessing: a claim
    abandoned long ago backfills to a timestamp already past, so it is
    immediately re-claimable; one genuinely running across the deploy
    backfills to a future timestamp and is untouched, and its next
    `TASK_STARTED` re-stamps it from the caller's TTL anyway. Rows with no
    `latest_status_at` to measure from are left null.
  - Purely additive otherwise: `NULL` means "no expiry known" and is
    treated as a claim that never lapses, exactly as before this column.
    After the backfill that is a much narrower population than it sounds —
    a claim stamped by a server predating the column and not re-started
    since — and those still need an operator (cancel, retry, evict) to
    release. Executor probing is unaffected and remains the better evidence
    where it is available.
- **Build status is now a column, so `GET /builds?status=` is exact for
  every status** ([#208](https://github.com/stardag-dev/stardag/issues/208)).
  It was derived by replaying the build's build-level events on every read,
  which had three consequences, all now gone: every `BuildResponse` cost an
  event scan; the `status` filter could not run in SQL, so it scanned the 500
  most-recently-active candidates, filtered in Python, and returned a `total`
  that was **the matches within that window** while looking like an exact
  count; and anything needing an unbounded "is this build RUNNING?" — the
  stale-build reaper — had to carry a second SQL encoding of the same rule,
  which disagreed with the replay when a terminal event shared a timestamp
  with a start/resume.

  Five denormalised columns on `builds` (`latest_status`,
  `latest_started_at`, `latest_completed_at`,
  `latest_status_triggered_by_user_id`, `latest_is_resumed`) now hold exactly
  what the replay returned, folded in-transaction by every build lifecycle
  path — the same pattern already used for `tasks.latest_*`. Backed by a new
  `(environment_id, latest_status, last_active_at)` index. Migration
  backfills existing builds by replaying their events, so no build changes
  status across the upgrade.

  - `status` filtering is a plain column predicate: exact `COUNT(*)`,
    server-side pagination, no window and no cap, for every status.
  - `status` combined with `idle_for_seconds` **no longer 422s** for
    non-`running` statuses. That restriction existed only because `running`
    was the sole value with a SQL predicate; "failed, and idle for a week" is
    now a real query.
  - **Tie-break:** two build-level events sharing a `created_at` resolve in
    _arrival_ order — the order the server committed them — because the fold
    shares a transaction with the event insert. Equal timestamps mean the
    timestamp lost information the commit order still has, and both previous
    implementations were guessing (the replay arbitrarily, the reaper by
    always reading a tie as "not running"). The reaper is unaffected in
    practice: it still only touches builds that are RUNNING _and_ have been
    silent for at least `idle_for_seconds`.
  - No response field changed shape or meaning.

### UI

- **A Builds view, replacing "Home".** Builds are listed with status,
  duration, last activity and the reactive app that owns them, and can be
  filtered by status, by owning app, and by how long they have been idle.
  The idle filter means _abandoned_, so it implies Running: a finished
  build has no activity by definition, and including terminal builds would
  fill a staleness listing with history sorted oldest-first.

- **Bulk cleanup of abandoned builds**, admin-gated to match the API. Rows
  are selectable per page, and "Clean up idle builds…" sweeps the whole
  environment on the server rather than the visible page. Both paths open a
  confirmation showing a **dry run of the real query** — the same selection
  `POST /builds/bulk-cancel` will act on, including the per-build reasons
  anything was skipped — because a preview that disagrees with the action
  is worse than no preview.

- **A failed build says why it failed.** The scheduling panel explains a build
  while it is _stalled_, and goes quiet the moment it fails — failing skips the
  blocked tasks, and terminal tasks leave the frontier's blocker list. The
  reason the scheduler recorded on its way out (which task, its status and age,
  the owning build, why nothing will move it, and the remedy) is now shown on
  the failed build, in the place the panel's explanation occupied. Untruncated,
  because the remedy is at the end of it.

- **A build that is not progressing now says why.** When nothing is
  actionable and nothing is running, the build view names each blocking
  upstream, its status, how long it has been held, and which build owns it.
  Each carries what happens next, which follows from the blocker's
  **status**: `running` resolves when the claim finishes or expires;
  `cancelled` is a revocation, so this build's next tick resets it and runs
  it; `suspended` resolves as the owning build works through the dynamic
  dependencies it yielded; `failed` and `skipped` are results left to this
  build's `fail_mode`, so re-triggering is the way to reset them. Behind a
  disclosure: the scheduler's own tick trail, with runs of identical ticks
  collapsed ("lease held ×20") and counters that stayed at zero dropped, so
  a stalled build's repetition reads as a diagnosis instead of a log.

- **Claim triage in the task explorer.** Lists the tasks holding an
  execution claim, longest-held first, with the build that owns each one,
  and releases them in bulk. Only tasks actually holding a claim are
  selectable, and every release reads the resulting status back — a task
  that finished between listing and acting is reported as such rather than
  counted as released.

- **Fixed:** unchecked checkboxes rendered as white boxes in dark mode
  (`color-scheme` was never set, so every native control used the light
  theme); durations past a day read as `371h 41m` instead of `15d 11h 41m`;
  dates rendered in the browser's locale order (`8/9/2026` or `9/8/2026`
  depending on the reader) rather than `YYYY-MM-DD`.

### SDK

- **Reactive builds now discover the DAG inside Modal, not on the machine
  that triggers them.** `build_trigger(..., reactive=True)` used to walk
  the whole DAG locally, which meant one target existence check per task
  from the triggering process. For a `modalvol://` target root each of
  those is a Volume API call from outside Modal, and they are rate
  limited: triggering a 127-task DAG from a laptop spent ~64 s almost
  entirely in backoff, and before that was hardened it failed outright
  with `VolumeListFiles rate limit exceeded`.

  The trigger now mints (or resumes) the build, registers the root tasks —
  neither of which touches a target — and spawns a new deployed
  **`bootstrap`** function with the roots passed by value. The bootstrap
  does the walk, the registration, the task-module coverage check and the
  task-store writes _in the container_, where the same volume is a mounted
  filesystem, then arms the build and spawns the first tick. Triggering is
  fast and needs registry credentials only; it performs no target I/O at
  all.

  `bootstrap` is its own function rather than work folded into the first
  tick because the two need different timeouts: a tick is one frontier
  pass (and its timeout derives the per-pass spawn cap), while discovery
  is a single whole-DAG walk paid once per trigger. It defaults to
  `builder_settings` — the same image, secrets and volume mounts as the
  builder, which runs the same discovery for resident builds — and is
  configurable with the new `StardagApp(bootstrap_settings=...)`.

  The ordering that makes ticks safe is preserved and now stated in code:
  the reactive marker (`reactive_app_name`, without which a tick no-ops)
  is written **last**, after discovery and persistence, so no tick can
  ever observe a partially-registered DAG. Failure handling is preserved
  too, on both sides of the spawn — anything that fails once a trigger
  knows the build is `RUNNING` records a terminal `BUILD_FAILED` before
  propagating, including failures inside the bootstrap container and a
  failed first-tick spawn. A re-trigger whose `build_resume` fails is
  deliberately excluded: until that lands the build may still be terminal.

  `BuildTriggerResult.function_call` now carries the bootstrap call for
  reactive triggers (previously the first tick's), which is the honest
  handle: it is what the trigger spawned, and its failure is what means
  the build never started.

  `require_pickle_free=True` is still enforced and still fails loudly —
  now from the bootstrap, where the task store is written: it records a
  terminal `BUILD_FAILED` and re-raises on the bootstrap's Modal call. A
  side benefit of the move: the coverage check now compares your DAG
  against the **deployed** `task_modules` list rather than your local one,
  closing the stale-deploy blind spot it used to carry. The trigger also
  prints a labelled, roots-only advisory before spawning, so the common
  "I never declared my package" case still shows up in your terminal.

  Resident (non-reactive) builds are completely unaffected. Set
  `StardagApp(reactive_discovery="local")` to run the identical bootstrap
  in the triggering process — the previous behaviour — for apps deployed
  before the `bootstrap` function existed, or when the target root is
  reachable from the trigger but not from the Modal app. **Redeploy your
  app** to get the `bootstrap` function.

- **The SDK now identifies its version to the registry.** Every registry
  request carries `X-Stardag-SDK-Version` (plus a descriptive `User-Agent`
  for logs; the server keys on the header, never on the agent string). This
  ships ahead of anything that reads it, because the check only works
  forwards: a server can tell an SDK "you are too old" only if that SDK was
  already announcing itself when it was released, and no later server change
  fixes a silent release retroactively.

  When a registry is configured with a minimum SDK version and this SDK is
  below it, the `426 Upgrade Required` response now raises
  `SDKVersionUnsupportedError` (exported from `stardag`), carrying the
  server's own message — which names both versions and the exact
  `pip install --upgrade` line — plus `sdk_version` and
  `minimum_sdk_version`. CLI commands print that message as written rather
  than a repr. No minimum is configured by default, so nothing changes for
  an up-to-date pair.

- **`stardag builds cleanup` and `stardag builds ticks` say when the
  registry is too old**, instead of reporting the missing endpoint as
  "resource not found" — which read as a bad build id and sent people
  looking for a build that was fine. Both now name the command, the missing
  endpoint and the upgrade; `cleanup` also points at
  `stardag builds cancel <build-id>` as the one-at-a-time fallback. A
  genuine resource-level 404 is unaffected.

- **Reactive builds now have a task-level retry policy.** A reactive tick
  recorded a failed execution and never respawned it, on the reasoning that
  retries are the execution backend's job. They partly are — a backend's
  function-level retries (Modal's `retries=`) cover exceptions raised
  _inside_ the container — but they cannot cover a spawn that failed before
  any container existed, an execution the backend killed (OOM, timeout), a
  preempted worker, or one that died after writing partial output. Under
  `FAIL_FAST`, any one of those ended the whole build.

  `TickConfig.max_attempts` (default **2**, also accepted as a Modal
  `tick_kwarg`) is a budget, per task per build _round_, on how many
  executions the scheduler starts. A failure the tick records is reset to
  pending and picked up on the next pass while the budget allows. The
  budget covers exactly the failures no backend can retry: a failed spawn,
  an execution the backend reports failed, and a task whose execution claim
  lapsed with nothing left to probe. It deliberately does **not** cover a
  task whose object cannot be rehydrated — the same absence on the second
  reading — and it never sees an exception inside a task at all, since the
  worker self-reports that and the task leaves the frontier. Set
  `max_attempts=1` for the previous behaviour.

  A **round** runs from the build's most recent `BUILD_RESUMED` event, which
  makes the recovery path the one you already reach for: **re-triggering the
  build** (`build_trigger(..., build_id=<this build>, reactive=True)`)
  records `BUILD_RESUMED` ahead of its discovery retries, so every task
  starts the new round at zero — optionally with a raised budget via
  `tick_kwargs={"max_attempts": N}`. A **bare** retry (the UI's Retry,
  `stardag tasks retry`, the retry route) does not start a round and does
  not reset anything.

  Exhaustion is loud in both directions. A tick that declines to respawn
  names the task, the attempts spent, the budget and the re-trigger. And on
  a task already at budget, a bare retry succeeds server-side while the
  scheduler still refuses to start it — previously a silent no-op; now the
  tick says exactly that, distinguishes it from a re-trigger, fails the task
  again rather than leaving it pending and inert, and spells out the
  re-trigger that would work. New `TickSummary` counters `retried`,
  `retry_exhausted` and `budget_denied` carry the same facts into the
  persisted per-build summary.

  Resuming a **suspended** task is never budget-gated: a dynamic-dependency
  yield records a fresh start, so gating resumption would cap dynamic
  dependencies rather than retries. Server support is required
  (`attempt_count` on the frontier); against a registry that does not report
  it, no budget can bound a retry loop, so retries stay off and the tick
  says why.

- **Reactive ticks fan out concurrently.** Acting on a frontier was a plain
  `for` loop with awaits inside it: per actionable task, a task-store read,
  an execution-claim acquisition, an executor spawn and a start recording
  the ref — 2–3 registry round-trips plus a spawn, strictly serialised. A
  layer thousands of tasks wide was therefore thousands of sequential HTTP
  calls in one short-lived container, racing a function timeout nothing
  related it to. Each pass now runs those actions with bounded concurrency
  (`TickConfig.max_concurrent_actions`, default 50 — the bound the resident
  engine has always used). Ordering _within_ a task is unchanged: the
  acquiring start still precedes the spawn (a denied task never occupies a
  worker) and the ref-recording start still follows it.

- **A per-tick spawn cap, derived from the tick container's own timeout.**
  The old cap was "however many tasks are actionable", which is unrelated
  to how long the container may live. `TickConfig.max_spawns_per_tick`
  bounds one pass; left unset it is derived as a duration budget — a
  fraction of the tick's wall-clock limit, spread over the in-flight bound.
  The limit is resolved down a ladder: the explicit cap, then
  `TickConfig.tick_timeout_seconds` (which the Modal integration fills in
  automatically from the `timeout` the deployed `tick` function carries,
  falling back to `builder_settings` exactly as function registration
  does), then the executor's `execution_timeout_seconds` as a fallback
  proxy, then a conservative default. Every tick logs its cap and which
  rung produced it. Truncation is logged, never silent, and never a stall:
  the pass acted, so the tick re-evaluates on a fresh frontier immediately
  and takes the next batch. `max_spawns_per_tick` and
  `max_concurrent_actions` are accepted as Modal `tick_kwargs`;
  `tick_timeout_seconds` deliberately is not — it is a deploy-time fact
  about the container, not per-build state.

  The watchdog sweeps every running build sequentially inside one
  container, so it now hands each build a proportional share of that
  container's budget rather than letting the first wide build size its
  fan-out as though it owned the whole timeout.

- **Concurrent DAG discovery in reactive mode.**
  `discover_and_register_aio` walked the DAG with a recursive `await
task.complete_aio()` and no concurrency, while the resident engine did
  the same work 50 at a time. That cost was not paid once per build: this
  walk runs at every reactive trigger _and_ in every worker registering
  dynamically yielded dependencies, i.e. on the hot path of every dynamic
  dependency. It is now bounded-concurrent on the same default
  (`max_concurrent_discover=50`). The completion checks overlap; the
  ordering does not — post-order registration (dependencies before the
  tasks that need them, so the bulk endpoint never creates phantom rows),
  diamond deduplication, `retry_failed` behaviour and all three
  `DiscoveryResult` collections come out identical to the serial walk's,
  element for element.

- **Blocker liveness is now read from the execution claim's expiry, not
  inferred.** A reactive tick decided whether to wait on a RUNNING upstream
  owned by another build from a table over `(in-build, status, owning-build
liveness)` plus a staleness bound on how long the blocker had sat in its
  status — an educated guess, since no build can probe another build's
  executor. The registry now stamps every execution claim with an expiry, so
  the question is answered by a read: a RUNNING blocker whose claim is live
  is waited on; one whose claim has **lapsed** fails the build, with the
  message saying the claim is provably abandoned rather than presumed so.

  What this does **not** change: proving a blocker dead does not make it
  schedulable. A RUNNING task is not runnable whoever holds the lapsed claim,
  so the build still fails — just with certainty about why, and pointing at
  the cancel that releases it. And the collapse applies to RUNNING blockers
  only: a SUSPENDED or PENDING blocker holds no claim and therefore carries
  no expiry, so "will anyone move it?" is still asked of its owning build (an
  abandoned-SUSPENDED upstream remains a real wedge, recovered by
  re-triggering the build that is waiting on it).

- **Every start records a claim TTL derived from the executor's own
  timeout.** For Modal that is the worker function's `timeout` from its
  `FunctionSettings`, plus a grace margin; where no timeout is known the
  registry's default applies. Granting an expiry on every start is what
  makes an abandoned claim heal, but it also means a task outliving its TTL
  could have its claim taken while alive — deriving the TTL from the limit
  the backend itself enforces is what keeps that from being a real risk.
  Setting an explicit `timeout` on long-running Modal workers is therefore
  worth doing.

- **Removed: `TickConfig.stale_running_no_ref_seconds` and
  `ClaimConfig.stale_running_no_ref_seconds`.** Both configured a local
  guess at how long "too long" is, which the claim's own expiry now answers.
  A task RUNNING without an executor ref (a scheduler that died between the
  claiming start and the spawn) is failed when its claim lapses instead of
  after a fixed bound, and a competing claimant recovers a ref-less winner
  on the same evidence. Against a registry that does not report expiry,
  every one of these paths waits rather than failing — a missing expiry
  means "never lapses", not "dead".

- `FrontierTaskRef.latest_status_expires_at` and
  `FrontierExternalBlocker.blocking_status_expires_at` model the new server
  field; `task_start`/`task_start_aio`/`task_start_claim_aio` accept
  `claim_ttl_seconds`; `TaskExecutorABC.execution_timeout_seconds` is the
  new (optional, default `None`) hook an executor implements to expose its
  wall-clock limit. `StartClaimResult.latest_status_at` is replaced by
  `latest_status_expires_at`.

- **`stardag builds show` prints a failed build's reason.**
  `BuildSummary.latest_error_message` models the new server field, and the
  command renders it as **Failure reason** — the most useful row on a failed
  build, since the reactive scheduler's reasons name the blocking task, the
  build that owns it and what to run. Absent on servers predating the field.

- **New `stardag builds` and `stardag tasks` CLI groups.** There was no way
  to list builds, inspect a build's scheduling frontier, cancel a build or
  task, or clean up abandoned state without writing a script against the
  registry API — which is what made a wedged or spuriously-failed build hard
  to diagnose: the failure was visible in logs and in the UI, but "what does
  the scheduler actually think the state is?" required hand-rolled calls.

  ```
  stardag builds list [--status running] [--reactive-app NAME] [--older-than 24h]
  stardag builds show <build-id>
  stardag builds frontier <build-id>
  stardag builds ticks <build-id> [--limit N]
  stardag builds cancel <build-id> [--cascade] [--yes]
  stardag builds cleanup [--older-than 24h] [--build-id ID ...] [--apply] [--yes]
  stardag tasks list [--status running] [--older-than 1h]
  stardag tasks cancel <build-id> <task-id> [--yes]
  stardag tasks retry <build-id> <task-id> [--yes]
  ```

  `builds frontier` is the diagnostic one: besides the actionable/running
  partitions it renders the build's **external blockers** — tasks of this
  build held back by an upstream whose current status _another_ build
  produced — naming the blocking task's namespace/name (not just an id), its
  status, how long it has been in it, the owning build, and what happens next,
  which follows from the status rather than from which build produced it
  (`running` waits on the claim, `cancelled` is reset by the next tick,
  `suspended` waits on the owning build, `failed`/`skipped` need a
  re-trigger). It also states honestly that the registry computes that list
  only for a
  build with nothing actionable and nothing running, so an empty list never
  reads as "no blockers" for a build that is merely progressing.

  `builds cleanup` is the recovery for builds abandoned by a process that
  died: build status is derived from build-level events, so such a build
  stays `RUNNING` forever while holding every execution claim and
  concurrency-limit slot its tasks had. It **defaults to a dry run** — the
  server's own selection, so what you review is what you get — printing the
  builds, the claims that would be released and any per-build skip reasons.
  **`--apply` is the only thing that makes it act**; `-y/--yes` only skips
  the confirmation prompt, so `cleanup -y` on its own is still a dry run.
  Cascade is on by default here, and reactive builds are excluded unless
  asked for.

  `--older-than` accepts `24h` / `90m` / `3d` (one number, one optional unit
  of `s`/`m`/`h`/`d`/`w`; bare numbers are seconds) and converts at the
  boundary to whatever the endpoint takes — a duration for builds, an
  absolute cutoff for tasks. It is applied server-side by the same predicate
  the reaper uses, so `builds list --older-than 24h` and
  `builds cleanup --older-than 24h` agree on what is stale; on `builds list`
  it **implies** `--status running` and may not be combined with any other
  status (idleness only means anything for a build that has not finished —
  a completed one has no activity by definition, and always will). A registry older than the
  CLI silently ignores the filter, so the command detects that and warns on
  stderr that the results are unfiltered rather than quietly filtering the
  page itself (which would under-report exactly the oldest builds).

  The read-only commands — and `cleanup`'s dry run — take **`--json`**, a new
  convention for the CLI: stdout carries exactly one JSON document (the SDK's
  model of the API payload) and every hint, warning and prompt goes to
  stderr, so piping to `jq` is safe.

- **Reactive scheduler ticks now report their `TickSummary` to the
  registry** (`stardag builds ticks <build-id>`). A reactive build is driven
  by many short-lived ticks, each in its own container, so the summary — the
  scheduler's own account of what it did and why — used to reach nobody but
  that container's log, and reconstructing why a build stalled meant reading
  logs across dozens of them. Reporting is strictly best-effort: it sits at
  the end of every tick and can never fail one, change its outcome or mask
  its exception; it tolerates a registry that predates the endpoint (and
  stops retrying a route that 404s); and every outcome except `not_reactive`
  is recorded. A tick that **crashes** is recorded too, under a new
  `"error"` outcome carrying `TickSummary.error_type` and a length-bounded
  `error_message` — the most informative thing a "why did this build stall?"
  query can find — after which the original exception is re-raised
  untouched. Turn reporting off for a deployment with
  `TickConfig(report_tick_summaries=False)` — app-level configuration, like
  the other staleness knobs, not a per-trigger `tick_kwarg`. The summary is
  stored verbatim server-side, so future `TickSummary` fields need no server
  release.

- **The reactive watchdog's build sweep now filters server-side.**
  `build_list_running` passed no `status` and matched on the derived status
  in Python, so an environment holding more non-running builds than the
  sweep's page budget could starve it of the running builds it exists to
  find — silently disabling the safety net exactly when a backlog makes it
  necessary. Same ordering, page budget and truncation warning as before.

- **New registry-client methods** for the operational surface, all on
  `RegistryABC` (with safe defaults) and `APIRegistry`: `build_list`,
  `build_get_summary`, `build_bulk_cancel`, `build_report_tick_summary[_aio]`,
  `build_list_tick_summaries`, `task_list`, and id-addressed
  `task_cancel_by_id[_aio]` / `task_retry_by_id[_aio]` (operator tooling only
  ever has the id, and rehydrating a task object to cancel it would fail for
  exactly the abandoned tasks that most need cancelling). `build_cancel` gains
  a `cascade` keyword and now returns the cancelled build plus the claims the
  cascade released, or `None` for backends that don't report it — the same
  optional-return convention as `task_register_bulk`. New response models
  `BuildSummary`, `BuildListPage`, `BuildCancelResult`, `BulkCancelResult`,
  `BulkCancelBuildRef`, `TaskSummary`, `TaskListPage` and `TickSummaryRecord`
  are exported from `stardag.registry` and ignore unknown response fields.
  `build_list` takes the server's `status`, `reactive_app_name` and
  `idle_for_seconds` filters.

- **`StardagApp(task_modules=[...])`: declare the modules whose import
  registers your task classes, so reactive scheduler ticks can rebuild
  tasks from registry data instead of pickles.** A tick reconstructs task
  objects from the registry's stored payload, which resolves a class
  through the polymorphic registry — populated only as a side effect of
  importing the defining module. Without a declaration, whatever a tick
  container happens to import is arbitrary, so the build task store's
  pickles were load-bearing (and needed target-root write access at
  trigger time, and were invalidated by every redeploy).

  Patterns are exact modules (`"my_pkg.tasks.ingest"`) or trailing
  recursive wildcards (`"my_pkg.tasks.*"`); the default infers the root
  package of the module defining the app, and `[]` opts out. They are
  expanded to a concrete module list at deploy time (without importing
  submodules) and baked into the deployed tick, so **adding or moving task
  classes requires a redeploy**; `stardag modal deploy` reports the
  expansion (`--no-check-task-modules` skips the warn-only local import
  check). Task modules are imported in every tick container, so keep heavy
  runtime dependencies inside `run()` rather than at module scope.

  With the declaration in place, a reactive trigger writes **no pickle**
  for any task whose class is covered and whose payload round-trips to the
  same task id — a fully covered build needs no target-root write access
  at all. Everything else keeps its pickle exactly as before, including
  `AliasTask` payloads (pickled `loads_type`, never auto-unpickled from
  registry data by design) and non-importable classes. The trigger warns
  about classes the patterns don't cover, naming the pattern to add;
  `require_pickle_free=True` turns that fallback into a hard error.

  Skipping pickles requires declaring `task_modules` explicitly; the
  inferred default only drives the coverage warning. Upgrading stardag
  therefore changes nothing on its own — a newer SDK triggering against an
  app deployed by an older one still writes pickles, because eliding them
  would depend on a baked-in module list that deployment does not have.
  Resident (non-reactive) builds are unaffected either way, and
  `task_modules=[]` behaves exactly as before. **Redeploy the app whenever
  you change `task_modules`**, before triggering.

- **Fixed: a reactive build no longer fails because another build is
  running one of its upstreams.** Task state is per environment, so an
  upstream some other build left RUNNING gates this build's tasks — while
  contributing nothing to the `running` count and status counts a tick
  sees, which are scoped to this build. That shape read as "nothing
  runnable, nothing running, so this build is dead", and the build was
  failed within seconds of triggering with an error naming only status
  counts. Common whenever DAGs overlap, and worst when the blocker was a
  dynamic dependency registered under an earlier build — which plan closure
  (see the Registry API section) now pulls into the new build's plan instead
  of leaving it gating a build that could never schedule it.

  A tick now reads the frontier's blocking upstreams and asks, for each,
  whether anyone is going to move it. A blocker **another build is
  executing** is waited out (like a busy concurrency-limit slot — its
  completion wakes this scheduler), and so is one **a still-live build has
  yet to schedule**: that build is going to run it, and failing here would
  only trade one spurious failure for another. A blocker **no live build is
  going to run** — its owning build has gone terminal, no build owns its
  status, or that status could not be resolved — fails the build
  immediately, naming the task, its namespace/name, its status, how long it
  has been in it, the build that owns it, why that owner will not move it,
  and the one remedy there is: **re-trigger this build**, which resets the
  blocker (it is in this build's plan) and runs it here.

  **Which of those questions gets asked is decided by the blocker's status,
  not by which build owns it.** A build's plan is closed under the dependency
  relation, so a gating upstream is this build's own task; deferring to
  whichever build last touched it is what turned one build's fail-fast into
  every overlapping build's failure. A `CANCELLED` blocker is a revocation of
  permission to run, not a verdict on the task, and permission is not
  build-scoped — the tick **resets it and runs it**, bounded by
  `max_attempts`. `FAILED` and `SKIPPED` are _results_: `fail_mode` owns them
  (FAIL_FAST has already failed the build on the same count; CONTINUE means
  "finish what you can, then fail"), so a tick names them in the failure and
  changes nothing. A `SUSPENDED` blocker is waited on while its owning build
  lives, because resetting it would redo all of the task's pre-yield work
  while that build is legitimately progressing the children it yielded. At
  **trigger** time the whole retryable set is reset, `RUNNING` excepted — the
  asymmetry is deliberate: at trigger you asked, mid-flight nobody did.

  Waits are bounded by evidence rather than by a timer. For a RUNNING blocker
  it is the claim's expiry; for a SUSPENDED or PENDING one — which holds no
  claim, so has no expiry to read — it is the owning build going terminal,
  and a build gone silent without transitioning is reaped server-side.
  `TickSummary` gains `external_blockers`, `external_blockers_waited`,
  `external_blockers_fatal` and `in_build_blockers_reset`, and `BuildInfo`
  gains `status` (the build's derived status, `None` when a server or custom
  registry does not report it). Owner liveness is resolved only when a build
  actually looks stalled, only for the blockers whose status needs it, and
  once per owning build per pass, so a healthy build issues no extra requests
  however often it polls. Requires a stardag-api version matching this SDK;
  against an older server the blocker list is always empty and terminal
  detection behaves exactly as before.

- **Fixed: re-triggering a reactive build now recovers tasks left
  `SUSPENDED`.** A task that suspended for dynamic dependencies and was
  then abandoned (its orchestrator died, or the build was cancelled) was
  permanently unschedulable: the re-trigger's retry pass skipped it, and
  the only escape was to cancel it purely to reach a status that _was_
  retryable and then retry. `suspended` joins failed/cancelled/skipped in
  the set a re-trigger resets to pending. Safe because a suspended task has
  no live execution — the suspension means the execution yielded and
  returned — so nothing can be orphaned. Workers registering dynamically
  yielded dependencies do not retry at all and are unaffected. `running`
  remains deliberately non-retryable: it holds a live execution claim, and
  releasing that claim is cancellation, not retry.

- **Fixed: a task stuck RUNNING with no executor ref is now actually
  recovered.** A scheduler that died between the claiming start and the spawn
  leaves a task RUNNING that no worker will ever report on.
  `stale_running_no_ref_seconds` was supposed to bound that, but it was
  measured against `FrontierTaskRef.latest_status_at`, which the server
  declared and never populated — so it serialised as `null` on every ref and
  the guard was dead code for its whole life. The frontier now populates the
  field, and the bound it feeds has been replaced outright by the claim's
  expiry (see the removal above), which is evidence rather than a guess and
  needs no tuning for long ref-less tasks.

### Fixed

- **The reactive watchdog now asks the registry only for the RUNNING builds
  its own app owns** (`GET /builds?status=running&reactive_app_name=...`,
  filters the API has supported since the reactive-metadata release but the
  SDK never used). Previously each sweep paged the whole build listing,
  filtered the derived status client-side, and then spent a full `tick`
  invocation on every RUNNING build in the environment just to discover
  most were not reactive. Worse, the sweep's per-period cap was consumed by
  those irrelevant builds: an environment holding more stale RUNNING builds
  than the cap could stop reaching genuine reactive builds entirely, and
  silently — disabling the safety net exactly when it was needed. The
  truncation warning now says how many _reactive_ builds owned by _which
  app_ it truncated, and what to do about it. `build_list_running` gained an
  optional `reactive_app_name` argument; the client-side status re-check is
  retained so a server predating the filters degrades to a wider listing
  rather than to ticking terminal builds. Note that scoping removes the
  incidental cross-app coverage a sweep used to provide: a build owned by
  an app deployed without a watchdog is no longer swept by another app's
  watchdog. ([#208](https://github.com/stardag-dev/stardag/issues/208) A3)
- **A reactive trigger that fails part-way no longer leaves a build stuck
  in RUNNING forever.** `build_trigger(reactive=True)` mints the build
  before running discovery and persisting the task store, and a build's
  status is derived from its events — so a failure in between (most often a
  target-root permission or storage error on the task-store write) left a
  RUNNING build that nothing would ever terminate and that carried no
  reactive owner, so it was invisible to its own app's sweep and pure
  overhead for every other one. All post-mint trigger work is now wrapped:
  any failure emits a terminal `BUILD_FAILED` naming the stage that failed
  before the original exception propagates (a failure to record that event
  is logged, never allowed to mask the root cause).
  ([#208](https://github.com/stardag-dev/stardag/issues/208) A4)

## [0.17.0] — 2026-08-06

### SDK

- **The published distribution now ships a [PEP 561](https://peps.python.org/pep-0561/)
  `py.typed` marker** (plus the `Typing :: Typed` classifier), so type
  checkers use stardag's inline annotations instead of discarding them. No
  API change — the minor bump reflects the downstream effect below.

  **Downstream:** mypy previously skipped the package entirely —
  `module is installed, but missing library stubs or py.typed marker` —
  and treated every stardag symbol as `Any`, so genuine mismatches in
  consumer code went unreported. They are now flagged, which means a
  previously green mypy run can surface new — real — errors after
  upgrading. Two workarounds also go stale and should be removed:
  `# type: ignore[import-untyped]` comments on stardag imports, and any
  `ignore_missing_imports` override for `stardag.*` (`warn_unused_ignores`
  will report them as unused). Pyright already resolved stardag's types
  from the installed package; the only change there is that strict mode
  no longer emits `reportMissingTypeStubs`.

## [0.16.1] — 2026-07-17

### Fixed

- **`stardag self-host` now creates a shared workspace named after your
  Modal workspace by default.** Modal's token lookup returns an empty
  `workspace_name` for _both_ personal and team/org workspaces (only
  `username` differs), so the CLI could no longer tell them apart and
  misclassified org workspaces as "personal": it never set
  `AUTH_PRIMARY_WORKSPACE_NAME`, and the server bootstrapped `main` in the
  admin's personal workspace instead of a shared one. The primary workspace
  is now an explicit, well-defaulted choice keyed off the Modal `username`
  (always present): `up`/`connect` default to creating a **shared** Stardag
  workspace named after the Modal workspace (with the admin as owner) and
  wire the target root, API key, and local profile to _that_ workspace's
  `main` environment. Use `--no-primary-workspace` for solo/individual use
  (personal workspace), or `--primary-workspace NAME` for an explicit name.
- **`stardag self-host connect`/`up` no longer overwrite an existing
  `stardag-api-key` Modal secret without confirmation.** Pushing the secret
  into an execution Modal environment that already had one could silently
  repoint all DAG execution in that environment (e.g. an existing
  cloud/app.stardag.com setup) to the self-hosted registry. When the secret
  already exists the CLI now warns and requires a typed confirmation phrase
  interactively, or the explicit `--overwrite-api-key-secret` flag under
  `--yes`; otherwise it leaves the secret untouched and completes the rest
  of the setup. (The standalone `stardag modal stardag-api-key create`,
  whose purpose is to (re)push the secret, now prints a warning when it
  replaces an existing one.)

## [0.16.0] — 2026-07-17

### SDK

- Default prebuilt server image bumped to `server-v0.1.1` (rebuilt from
  the v0.15.0+ line, so a prebuilt `self-host up` now serves the UI with
  the corrected version footer). `DEFAULT_SERVER_VERSION = "0.1.1"`.
- **`stardag self-host` prebuilt deploys no longer require a matching
  client Python
  ([#196](https://github.com/stardag-dev/stardag/issues/196)).** The
  prebuilt-image path now defines the Modal `web`/`migrate` functions by
  reference (`serialized=False`) against a module-level entry point that
  Modal imports inside the server image, instead of cloudpickling closures
  with the client interpreter. Because nothing is serialized, the CLI runs
  under any supported Python (≥ 3.10) — the previous `uvx --python 3.12 …`
  requirement (and its fail-fast check) is gone. `--from-source` is
  unchanged (still serialized closures with a client-matched image Python).

## [0.15.0] — 2026-07-17

### SDK

- **Self-host the Stardag service on Modal with one command
  ([#187](https://github.com/stardag-dev/stardag/issues/187)).** New
  `stardag self-host` CLI (`up`/`upgrade`/`status`/`destroy`, extra:
  `stardag[selfhost]`): provisions a Postgres database on Neon from an
  API key (or bring-your-own via `--database-url`), applies migrations,
  and deploys the Registry API + web UI as a single Modal web endpoint.
  Deploys a prebuilt public server image by default
  (`--server-version`); `--from-source` builds from a repo checkout
  (the UI compiles inside the Modal image build — no local Node/Docker
  required). See the "Self-host on Modal" guide.
- **`stardag self-host up` completes the whole setup** (new `connect`
  subcommand re-runs it idempotently): the server app (`server`) + its
  secrets are isolated in a dedicated Modal environment (`stardag-host`,
  flag `--server-modal-env`); a primary Stardag workspace is created
  mirroring a shared Modal workspace's name (`--primary-workspace` /
  `--no-primary-workspace`; personal Modal accounts use the personal
  workspace) with a `main` environment; an API key is minted and pushed
  as the Modal secret `stardag-api-key` into the DAG-execution Modal
  environment (`--execution-modal-env`); a default target root
  `modalvol://stardag-targets-<workspace-slug>-<environment-slug>/default`
  (a dedicated Modal volume per workspace + environment) is registered
  (`--target-root`/`--no-target-root`); and a local SDK registry +
  profile (`selfhosted`) are written. In OIDC auth mode, `connect` runs
  the browser login first and provisions via the API.
- **`stardag auth login` supports local-auth registries**: when the
  registry reports `auth_mode=local` it prompts for email/password and
  stores the session token; the existing token-refresh chain uses it
  transparently.

### Registry API

- **Local authentication mode** (`AUTH_MODE=local`): email/password
  accounts managed by the API itself — no external identity provider.
  New endpoints `POST /auth/login`, `POST /auth/register` (disabled by
  default), `POST /auth/change-password`; user-scoped session tokens
  (`token_use=session`) accepted by `/auth/exchange` and bootstrap
  endpoints; bcrypt hashing with timing-uniform verification; login
  rate limiting; idempotent bootstrap-admin provisioning at startup
  (`AUTH_BOOTSTRAP_ADMIN_EMAIL`/`_PASSWORD`). OIDC mode (default) is
  unchanged.
- **Primary workspace bootstrap** (local auth mode): startup idempotently
  ensures a shared workspace named `AUTH_PRIMARY_WORKSPACE_NAME` (bootstrap
  admin as owner) and an `AUTH_PRIMARY_WORKSPACE_ENVIRONMENT` environment
  (default `main`; empty disables) — in the named workspace, or in the
  bootstrap admin's personal workspace when no name is set.
- `GET /auth/config` now serves the full client auth configuration
  (auth mode, issuer, UI client id, Cognito domain, registration flag)
  so UIs and CLIs can be configured at runtime.
- `GET /api/v1/version` reports the server version
  (`STARDAG_SERVER_VERSION`, stamped by the release pipeline) and the
  installed API package version.
- Serverless/pooler support: `STARDAG_API_DATABASE_URL_DIRECT`
  (migrations bypass transaction-mode poolers) and
  `STARDAG_API_DATABASE_POOLER_COMPAT` (asyncpg prepared-statement
  settings for PgBouncer-style poolers).

### UI

- Auth/API configuration resolves at runtime from the API — a prebuilt
  UI bundle works against any IdP or auth mode without rebuilding
  (build-time `VITE_*` values still take precedence when set).
- Local-auth mode: sign-in/registration page and a "Change password"
  action in the user menu.
- The Settings page footer shows the server version.

### Deployment

- **Server release pipeline**: git tags `server-vX.Y.Z` publish the
  combined server image `ghcr.io/stardag-dev/stardag-server:X.Y.Z`
  (Registry API + web UI + migrations, one joint version) and attach
  the built UI dist to the GitHub release. `app/server.Dockerfile` is
  the single image definition; `scripts/server-version.sh` derives
  truthful versions for non-release builds (`X.Y.Z+N.g<sha>`).
- AWS CDK: optional `apiImageUri` to run the public server image
  instead of building to ECR; `deploy-ui.sh --release` deploys the
  prebuilt UI dist; opt-in CloudFront same-origin `/api/*` proxy
  (`uiApiProxy=true`); ECR pull-through-cache recipe documented.

## [0.14.0] — 2026-07-16

### SDK

- **Exactly-once task execution by default (execution claims,
  [#185](https://github.com/stardag-dev/stardag/issues/185)).** Task
  starts now carry an atomic per-task _claim_ wherever a registry with
  claim support is configured and the execution is probeable (detached
  Modal executions — resident and reactive): a start racing an
  already-RUNNING task is denied with the running execution's ref echoed,
  and the loser re-attaches to the winner instead of spawning a duplicate
  (or self-heals an existing completion, records a provably dead winner
  and re-claims, or waits for a ref-less winner with backoff —
  `ClaimConfig`). This closes the cross-build both-see-PENDING race that
  previously could run one task in two workers. Control via
  `build(..., claim=None|True|False)` and `TickConfig.claim`; older
  servers/custom registries degrade gracefully. Custom arbitration
  backends implement `RegistryABC.task_start_claim_aio`
  (`StartClaimResult`).
- **`GlobalLockConfig` is deprecated** in favor of claims (a
  `DeprecationWarning` is emitted when enabled). The lock remains
  functional for executions without probeable liveness; the engine now
  **renews held locks in the background**, fixing the 60s-TTL expiry
  under long-running tasks. The `GlobalConcurrencyLockManager` protocol
  itself is unchanged (it backs the reactive scheduler lease and remains
  the registry-less escape hatch).

### Registry API

- `POST .../tasks/{id}/start` accepts `claim=true`: atomically deny with
  409 `task_already_running` (echoing `executor`/`executor_ref`) or
  `task_already_completed` inside the FOR-UPDATE start transaction. A
  denied claim records nothing — including no concurrency-limit slots
  (all-or-nothing with `enforce_limits`).

## [0.13.0] — 2026-07-16

### SDK

- **Strict (bare concrete) polymorphic fields now also reject subclass data on
  the deserialize path.** v0.12.0 rejected a subclass _instance_ assigned to a
  strict field; it now also rejects _serialized data_ — an input dict whose
  `__namespace`/`__name` discriminator resolves to a subclass of the declared
  strict type — instead of silently coercing it into the base type (dropping the
  subclass's parameters). Plain dicts without a discriminator are unaffected
  (validated as the exact strict type). **Note:** loading data that was already
  lossily truncated into a strict field by a pre-0.12.0 version now raises
  `StrictPolymorphicTypeError` rather than loading the degraded base type — the
  correct "fail loud on corrupt data" behavior; switch the field to
  `SubClass[...]` if it should accept subclasses.

## UI Only — 2026-07-15

### UI

- **Task detail "Execution" section: minimal Modal call-ref line.** Shows the
  function-call id (`fc-…`) as a link to the Modal call page, with a button
  that copies the id itself; the per-level app/function/environment deep
  links now live in the collapsible "More details" table (each value linked,
  with a copy-the-value button).

## [0.12.0] — 2026-07-15

### SDK

- **Bare abstract task-typed fields are now rejected at class-definition time.**
  A field annotated directly with an abstract polymorphic base (e.g.
  `child: BaseTask`, `deps: list[Task[int]]`) rather than wrapping it in
  `SubClass[...]` / `TaskLoads[...]` used to serialize by silently dropping
  every subclass-specific parameter and then crash on load (the payload tried
  to instantiate the abstract base directly). Such annotations now raise
  `NakedPolymorphicFieldError` as soon as the class is defined, with a message
  pointing at the correct polymorphic form.

- **Bare _concrete_ task-typed fields are now strict (exact-type) at
  validation time.** A field like `child: MyTask` (a concrete base, without
  `SubClass[...]`) means exactly `MyTask`. Passing a _subclass_ instance
  (`child=ChildOfMyTask(...)`) previously succeeded but silently dropped the
  subclass's extra parameters on serialization — and, because task identity is
  derived from the serialized form, distinct subclass values collapsed to the
  same task id. This now raises `StrictPolymorphicTypeError` at construction,
  directing you to `SubClass[MyTask]` if you intend to accept subclasses.
  Passing an exact-type instance is unaffected.

## [0.11.0] — 2026-07-15

### SDK

- **Reactive build metadata moved from the target root to the registry.**
  The reactive marker, owning app name, and `tick_kwargs` used to be stored
  in a `meta.json` on the default target root; they now live in the
  registry (the build's `reactive_app_name` + `reactive_tick_kwargs`,
  surfaced on the build frontier the tick already fetches, and on the
  lighter `GET /builds/{id}` the pre-lease gate now uses). The per-build
  task store is now pickle-only (task _objects_ still live on the target
  root). Because the registry is mutable — unlike a possibly-immutable
  target root — **a re-trigger may now update `tick_kwargs`** (previously
  fixed at first trigger in 0.10.1); a _bare_ re-trigger (no explicit
  `tick_kwargs`) preserves the stored config. Reactive scheduling now also
  requires a registry server new enough to support the reactive-meta
  endpoint; an older server fails the reactive trigger clearly (matching the
  existing frontier/notify version contract) rather than degrading silently.

  **⚠️ Upgrade note:** reactive builds already in flight when you upgrade
  across this release are **not** migrated — the tick now reads the marker
  from the registry only, and pre-upgrade builds have no registry marker, so
  their ticks no-op silently. **Re-trigger any in-flight reactive build**
  (`build_trigger(..., build_id=<id>, reactive=True)`) after upgrading. See
  RELEASE_NOTES.md.

- **Modal executor metadata now records the app id (`app_id`, `ap-…`) and
  worker function id (`function_id`, `fu-…`).** These ride the existing
  executor-metadata channel (base metadata + the worker env-override
  propagation, read back by the worker lifecycle reporter) so a worker's
  self-reported start carries them too. They let the UI build stable
  dashboard deep links in the app-id URL form, which keeps resolving after
  an app version is stopped or redeployed (the deployed-app-name form does
  not). Best-effort throughout: on any error the key is simply omitted rather
  than raised, so resolution never fails a task start. The two lookups sit on
  the critical path before `spawn`, so they can add latency — but each is
  bounded by a short (3 s) timeout, so a slow or hung Modal API cannot stall
  a start beyond that cap. Resolved values (including a resolved-but-missing
  id) are cached per process, so a failing lookup is not re-paid on every
  start.
- **CLI: `stardag concurrency-limits` command group for managing named
  concurrency limits.** Wraps the registry's concurrency-limit endpoints for
  the active profile / environment (override with `-p/--stardag-profile` and
  `-e/--stardag-env`). Subcommands: `list` (with optional `--holders` counts);
  `set` to upsert a limit (`stardag concurrency-limits set <key> <max_concurrent>`);
  `delete <key>` (`--yes` to skip confirmation); `holders <key>` (RUNNING slot
  holders, oldest first); and `evict` to free leaked slots
  (`stardag concurrency-limits evict <key> <task_id>`). Replaces the need for an
  ad-hoc script to `PUT /api/v1/concurrency-limits/{key}`. Backed by new
  `APIRegistry` `concurrency_limit_{list,set,delete,holders,evict}` methods.

### Registry API

- **Reactive-scheduling metadata on builds.** Two new nullable columns:
  `builds.reactive_app_name` (indexed `String` — the owning app + marker;
  NULL = not reactively scheduled, so presence
  (`reactive_app_name IS NOT NULL`) is the marker) and
  `builds.reactive_tick_kwargs` (JSONB — the SDK-owned `TickConfig` kwargs).
  Set via a `PUT /api/v1/builds/{id}/reactive-meta` upsert endpoint
  (env-scoped, rate-limited); `tick_kwargs` is only updated when provided,
  so a bare re-trigger preserves it. Both fields are exposed on the build
  response and the build frontier so a reactive scheduler tick reads
  marker/owner/config in the call it already makes. `GET /api/v1/builds`
  gains `reactive_app_name` and `status` filters (e.g.
  `?reactive_app_name=<app>&status=running`) so "RUNNING reactive builds
  owned by app X" — the watchdog's real question — is a server-side query.
  Additive/nullable migration (instant).

### UI

- **Stable, stop/redeploy-proof Modal function-call deep links.** The task
  detail and concurrency-holder "View on Modal" links previously used a
  query-param form that didn't resolve. They now build the app-id URL
  (`.../apps/{workspace}/{env}/{app_id}?activeTab=functions&functionId=…&functionSection=calls&fcId=…`)
  when the newly captured `app_id`/`function_id` metadata is present,
  falling back to the deployed-app-name form and then the plain app-page
  link. The app-page fallback itself prefers the stable app-id form
  (`.../apps/{workspace}/{env}/{app_id}`) whenever `app_id` is available —
  so metadata with an `app_id` but no `function_id` still degrades to a
  stop/redeploy-proof link rather than the deployed-name page. Reads the
  new metadata defensively, so older data without the ids still gets a
  working, non-dead link. Pairs with the SDK change that records
  `app_id`/`function_id` in the executor metadata.
- **Task detail: "more details" block for Modal identifiers.** The
  Execution section now has a collapsible list of every captured Modal
  identifier (kind, app/function names, workspace, environment, app id,
  function id, and the function-call ref), each click-to-copy, so a
  reference can be reconstructed by hand if the dashboard URL format
  drifts. Only present fields render, and the block is gated to Modal
  executions — it never surfaces its Modal-labeled fields for an
  explicitly non-modal executor kind.
- **Sidebar: shortened the "Concurrency Limits" nav item to "Concurrency"
  and made every nav label left-aligned and single-line (truncating with
  an ellipsis instead of wrapping and centering).** Each item also carries
  a `title` tooltip with its full label, so a truncated label stays
  readable on hover.

## [0.10.2] — 2026-07-14

### SDK

- **`StardagApp(stardag_api_key_secret=...)`: cleaner registry-credential
  handling.** A single, explicitly named secret (default
  `"stardag-api-key"`, the name `stardag modal stardag-api-key create`
  uses) is injected into every function (build, workers, tick, watchdog) —
  all of which talk to the registry. Accepts a `modal.Secret`, a name
  (`str`, resolved lazily), or `None` to disable. A by-name secret that
  doesn't exist raises a clear error at `finalize()`. **This replaces the
  0.10.1 behavior of propagating _all_ builder-declared secrets to the
  workers/tick** — per-function `secrets` are now function-local again;
  only the api-key secret is shared. Declare the registry key via this
  argument (or rely on the default) rather than putting it in
  `builder_settings.secrets`.
- **Fix: Modal workspace now resolves and populates executor metadata /
  the app-level UI dashboard link.** The workspace was resolved from the
  Modal token, which only exists in the local triggering/deploy process —
  inside a Modal container (where task-level metadata is produced) the
  lookup returned nothing, so the UI showed a blank workspace. Two fixes:
  the token lookup now falls back to the account `username` (the
  `WorkspaceNameLookupResponse.workspace_name` is empty for personal
  workspaces), and `finalize()` resolves the workspace at deploy time and
  bakes it into every function's env (`STARDAG_MODAL_WORKSPACE`), which the
  in-container resolver reads first. (The per-function-call deep-link URL
  format is a separate UI fix, tracked as a follow-up.)
- Completed the `StardagApp.__init__` docstring (previously several
  arguments were only described in inline comments).

## [0.10.1] — 2026-07-14

### SDK

- **Fix: reactive Modal scheduling crashed in fresh containers.** A
  `resource_provider` (e.g. `registry_provider`) captured by a
  `serialized=True` Modal function is cloudpickled by value; its unset
  sentinel is a bare `object()` whose identity does not survive pickling,
  so a deserialized provider returned that bare `object()` from `get()`.
  In a deployed app this crashed the reactive scheduler `tick` and the
  scheduled `tick_watchdog` (which runs cold, with no build) with
  `AttributeError: 'object' object has no attribute 'build_list_running'`.
  Providers now serialize without their live resource and re-initialize
  lazily in the new process (`ResourceProvider.__getstate__`/`__setstate__`).
- **`StardagApp` propagates the builder's secrets to workers and the
  tick/watchdog.** Since worker-side lifecycle reporting, every deployed
  function talks to the registry, so all of them need registry
  credentials — but the secret is naturally declared only on the builder.
  `finalize()` now applies the builder's declared secrets to the worker
  functions and the tick/watchdog, de-duplicated by name (a function that
  also declares the same secret still gets it once). Previously workers
  `401`ed on their self-reported lifecycle events unless the secret was
  repeated on every worker.
- **Fix: re-triggering a reactive build crashed on an immutable/no-overwrite
  target root.** The per-build task store rewrote its `meta.json` on every
  re-trigger (add-roots / retry), but a target root may refuse overwrites
  (Modal volumes raise; an immutable/object-locked S3 root would too), so
  the re-trigger crashed with `FileExistsError`. The store is now
  write-once: the reactive marker is written only at the first trigger, and
  task pickles are skipped if already present. Build roots are tracked
  solely in the registry (the scheduler reads them from the frontier), so a
  re-trigger no longer mutates the store. Note: `tick_kwargs` are fixed at
  first trigger until reactive build metadata moves to the registry.

## [0.10.0] — 2026-07-13

Modal as a first-class execution layer. A large, fully backward-compatible
feature release: restart-safe build triggering, detached task execution
with re-attach, worker-side lifecycle reporting, reactive (tick-based)
scheduling (experimental), registry-backed named concurrency limits,
pickle-free task rehydration, and executor metadata surfaced as Modal
dashboard deep links in the UI, plus a concurrency-limits admin surface.
See
[RELEASE_NOTES.md](RELEASE_NOTES.md#v0100--modal-as-a-first-class-execution-layer)
for the SDK-user overview and upgrade notes.

### SDK

- **`stardag/integration/modal`**: New `StardagApp.build_trigger()` — triggers
  a build with the registry build id minted at the trigger point and passed to
  the Modal build function as `resume_build_id`. Restarts of the build
  function (Modal retries after preemption, or re-triggering with the returned
  `build_id`) resume the same build instead of creating a new one:
  already-completed task outputs are detected during discovery and skipped.
  Returns a `BuildTriggerResult(build_id, function_call)`. Requires registry
  credentials in the calling process; `build_spawn` remains available for
  Modal-credentials-only triggering.
  ([#154](https://github.com/stardag-dev/stardag/pull/154))
- **`stardag/build` + `stardag/integration/modal`: detached task execution
  (restart-safe long-running tasks).** `ModalTaskExecutor` now spawns worker
  invocations as detached Modal function calls by default instead of holding
  blocking `remote` calls: the function call id is recorded with the
  TASK_STARTED event, and a build that is restarted (Modal retry after
  preemption, or a `build_trigger` re-trigger with the same build id)
  **re-attaches to still-running workers instead of re-executing them**. A
  task's execution now survives orchestrator crashes. FAIL_FAST and user
  cancellation explicitly cancel the tracked function calls (previously,
  workers of a dead build kept running). Opt out with
  `ModalTaskExecutor(detached=False)` / `Builder(detached=False)`.
  Generic executor surface: `TaskExecutorABC.supports_detached()` /
  `submit_detached()` / `reattach()` + `DetachedHandle`, so other execution
  backends can implement the same semantics; `RoutedTaskExecutor` forwards.
  ([#155](https://github.com/stardag-dev/stardag/pull/155))
- **`stardag/registry`**: `task_start[_aio]` accepts optional
  `executor`/`executor_ref`; `task_register_bulk[_aio]` returns per-task
  `RegisteredTaskInfo` (current global status + executor ref) used by the
  build engine for re-attach. Custom `RegistryABC` implementations with the
  old signatures keep working (refs are dropped gracefully).
  ([#155](https://github.com/stardag-dev/stardag/pull/155))
- **`stardag/integration/modal`: worker-side lifecycle reporting.** The
  default `Runner` now reports the task's lifecycle from inside the worker —
  TASK_STARTED (carrying the worker's own function call id as executor ref),
  TASK_COMPLETED + artifact upload, TASK_SUSPENDED (dynamic deps), and
  TASK_FAILED — whenever the executor forwarded a build id (via the
  `STARDAG_BUILD_ID` env override; no worker signature change, older
  deployed workers are unaffected) and the container has registry
  credentials. Events therefore land even if the build orchestrator dies
  mid-task, and each re-invocation records a fresh re-attachable ref. The
  build engine suppresses its own completed/suspended/resumed reporting for
  such tasks (`TaskExecutorABC.reports_lifecycle` seam), keeping started
  (immediate detached-spawn re-attachability) and failed (fallback for
  workers that die before reporting) — duplicate events are tolerated by
  the event-sourced status derivation. Opt out with
  `ModalTaskExecutor(worker_reports_lifecycle=False)` (required when driving
  an app deployed with an older stardag from a newer local SDK) or
  `Runner(report_lifecycle=False)`. New `stardag.build.get_current_build_id()`
  exposes the ambient build id inside `build[_aio]()`.
  ([#161](https://github.com/stardag-dev/stardag/pull/161))
- **Reactive (tick-based) build scheduling for Modal — experimental.** A
  build can now run with **no resident orchestrator**:
  `StardagApp.build_trigger(tasks, reactive=True)` runs discovery at the
  trigger, persists the task objects to a per-build task store under the
  default target root, and short-lived, idempotent scheduler **ticks**
  (spawned by the trigger, by workers finishing tasks, and by an optional
  periodic watchdog) drive the build: each tick fetches the build's
  scheduling frontier from the registry, spawns ready tasks as detached
  Modal function calls, probes running refs (leaving live ones alone,
  self-healing completions from target existence, recording failures),
  handles terminal states (completed / failed / externally cancelled —
  cancelling running function calls), then lingers briefly on the build's
  wake-up flag and exits when quiet. Long-running builds therefore cost no
  orchestrator container time while tasks execute, and there is no
  orchestrator to crash. Ticks are single-flighted per build via a
  scheduler lease on the existing distributed-lock service. New public
  surface: `stardag.build.run_tick_aio` / `TickConfig` / `TickSummary` /
  `BuildTaskStore` / `discover_and_register_aio`,
  `TaskExecutorABC.detached_status()` / `cancel_detached()` /
  `DetachedExecutionStatus`,
  `StardagApp(watchdog_period_minutes=..., tick_settings=...)`. Current
  limitations (documented in
  `stardag/build/_reactive.py`): requires a registry; the global
  concurrency lock and build-local `ConcurrencyConfig` limits are not
  applied by ticks — registry-backed named limits are (see the entries
  below). ([#157](https://github.com/stardag-dev/stardag/pull/157))
- **Registry-backed named concurrency limits (reactive scheduling).**
  Environment-level named limits (`PUT /api/v1/concurrency-limits/{key}`) cap how
  many tasks tagged with a key may run concurrently — **across builds**, which
  build-local `ConcurrencyConfig` semaphores never could. A task
  occupies a slot simply by being RUNNING with the key recorded at start
  (no leases/TTLs: status liveness is already maintained by worker
  reporting and tick self-healing). Acquisition is atomic in the
  task-start transaction (`/start?limit_key=...&enforce_limits=true`,
  409 when at capacity, all-or-nothing across keys). Reactive ticks
  acquire before spawning via a key selector configured on the deployed
  app (`StardagApp(limit_key_selector=...)`, engine-level surface:
  `TickConfig.limit_key_selector`) — a denied
  task stays in the frontier and proceeds when a slot frees (same-build
  releases wake the scheduler directly; cross-build releases are covered
  by the watchdog). New `RegistryABC.task_start_with_limits_aio` (default:
  no enforcement, for custom backends). Resident-mode (`build_aio`)
  integration ships in this release too, via `RegistryConcurrencyLimiter`
  (see below).
  **Requires a matching stardag-api version**: an older server ignores
  the enforcement parameters (no error), so deploy the server before
  relying on limits. A staleness escape hatch
  (`TickConfig.stale_running_no_ref_seconds`, default 30 min) fails
  tasks stuck RUNNING without an execution ref — e.g. a scheduler crash
  between slot acquisition and spawn — so leaked slots always free; the
  watchdog is strongly recommended when limits are enforced.
  ([#158](https://github.com/stardag-dev/stardag/pull/158))
- Reactive scheduling: on a failure terminal, ticks now mark tasks
  transitively blocked by the failure as **skipped** (server-computed via
  `POST /api/v1/builds/{id}/skip-blocked`) — mirroring the resident engine, so
  blocked tasks no longer dangle pending in the UI while the build shows
  failed. Together with the retry/roots endpoints below, reactive
  re-triggers now have a complete recovery story: failed builds can be
  re-triggered (failed tasks reset to pending) and roots added mid-build
  are covered by completion detection.
  ([#160](https://github.com/stardag-dev/stardag/pull/160))
- **Pickle-free task rehydration from registry data.** New
  `stardag.task_from_registry_data(task_data, expected_task_id=...)`
  reconstructs a task instance from the payload stored at registration
  (`TaskMetadata.body`) via the polymorphic validator — the payload is
  self-describing (embedded namespace/name discriminators, recursively
  for nested `TaskLoads`/`SubClass` fields). Requirements/limits: the
  defining module must be imported; nested task fields must use the
  polymorphic annotations; `AliasTask` payloads are rejected (they embed
  pickled bytes); the optional identity check guards against
  non-round-trippable custom serializers. Reactive scheduler ticks now
  use it as a fallback when a task's stored pickle is missing or
  unloadable (healing the store on success) — an app redeploy with
  compatible task definitions no longer breaks in-flight reactive
  builds. This is also the foundation for UI-triggered task retries.
  ([#162](https://github.com/stardag-dev/stardag/pull/162))
- **Registry-backed concurrency limits for resident builds.** New
  `stardag.build.RegistryConcurrencyLimiter` implements the
  `ConcurrencyLimiter` seam on top of the registry's named environment
  limits: pass a `RegistryConcurrencyLimiter(key_selector=...)` as
  `concurrency_limiter` to `build`/`build_aio` and the named caps are
  enforced server-side — shared with reactive builds and other resident
  builds, across processes and machines (the "future global,
  server-driven limiter" the seam was reserved for). Acquisition is an
  enforced task start (slot = RUNNING status; freed on completion,
  failure, cancellation, or dynamic-deps suspension — parity with
  `LocalConcurrencyLimiter`); denials block-and-retry at a configurable
  poll interval (with jitter), transient registry errors retry with
  exponential backoff, and an optional timeout fails the task. Requires
  a matching stardag-api version. Note: unlike reactive scheduling,
  resident mode has no automatic healer for slots held by a crashed
  build process — see the
  [concurrency-limits docs](docs/docs/concepts/build-execution.md#concurrency-limits)
  and the new holders/evict admin API below.
  ([#163](https://github.com/stardag-dev/stardag/pull/163))
- **Reactive builds are owned by their triggering app.** With multiple
  `StardagApp`s deployed in one environment, a scheduler tick from an
  app that doesn't own the build (per the `app_name` recorded at trigger
  time) now forwards the wake-up to the owner app's tick (best-effort)
  and returns `outcome="foreign_app"` instead of driving the build with
  the wrong app's code — previously whichever app's watchdog won the
  scheduler lease would tick every reactive build in the environment.
  Forwarding means wake-ups landing on the wrong app (e.g. a previous
  owner's still-running worker finishing after a takeover) are not
  dropped, and every app's watchdog doubles as cross-app coverage.
  Same-name redeploys are unaffected; move a build to a new app by
  re-triggering it from that app (rewrites ownership and re-persists the
  task objects — new ticks only: a mid-linger tick of the old owner
  drains first). Builds triggered by older SDK versions (no recorded
  owner) are ticked by any app, as before.
  ([#164](https://github.com/stardag-dev/stardag/pull/164))
- **Executor metadata for Modal executions.** Task starts and triggered
  builds now record a descriptive `executor_metadata` dict — for Modal
  the kind, app name, workspace, environment, and function name (plus a
  `reactive` flag at the build level) — surfaced by the UI as Modal
  dashboard deep links. Resolution is lazy, cached, and best-effort
  (workspace via a Modal token lookup; a failure never fails or delays a
  start); override with `StardagApp(modal_workspace=...)` or
  `ModalTaskExecutor(modal_workspace=...)`. Worker self-reported starts
  carry the same dict (forwarded via `STARDAG_MODAL_*` env overrides).
  Registry surface: optional `executor_metadata` on `task_start[_aio]`,
  `task_start_with_limits_aio`, `build_start[_aio]` and
  `build_resume[_aio]`, plus `DetachedHandle.executor_metadata`; custom
  `RegistryABC` implementations with the old signatures keep working
  (the metadata is dropped gracefully).
  ([#165](https://github.com/stardag-dev/stardag/pull/165))
- **`stardag/testing/modal`**: New `live_modal_guard()` centralizes gating of
  live-Modal tests, controlled by `STARDAG_MODAL_LIVE_TESTS`
  (`auto`/`1`/`0`) and an optional `STARDAG_MODAL_TEST_PROFILE` safety guard
  (skip unless the active Modal profile matches — protects shared/production
  workspaces from accidental test runs). Live test modules are now marked
  `modal_live`, so `pytest -m "not modal_live"` runs the pure unit tier. A new
  live-semantics test module pins the Modal platform behaviors stardag relies
  on (detached spawned calls, `FunctionCall.from_id` re-attach, call-id
  stability across retries, cancellation).
  ([#154](https://github.com/stardag-dev/stardag/pull/154))

### Registry API

- `POST /api/v1/builds/{id}/resume` no longer records a `BUILD_RESUMED` event for a
  "fresh" build (no activity beyond `BUILD_STARTED`), so attaching to a
  trigger-minted build id on the first run doesn't display the build as
  resumed. Real resumes (any task activity or terminal state) are recorded as
  before. ([#154](https://github.com/stardag-dev/stardag/pull/154))
- New reactive-scheduling endpoints: `POST`/`DELETE /api/v1/builds/{id}/notify`
  (scheduler wake-up flag, new `builds.needs_tick_at` column) and
  `GET /api/v1/builds/{id}/frontier` — the build's actionable tasks (global status
  pending/suspended/running with all upstream dependencies completed,
  including executor refs for liveness probing), per-status counts, root
  statuses, and build status, for scheduler ticks.
  ([#157](https://github.com/stardag-dev/stardag/pull/157))
- Named environment concurrency limits:
  `GET`/`PUT`/`DELETE /api/v1/concurrency-limits[/{key}]` (new
  `environment_concurrency_limits` +
  `task_limit_keys` tables) with atomic enforcement on task start (409
  `concurrency_limit_reached`; the environment's limit rows are locked
  while active RUNNING holders are counted, serializing concurrent
  acquires; re-starting a RUNNING task never self-blocks).
  ([#158](https://github.com/stardag-dev/stardag/pull/158))
- New `POST /api/v1/builds/{id}/skip-blocked`: emits `TASK_SKIPPED` for
  pending/suspended tasks transitively downstream of a
  failed/cancelled/skipped task (recursive dependency-edge closure, one
  transaction). ([#160](https://github.com/stardag-dev/stardag/pull/160))
- New `TASK_RETRIED` event + `POST /api/v1/builds/{id}/tasks/{task_id}/retry`:
  resets a failed/cancelled/skipped task to pending (never downgrades
  completed/running) — the retry path for reactive builds.
  ([#160](https://github.com/stardag-dev/stardag/pull/160))
- New `POST /api/v1/builds/{id}/roots`: append root task ids to a build
  (deduplicated), so completion detection covers roots added to an
  active build. ([#160](https://github.com/stardag-dev/stardag/pull/160))
- Executor metadata: task starts accept a JSON `executor_metadata` query
  param (recorded in the `TASK_STARTED` event metadata, denormalised to
  the new nullable `tasks.latest_executor_metadata` column with the same
  set/clear-on-every-start semantics as `latest_executor_ref`); build
  creation accepts an `executor_metadata` body field and
  `POST /api/v1/builds/{id}/resume` a JSON query param (new nullable
  `builds.executor_metadata` column — kept on resumes that don't carry
  metadata). Exposed as `latest_executor` / `latest_executor_ref` /
  `latest_executor_metadata` on task responses (detail, list rows,
  search results, build task rows, frontier refs, bulk-register refs)
  and `executor_metadata` on build responses. All additive/nullable —
  older SDKs and servers are unaffected. The metadata dict is capped at
  2 KB (compact JSON, 422 above) on all ingest paths.
  ([#165](https://github.com/stardag-dev/stardag/pull/165))
- Concurrency-limits admin: new `GET /api/v1/concurrency-limits/{key}/holders`
  (the RUNNING tasks currently counted against a key — task identity,
  running-since, executor fields; paginated via `limit`, oldest first)
  and `POST /api/v1/concurrency-limits/{key}/holders/{task_id}/evict` (records
  `TASK_FAILED` for a task that is currently RUNNING **and** holds the
  key — 404 otherwise, deliberately not a generic kill endpoint — freeing
  all its slots via the normal status transition; the evicting identity
  is recorded in the event). Closes the resident-mode slot-leak recovery
  gap: reactive builds self-heal leaked slots via scheduler ticks,
  resident builds now have an admin path. Eviction also sets the owning
  build's scheduler wake-up flag so reactive builds observe it promptly.
  ([#165](https://github.com/stardag-dev/stardag/pull/165))
- Concurrency-limit **writes are admin-gated on the user auth path**:
  `PUT`/`DELETE /api/v1/concurrency-limits/{key}` and the evict endpoint
  require the workspace ADMIN role (or higher) when authenticated as a
  user (JWT); API-key auth (machine credentials) keeps full access, and
  reads (limit list, holders) stay member-level.
  ([#165](https://github.com/stardag-dev/stardag/pull/165))
- Fix: `GET /api/v1/tasks`, `GET /api/v1/tasks/{task_id}` and the task registration
  responses now populate `is_phantom` (previously always the schema
  default `false`, so placeholder rows were indistinguishable from real
  tasks on these endpoints).
  ([#165](https://github.com/stardag-dev/stardag/pull/165))

### UI

- **Modal execution surfacing.** Tasks executed on Modal now show a
  "⚡ Modal" badge (tooltip shows the function call ref; click to copy)
  in the build task table, the Task Explorer, and DAG node hover. The
  task detail panel gains an **Execution** section — executor kind, app
  name, function name, call ref, and workspace/environment — with deep
  links into the Modal dashboard (app page and function call). The
  build view shows a "Modal: app-name" chip linking to the app page
  plus a "reactive" badge for tick-scheduled builds. All Modal URL
  patterns are centralized in `src/utils/modalLinks.ts`; links render
  only when the recorded metadata has the required fields (older
  servers / missing metadata degrade to plain text, never dead links).
  ([#166](https://github.com/stardag-dev/stardag/pull/166))
- **Concurrency limits admin view.** New env-scoped "Concurrency
  Limits" sidebar page: list the environment's named limits with
  current holder counts, create/edit/delete keys, and drill into a
  key's holders (task detail link, running-since, executor badge,
  Modal deep link) with an **Evict** action that fails a stuck RUNNING
  holder to free its slots — the recovery path for slots leaked by a
  crashed resident build process.
  ([#166](https://github.com/stardag-dev/stardag/pull/166))

### Docs

- Expanded the
  [Build & Execution concepts page](docs/docs/concepts/build-execution.md)
  with the new execution model: detached execution and re-attach,
  worker-side lifecycle reporting, reactive scheduling, and the
  concurrency-limit mechanisms (build-local, registry-backed named
  limits, global lock). The bundled `stardag` agent skill is updated to
  match. ([#159](https://github.com/stardag-dev/stardag/pull/159))

## [0.9.0] — 2026-06-16

### SDK

- **`stardag/build`**: Add build-level concurrency limits for task execution
  via a new `ConcurrencyConfig` accepted by `build` / `build_aio`. Supports an
  overall cap (`max_concurrent_tasks`) and named limits mapped to tasks through
  a callback (`limits={"request-to-service-x": 10}` with a `key_selector`); a
  task may be subject to multiple named limits at once. Enforced uniformly
  across all executors (local, Modal, routed) by gating the executor submit
  call. The slot is released while a task is suspended on its own dynamic deps
  and re-acquired on resume (unlike the global lock, which is held across
  suspension), and composes with the global concurrency lock.
  `ConcurrencyLimiter` is a protocol seam for a future global, server-configured
  limiter. Local to a single build for now.
  ([#151](https://github.com/stardag-dev/stardag/pull/151))
- **`stardag/integration/modal`**: A `WorkerSelector` may now return a
  `(worker_name, env_overrides)` tuple in addition to a bare worker name (new
  `WorkerSelection` type). When provided, `env_overrides` is a
  `dict[str, str]` of environment variables set temporarily around the task's
  `run` call inside the Modal worker and restored afterwards — e.g. to tune
  task-specific execution knobs (worker/thread counts, batch sizes, library
  env vars). `Runner.__call__` gained an optional `env_overrides`
  parameter; the `RunFunction` protocol's required signature is unchanged, so
  existing `(task)`-only run functions keep working (overrides are applied to
  the environment around the call for them). Also caches the per-worker
  `modal.Function.from_name` lookup in `ModalTaskExecutor` instead of
  recreating the handle on every submit. Backward compatible.
  ([#152](https://github.com/stardag-dev/stardag/pull/152))

## [0.8.1] — 2026-06-12

Compatibility fix for `stardag modal deploy` on modal >= 1.4.3. No
client-code changes required.

### SDK

- **`stardag/_cli/modal.py`**: Fix `stardag modal deploy` crashing with
  `ImportError: cannot import name 'ensure_env' from 'modal.environments'` on
  modal >= 1.4.3, where `ensure_env` moved to a private module. The small
  environment-resolution logic is now inlined using modal's public config API,
  avoiding any dependency on modal-internal modules.
  ([#148](https://github.com/stardag-dev/stardag/issues/148),
  [#150](https://github.com/stardag-dev/stardag/pull/150))
- Dev lockfile: bump modal 1.3.3 → 1.5.0 so CI continuously tests against a
  modal version affected by the import breakage. The supported range is
  unchanged (`modal>=1.0.0`).

## [0.8.0] — 2026-06-11

Behaviour fix to `StardagField(compat_default=...)` that can change task
IDs/hashes for fields with non-trivially-serialized types. See
[RELEASE_NOTES.md](RELEASE_NOTES.md#v080--compat_default-compares-the-raw-python-value)
for the migration guide.

### SDK

- **`stardag/base_model.py`**: `StardagField(compat_default=...)`'s hash-mode
  drop now compares the field's **raw Python value** against `compat_default`
  instead of the already-serialized value. Previously the feature silently
  no-opped for any field whose serialized form differs from its Python value —
  enums (→ `.value`), tuples (→ lists), or fields with a custom/hash-only
  serializer — so adding such a field with a compat default still changed
  existing task IDs/hashes. The comparison is now symmetric with the
  compat-validation path (which also uses the raw value): `compat_default` is
  supplied in its natural validated Python form rather than the serialized
  form. **Breaking** for the affected types — see the migration guide.
  ([#146](https://github.com/stardag-dev/stardag/issues/146),
  [#147](https://github.com/stardag-dev/stardag/pull/147))
- Documented `StardagField.compat_default` / `hash_exclude` (previously a
  `TODO` docstring).

## [0.7.3] — 2026-05-08

`stardag modal deploy` and `stardag modal stardag-api-key create` now
display the slug that actually corresponds to the resolved
workspace/environment UUID. Previously the slug was read from the active
CLI profile's TOML, which could be unrelated to the resolved UUID when
env vars or a custom `config_provider` override the IDs — producing
misleading lines pairing the resolved UUID with a slug from an unrelated
profile. No client-code changes — `pip install -U stardag` is
sufficient. See
[RELEASE_NOTES.md](RELEASE_NOTES.md#v073--correct-slug-display-in-stardag-modal-cli)
for details.

### SDK

- **`stardag/_cli/modal.py`**: replaced `_get_profile_slugs` with
  `_resolve_display_slugs`, which reverse-looks up the slug from the
  resolved UUID via the id-cache. Slug is omitted when no cache hit
  rather than guessing from the active profile.
- **`stardag/config/cache.py`**: added `get_cached_workspace_slug` and
  `get_cached_environment_slug` (UUID → slug reverse lookups).

## [0.7.2] — 2026-05-08

`sd.build(resume_build_id=...)` now fires a `BUILD_RESUMED` event so
resumed builds flip back to **running (resumed)** in the UI and jump to
the top of the Home list, instead of silently keeping their previous
terminal status. No client-code changes — `pip install -U stardag` is
sufficient. See
[RELEASE_NOTES.md](RELEASE_NOTES.md#v072--build-resume-status-fix-and-skipped-ui-polish)
for details.
([#141](https://github.com/stardag-dev/stardag/pull/141))

### SDK

- **`RegistryABC.build_resume` / `build_resume_aio`** added (default
  no-op for older registry backends). `build`, `build_aio`,
  `build_sequential`, `build_sequential_aio` call it whenever
  `resume_build_id` is set, immediately after adopting the existing
  build id. `APIRegistry` swallows the missing-route 404 from older
  servers via the existing `_is_route_not_found` pattern (warning
  logged, build runs to completion locally).

### Registry API

- **New `EventType.BUILD_RESUMED`** + **`POST
/api/v1/builds/{build_id}/resume`** endpoint, mirroring the existing
  `/complete` / `/fail` / `/cancel` shape. Status replay treats
  `BUILD_RESUMED` like `BUILD_STARTED` (flips status to `RUNNING`,
  clears `completed_at`) and exposes a derived `is_resumed: bool` flag
  on `BuildResponse` — true while the latest build-level event is
  `BUILD_RESUMED`, cleared by any subsequent terminal or
  `BUILD_STARTED` event.
- **New `Build.last_active_at` column** (Alembic migration backfills
  from `created_at`). Touched only on build-level lifecycle events
  (`BUILD_RESUMED` / `BUILD_COMPLETED` / `BUILD_FAILED` /
  `BUILD_CANCELLED` / `BUILD_EXIT_EARLY`) — task events deliberately
  skip this write to avoid row-lock contention against the build row
  under high task concurrency. `GET /builds` now sorts by
  `(last_active_at desc, id desc)` so resumed builds rise to the top
  while `Build.id` (UUID7) keeps pagination stable across timestamp
  ties.
- **`/tasks/search/values?key=status`** autocomplete returns the full
  filterable status set (was hardcoded to `pending`/`running`/
  `completed`/`failed`; now includes `suspended`/`skipped`/`cancelled`).
  `unregistered` is still excluded — it's an internal phantom-row
  marker, not a status users filter on.

### UI

- **"running (resumed)" badge** in `BuildStatusBadge` (Home list and
  build-view breadcrumb) when the API reports `is_resumed`.
- **`skipped` task status** added to `TaskStatus` (was previously
  unhandled). Renders in **amber** across `StatusBadge`, the DAG node
  border (`TaskNode`), and the Task Explorer table — was effectively
  near-invisible black-on-dark-blue before. The build-view status
  filter dropdown also gained the missing **Skipped** and
  **Cancelled** options.

### Compatibility

- **New SDK against an older Registry API** (no `/resume` route):
  degrades gracefully via `_is_route_not_found`. The build still runs
  to completion locally; the registry-side status flip is the only
  thing missing until the API is upgraded.
- **Older SDK against the new API**: unaffected. The new SDK call is
  additive, and `last_active_at` is initialised on insert by the column
  default plus bumped by the new build-level handlers, so list
  ordering is correct without SDK cooperation.

## [0.7.1] — 2026-05-05

Modal: `StardagApp.build_spawn` / `build_remote` now accept multiple root
tasks (`Sequence[BaseTask] | BaseTask`) and a new `build_kwargs` dict
forwarded to `stardag.build(...)`. **Breaking**: first parameter renamed
`task` → `tasks` for consistency with `stardag.build()` and
`Builder.__call__`; the `BuildFunction` protocol gains a 4th
`build_kwargs=None` parameter (custom implementations must accept it).
See
[RELEASE_NOTES.md](RELEASE_NOTES.md#v071--multi-root-builds-and-build_kwargs-on-stardagapp)
for the migration.
([#140](https://github.com/stardag-dev/stardag/pull/140))

## [0.7.0] — 2026-05-05

`FailMode.FAIL_FAST` now actually fails fast: in-flight sibling tasks
are cancelled (rather than silently abandoned) and tasks blocked by a
failed dependency emit `TASK_SKIPPED` rather than staying `PENDING`
forever. No client-code changes — `pip install -U stardag` is
sufficient. See
[RELEASE_NOTES.md](RELEASE_NOTES.md#v070--fail_fast-actually-fails-fast-explicit-skipped-status-for-blocked-tasks)
for details.
([#139](https://github.com/stardag-dev/stardag/pull/139))

### SDK

#### New behaviour

- **FAIL_FAST cancels in-flight siblings.** Asyncio cancel propagates
  into `modal.Function.remote.aio` and terminates the remote
  container; each cancelled task fires `TASK_CANCELLED` and releases
  any global lock it held. Previously the build re-raised in place,
  abandoning Modal calls (containers kept running and billing; registry
  left them stuck in `RUNNING`).
- **Tasks blocked by failed deps emit `TASK_SKIPPED`** (both
  `FAIL_FAST` and `CONTINUE`). A fixed-point walk after the loop emits
  per-task skip events for transitively blocked downstream work.
- **Sibling completions in the same `asyncio.wait` `done` batch as a
  failure are no longer lost** — `process_result` defers FAIL_FAST
  escalation until the batch finishes, so sibling
  `task_complete_aio`/`task_fail_aio` events still land.

#### New public API (additive; default no-op for existing implementations)

- `TaskExecutorABC.cancel(task)` — optional best-effort cancel hook.
  `RoutedTaskExecutor.cancel` routes to the matching child.
- `RegistryABC.task_skip` / `task_skip_aio`.
- `TaskCount.cancelled` and `TaskCount.skipped`; rendered by
  `BuildSummary.__repr__` when non-zero.

### Registry API

- **New `POST /api/v1/builds/{build_id}/tasks/{task_id}/skip`** —
  emits `TASK_SKIPPED`, mirroring the existing `/cancel` route.

### Compatibility

New SDK against an older Registry API (no `/skip` route): degrades
gracefully via the existing `_is_route_not_found` pattern (warning
logged, blocked tasks stay `PENDING` — pre-0.7.0 observable
behaviour). Older SDK against the new API: unaffected (additive
endpoint only).

## [0.6.1] — 2026-05-05

Patch release covering the Modal-volume integration. No breaking changes;
`pip install -U stardag` is sufficient. See
[RELEASE_NOTES.md](RELEASE_NOTES.md#v061--modal-volume-disk-cache-opt-in-and-reload-staleness-fix)
for details and the cache-config recipe.

### SDK

#### New features

- **Optional local-disk cache for `modalvol://` targets**: when a Modal
  volume is _not_ mounted locally (i.e. running outside Modal),
  `RemoteFileTarget`-backed reads and writes can now be transparently
  cached on local disk via a `CachedRemoteFileSystem` wrapper, mirroring
  the existing S3 integration. **Opt-in via
  `STARDAG_TARGET_MODALVOL_CACHE_ROOT`** — there is intentionally no
  default cache root, because Modal volume names are only unique within
  a `(workspace, environment)` pair (unlike S3's globally-unique bucket
  names) and a default would silently collide across profiles. When the
  volume _is_ mounted (running on Modal, or via
  `STARDAG_MODAL_VOLUME_MOUNTS` / the auto-mount path),
  `get_modal_target` continues to return a `ModalMountedVolumeFileTarget`
  that bypasses the RFS entirely — caching is automatically inactive on
  Modal workers.
  ([#135](https://github.com/stardag-dev/stardag/pull/135))

#### Bug fixes

- **Fix volume-reload staleness in `ModalMountedVolumeFileTarget`**:
  the previous lazy-reload path imposed a 5-second cooldown between
  reloads of the same volume to prevent thundering-herd reloads during
  discovery. As a side-effect, that cooldown could also suppress a
  reload that was _needed_ — e.g. a write committed at T+4 was
  invisible to an `exists()` check at T+4.5 if the last reload happened
  at T. Worst-case observable staleness: up to 5 seconds. The cooldown
  is replaced with per-volume singleflight coalescing (`threading.Lock`
  for sync, `asyncio.Lock` for async), and bookkeeping records the
  reload's _issue_ time (not its completion time) so a caller that
  started during another's in-flight reload correctly triggers a fresh
  reload of its own. The original thundering-herd protection during
  concurrent async discovery is preserved by the lock alone.
  ([#136](https://github.com/stardag-dev/stardag/pull/136))
- **Cross-loop safety for the async reload lock**: `asyncio.Lock`
  instances are bound to the running event loop at acquire-time. The
  cache is now keyed by `(volume_name, id(running_loop))`, so a fresh
  `asyncio.run()` gets its own lock instance instead of reusing one
  bound to a now-closed loop. ([#136](https://github.com/stardag-dev/stardag/pull/136))
- **Crash-atomic `CachedRemoteFileSystem.upload(_aio)`**: cache-write
  paths now publish via tmp-then-`replace` (mirroring the existing
  download paths), so a crash mid-`shutil.copy` can never leave a
  partial file at the final cache path. Uses `Path.replace` /
  `aiofiles.os.replace` for the atomic publish, which also lets cache
  refresh (re-uploading the same URI) work cross-platform — plain
  `rename` would fail to overwrite on Windows.
  ([#138](https://github.com/stardag-dev/stardag/pull/138))

## [0.6.0] — 2026-04-30

End-to-end overhaul of how tasks reach the registry during a build,
motivated by "tasks don't appear in the UI in the order they're
discovered, sometimes only after they finish executing." See
[RELEASE_NOTES.md](RELEASE_NOTES.md#v060) for the full story and migration
notes; the bullets below are the per-component summary.
([#133](https://github.com/stardag-dev/stardag/pull/133))

### SDK

#### New behaviour

- **Discover-time registration**: every task discovered during the build
  walk is now registered with the registry _before_ any task starts
  executing. The full DAG appears in the UI immediately rather than
  leaves-first as tasks become runnable. Applies to both `build` /
  `build_aio` and `build_sequential` / `build_sequential_aio`.
- **Post-order discovery walk**: `discover()` recurses into static deps
  first and only registers the parent once every child has registered.
  Eliminates the brief phantom-row window where the UI used to flash up
  `tid[:12]`-style placeholder names between parent and child
  registration. Same trick on the dynamic-dep path: `discover(dep)`
  runs before `task_add_dependencies(_aio)` so the dep row exists
  before the edge insert.
- **Bulk register**: build engines now collapse the discovered tasks
  into a single `task_register_bulk(_aio)` call per discover walk
  (initial + each dynamic-deps yield), chunked at 50 tasks per HTTP
  request (well under the API's 1000 hard cap so DB transactions stay
  short and request bodies stay friendly even with fat task specs).
  For large fan-out DAGs this is a dramatic reduction in HTTP
  round-trips — a 5000-task DAG goes from 5000 individual POSTs to
  100 bulk POSTs. Per-task fallback on 404 (older API deployments).
- **Gzipped request bodies on the wire**: JSON request bodies above 1KB
  are gzipped client-side before sending; bulk-register payloads with
  repeated structure compress 5–10× typically. The server's new
  `GZipRequestMiddleware` decompresses transparently — old SDKs and
  non-gzipped requests pass through unchanged.
- **`build_fail(_aio)` now emitted on discovery error**: if a task's
  `requires()` / `complete()` raises during discovery, the registry
  receives `build_fail` rather than the build being left RUNNING
  forever.

#### Breaking changes

- **`APIRegistry.task_start[_aio]` no longer auto-calls
  `task_register[_aio]`.** The contract is now "register first"
  everywhere — `/start` is a pure event endpoint. Internal callers
  (build engines, Prefect integration) are updated. External callers
  that used `task_start_aio` directly without first calling
  `task_register_aio` will now hit a 404 from `/start`. Migration:
  add the explicit `await registry.task_register_aio(build_id, task)`
  call before `task_start_aio`.
- **Sequential build registers tasks in post-order DFS** (deps before
  parents) where it previously used pre-order. Concurrent build is
  approximately post-order — siblings still interleave but every
  task's deps are guaranteed to register before it. Visible via the
  UI / registry only; no API breakage.

#### Compatibility

- **Backwards compatible with the old Registry API**: the SDK's
  `task_register_bulk(_aio)` catches the FastAPI "missing route" 404
  and falls back to per-task `task_register(_aio)`, mirroring the
  pattern from `task_add_dependencies`. Existing
  `task_register(_aio)`, `task_start(_aio)`, etc. endpoints are
  unchanged.

### API

#### New features

- **New endpoint `POST /builds/{build_id}/tasks/bulk`**: registers up
  to 1000 tasks in a single transaction, processing the array in order
  so within-batch dep references resolve to existing rows (no
  phantom-creation in `_reconcile_dependency_edges`). Deduplicates by
  `task_id`, keeping the first occurrence. Same TASK_PENDING /
  TASK_REFERENCED event semantics as the single-task endpoint.
  Optional `?id_only=true` query param returns only `{id, task_id}`
  per task instead of the full `TaskResponse` (~10× smaller
  response — the SDK passes this since it doesn't read the response).
- **`GZipRequestMiddleware`**: ASGI middleware that decompresses
  incoming `Content-Encoding: gzip` request bodies before route
  handlers parse them. Pass-through for non-gzipped requests so old
  SDK versions, direct `curl` callers, and non-bulk endpoints keep
  working unchanged. Returns 400 on malformed gzip so clients see a
  clear error rather than a downstream parse failure.
- **`is_phantom` on `TaskResponse` / `TaskWithStatusResponse`**: the
  flag has existed on the `Task` model for a while; it's now exposed
  in the response so the UI (and other consumers) can distinguish
  placeholder rows from real registered tasks.
- **`GET /builds/{id}/tasks` orders by per-build first event**:
  joins against `events` filtered to this build and orders by
  `min(events.created_at), Task.id`. Re-referenced tasks now appear
  at the position where they were _first seen in this build_, not
  where they were first ever inserted in the environment. Previously
  unordered (DB insertion order, effectively arbitrary).

#### Behaviour change

- **Phantom-creation in `_reconcile_dependency_edges` is now a safety
  hatch**: with the SDK's post-order discover walk + the bulk
  endpoint's in-array ordering, every dep `task_id` resolves to an
  existing row in normal operation. Phantom-creation only triggers
  when a build crashes mid-discover, when an out-of-band caller
  registers an edge before its upstream task, or when an older SDK is
  used. Documented inline.

### UI

- **Phantom rows hidden from the build's task table** and the "X tasks"
  counter. They still render in the DAG view (dropping nodes there
  would leave dangling edges). Reads `is_phantom` from the API's
  `TaskResponse`.

## `app/stardag-api|ui` only — 2026-04-28

> App (API + UI) changes only — no SDK release. Deployed continuously
> via `stardag-cloud`.

### API

- **Performance**: bcrypt API-key validation moved off the event loop (in-process TTL cache + `asyncio.to_thread`), explicit DB pool config, gunicorn `--preload`/`UvicornWorker` with parameterised workers and sizing. JSONB metadata columns, `(environment_id, created_at)` indices, batched dependency reconciliation. Denormalised `Task.latest_*` status columns with `SELECT … FOR UPDATE` concurrent-write protection. ([#125](https://github.com/stardag-dev/stardag/pull/125), [#126](https://github.com/stardag-dev/stardag/pull/126), [#127](https://github.com/stardag-dev/stardag/pull/127))
- **Bug fix**: `/tasks/search` no longer 500s on `filter=build_id:=:<uuid>`; malformed UUIDs now return 400. ([#128](https://github.com/stardag-dev/stardag/pull/128))
- **Stable internal JWT signing key (optional)**: when `STARDAG_API_JWT_PRIVATE_KEY_SECRET_NAME` is set at CDK synth time, the named Secrets Manager secret (containing a PEM RSA private key under `private_key`) is mounted into the container as `JWT_PRIVATE_KEY` and used by the internal token manager instead of generating an ephemeral keypair per container. Deploys and scaleouts no longer invalidate cached internal tokens. ([#129](https://github.com/stardag-dev/stardag/pull/129))

### UI

- **401 retry + session-expired overlay**: `fetchWithAuth` retries 401 once with a force-refreshed Cognito token; on unrecoverable 401 a non-dismissible modal prompts re-login instead of leaving the user on silent empty states. ([#130](https://github.com/stardag-dev/stardag/pull/130))
- **Loading-state and nav hygiene**: BuildsList no longer flashes "No builds yet" between env-arrival and the first fetch; BuildsList + TaskExplorer reset to page 1 on env change (no extra fetch with the old page); BuildView clears filters / pagination / selected task when navigating between builds; switching env from a `/builds/:id` page redirects to `/` instead of surfacing "Failed to fetch graph". ([#131](https://github.com/stardag-dev/stardag/pull/131))

## [0.5.9] — 2026-04-20

### SDK

#### New features

- **Dynamic dependency edges now reach the Registry** so they render as upstream deps in the DAG view. Previously, a task yielded from a `run()` / `run_aio()` generator only had its own static `requires()` chain recorded — the parent → yielded-dep relationship was invisible in the UI. Build executors (`build`, `build_sequential`, and their `_aio` variants) now call a new `RegistryABC.task_add_dependencies(_aio)` method at each dynamic-deps yield, passing the upstream tasks and an `is_dynamic=True` flag. ([#123](https://github.com/stardag-dev/stardag/pull/123))
- **`RegistryABC.task_add_dependencies(_aio)`**: new registry protocol method for recording dependency edges after `task_register`. Default no-op for in-memory registries (`NoOpRegistry` etc.). `APIRegistry` POSTs to the new `POST /builds/{build_id}/tasks/{task_id}/dependencies` endpoint.

#### Compatibility

- **Graceful fallback for older Registry APIs**: the SDK's `APIRegistry.task_add_dependencies(_aio)` catches the specific FastAPI "missing route" 404 (`{"detail": "Not Found"}`) and logs a warning — builds against an older API deployment continue to work, they just don't record dynamic edges. App-level 404s (e.g. `"Build not found"`, `"Task … not registered …"`) re-raise normally so genuine errors aren't hidden.

### API

#### New features

- **`is_dynamic` column on `task_dependencies`** (nullable, `server_default='false'`) — distinguishes edges discovered at runtime from those declared via `task.requires()`. Included in `TaskEdge` / `TaskEdgeExtended` response schemas. Alembic migration `94003640952d` is additive and safely reversible.
- **New endpoint `POST /builds/{build_id}/tasks/{task_id}/dependencies`**: accepts `{upstream_task_ids, is_dynamic=True}`, creates phantom upstream tasks for unknown ids, inserts edges idempotently via `ON CONFLICT DO NOTHING`, and returns `{added, total}`.
- **Grouping applies at depth=0**: `GET /builds/{id}/graph` now always routes through the grouping traversal path, so `max_per_type_per_level` is honored uniformly regardless of `upstream_depth` / `downstream_depth`. Structurally-identical tasks within a build (e.g. many chunks) collapse into batch nodes in the default in-build view.
- **`is_dynamic` propagates through group collapse**: when the extended graph collapses same-type tasks into a batch node, the resulting aggregate edge is marked `is_dynamic=True` if _any_ underlying contributor is dynamic.

#### Schema change

- The `GET /builds/{id}/graph` response is now always the extended shape (`TaskGraphExtendedResponse` — with `groups`, `truncated`, `total_upstream_count`, `total_downstream_count`). Previously it returned the basic `TaskGraphResponse` shape when both depths were 0. The UI already handled both shapes via `isExtendedResponse`; other consumers reading the basic shape should switch to the extended one (all the same fields are present, plus the extended fields default sensibly when depths are 0).

### UI

- **Dynamic dep edges render dashed** (`strokeDasharray: "6 4"`) with the same grey stroke as static deps — subtle visual distinction that stays readable in dense DAGs.
- **Hover tooltip on dynamic edges** ("Dynamic dependency — yielded at runtime from the upstream task's run() generator.") via a new `DynamicEdge` React Flow edge type with an SVG `<title>` child and a wider transparent hit-path for easy hovering.

### Examples

- New `stardag_examples.general.dynamic_deps_demo` — an `Orchestrator` that first runs `GetChunksToProcess(source_uri)` to decide how many chunks to process, then dynamically yields one `TransformChunk` per chunk; each `TransformChunk` statically requires its own `LoadChunk`. Deterministic `sha256(source_uri)`-based heuristic yields 1–6 chunks of size 1–8 per URI. Good demo of both static + dynamic deps in one pipeline.

## [0.5.8] — 2026-04-20

### SDK

#### Bug fixes

- **`build_sequential` now resolves dynamically-yielded tasks' `requires()` chain.** Previously `build_sequential` (and `build_sequential_aio`) could execute a task yielded from a `run()` generator without first building that task's own static `requires()`, causing it to fail when it tried to `load()` a dep that was never built. The concurrent `build()` already handled this correctly; the sequential executor now matches. ([#118](https://github.com/stardag-dev/stardag/issues/118), [#119](https://github.com/stardag-dev/stardag/pull/119))

#### New features

- **Async generator dynamic dependencies.** `async def run_aio(self): yield ...` is now a supported form for declaring dynamic deps on the class-based Task API — the build system detects async generators via `inspect.isasyncgenfunction` and drives them with `async for`. Both sequential and concurrent executors support it, and the Modal integration handles it via idempotent re-execution. ([#120](https://github.com/stardag-dev/stardag/pull/120))
- **Modal integration: dynamic-deps tasks can now run remotely.** `Runner.run()` now drives sync and async generators and returns a `TaskStruct` of yielded deps for idempotent re-execution (generators cannot be pickled across the Modal boundary). Async-only tasks (`run_aio` without `run`) are executed via `asyncio.run`. ([#120](https://github.com/stardag-dev/stardag/pull/120))

#### Minor breaking change

- **`@task` decorator rejects generator functions.** Declaring `@task`-decorated functions as generators (`yield`) or async generators now raises `TypeError` at decoration time, with an error message pointing to the class-based Task API. Dynamic dependencies were never well-supported by the decorator API — the class-based API is the intended path and always has been. If you relied on this (undocumented) behavior, migrate the function to a `Task` subclass with a generator `run()` / `run_aio()` method. ([#120](https://github.com/stardag-dev/stardag/pull/120))

#### Notes

- The sequential executor's handling of previously-complete dependencies surfaced at runtime (e.g. static deps of a dynamically-yielded task that happen to already be on disk) is more consistent — a `runtime_discover()` wrapper ensures they receive `task_register` + `task_complete` events so they appear in the build's task list in the Registry. Previously they could be silently excluded.
- Known limitation: `StardagApp.build_remote` does not yet support passing runtime configuration overrides to the remote build/worker containers — target roots and other config are baked into the deployed Modal app via Secrets. Tracked as [#121](https://github.com/stardag-dev/stardag/issues/121).

## [0.5.7] — 2026-04-19

### SDK

#### New features

- **Generic task classes can now be instantiated directly.** Previously, a user-defined generic task (e.g. `class MyGeneric(Task[list[T]], Generic[T]): ...`) was silently skipped during polymorphic registration because any class with unresolved `__parameters__` was excluded. With no `__type_id__` attached, the first `model_dump()` raised `AttributeError: __type_id__`. The registration filter has been narrowed to only skip parameterized generic aliases (e.g. `Task[int]`) and classes explicitly marked `__stardag_abstract__ = True`; user-defined generic tasks now get their own `__type_id__`. The internal abstract bases `Task`, `LoadableTask`, and `TargetTask` carry the marker so their current unregistered status is preserved.
- **`SubClass[T]` field annotations now accept `TypeVar`s bound to a `PolymorphicRoot` subclass.** A generic task can declare e.g. `field: SubClass[T]` where `T = TypeVar("T", bound=MyRoot)`; the schema is built using the TypeVar's bound for the generic form and re-built strictly for each parameterized form. Unbounded `TypeVar`s still raise a clear `TypeError` at schema-build time.

#### Notes

- TypeVars on a generic `Task` remain a **static-typing convenience** — runtime behavior (serializer, target selection, etc.) is fixed at class-definition time. If different type parameters need different runtime behavior, define a concrete subclass (e.g. `class MyInt(MyGeneric[int]): pass`) — concrete subclasses get their own `__type_id__` and distinct task id.

## [0.5.6] — 2026-04-19

### SDK

#### Behavior changes

- **`Polymorphic(on_generic_type_mismatch=...)` default is now `"warn"`** (was `"raise"`). Generic-type mismatches detected at validation time — including inside `SubClass[...]` annotations — now emit a `UserWarning` by default instead of raising `ValidationError`. Set `STARDAG_POLYMORPHIC_ON_GENERIC_TYPE_MISMATCH=raise` to restore the previous behavior, or `=ignore` to suppress the warning entirely. An explicit non-`None` value passed to `Polymorphic(...)` always overrides the env var. The emitted warning now includes the env-var suppression hint.

## [0.5.5] — 2026-04-09

### SDK

#### Breaking changes (Modal integration only)

- **`builder_type` removed** from `StardagApp.__init__`. Use `build_function=` instead.
- **`default_build`/`default_run` functions removed**. Replaced by `Builder` and `Runner` classes.
- **`BuildFunction` protocol signature changed**: `(tasks: Sequence[BaseTask] | BaseTask, worker_selector, app_name) -> BuildSummary`.
- **`build_remote`/`build_spawn` kwargs renamed**: `task=` → `tasks=`, `modal_app_name=` → `app_name=`.

#### New features

- **`Builder` and `Runner` classes**: Subclassable defaults for `StardagApp.build_function` and `run_function` with overridable `setup()`/`teardown()` hooks for custom container-level initialization (logging, GPU setup, config, etc.).
- **`PrefectBuilder`**: `Builder` subclass for Prefect-based build orchestration (replaces `_prefect_build` function).
- **`BuildFunction` and `RunFunction` Protocol types**: Clear contracts for custom build/run callables.
- **`stardag.testing.modal`**: Test tasks and app factory (`create_test_app()`) for Modal integration tests.

## [0.5.4] — 2026-04-08

### SDK

#### Bug fixes

- **Fix `modal >= 1.4` compatibility**: Remove import of `modal.gpu.GPU_T` which was deleted in modal 1.4. `FunctionSettings.gpu` now uses `str | list[str]` directly. (#113)

## [0.5.3] — 2026-04-05

### SDK

#### Security

- **Secret masking**: `RegistryAuth.api_key` and `RegistryAuth.access_token` now use Pydantic `SecretStr`. Values are masked as `**********` in `repr()`, `str()`, `model_dump()`, and log output, preventing accidental leakage of credentials.
- **`STARDAG_API_URL` env var**: Replaces `STARDAG_REGISTRY_URL` as the canonical env var for the registry API URL. `STARDAG_REGISTRY_URL` still works as a deprecated alias with a warning. Consistent with `STARDAG_API_KEY` and `STARDAG_API_TIMEOUT`.

#### Bug fixes

- **Token auth with env var overrides**: When `STARDAG_API_URL` is set (bypassing profile for URL/workspace/environment), the loader now inherits user and registry_name from the active profile so that OIDC token auth still works.

## [0.5.2] — 2026-04-05

### SDK

#### Breaking changes (configuration only — core task/build API unchanged)

- **`StardagConfig` restructured**: `config.api` (`APIConfig`), `config.context` (`ContextConfig`), and the loose `config.access_token`/`config.api_key` fields are replaced by `config.registry: RegistryConfig | None` and `config.context: ConfigContext`. Code using `config.api.url` must use `config.registry.url` (with null check for offline mode).
- **`APIConfig` removed**: Subsumed by `RegistryConfig` (url, timeout, workspace_id, environment_id, auth).
- **`ContextConfig` removed**: Replaced by `ConfigContext` (profile, registry_name only — user/workspace_id/environment_id moved to `RegistryConfig`).
- **`RegistryConfig` repurposed**: Was `RegistryConfig(url: str)` (TOML entry). Now `RegistryConfig(url, workspace_id, environment_id, auth, timeout)` (runtime config). TOML registry entries are now plain `dict[str, str]` in `TomlConfig`.
- **`config/__init__.py` trimmed**: Only public API symbols are exported. Internal code should import from submodules (`config.paths`, `config.cache`, `config.io`, `config.models`, `config.loader`).
- **`DEFAULT_API_URL` removed**: Unused constant.

#### New features

- **Automatic JWT token refresh during builds**: `APIRegistry` now uses `httpx.Auth` subclasses (`StardagAPIKeyAuth`, `StardagTokenAuth`) that transparently refresh expired tokens before each request. Long-running builds no longer fail when JWT tokens expire mid-execution.
- **`STARDAG_NO_REGISTRY=1` env var**: Forces offline/local mode (`config.registry = None`, `NoOpRegistry`).
- **Profile-less auth**: `StardagTokenAuth` can derive credential storage keys from the registry URL when no TOML profile is configured, enabling env-var-only setups.

#### Improvements

- `config.py` split into `config/` package with focused submodules: `paths`, `io`, `cache`, `models`, `loader`.
- Token refresh logic extracted from `_cli/credentials.py` to `registry/_auth.py`, removing code duplication and the `config → _cli` circular dependency.
- `get_user_workspaces()` and `get_environments()` now propagate exceptions instead of silently returning empty lists.

## [0.5.0] — 2026-03-18

### SDK

#### New features

- **`LoadValidator[T]`** — abstract base class for validators that run automatically on `Task._save()` and `Task.load()`. Attached via `typing.Annotated`, supports chaining, transforming, and an attribute-based escape hatch for MRO conflicts. Works with both the class API and `@task` decorator.
- **`test_harness`** context manager in `stardag.testing` — sets up isolated test environments with temp target roots and `NoOpRegistry` by default.
- **`get_default_relpath()`** — standalone public utility for constructing default task relpaths (previously internal to `Task._relpath`).
- **`BuildSummary.raise_on_failure()`** — raises new `BuildFailed` exception (with `.summary`) on `FAILURE` status.
- **`TaskExecutionError`** — wraps task executor exceptions with pre-formatted tracebacks, preserving context across thread/process boundaries.
- **`on_registry_failure` parameter** on all build functions — `"warn"` (default) or `"raise"` to control registry error handling.
- **`register_all` flag** on all build functions — opt-in full DAG registration, recursing into already-complete task dependencies.
- **Commit hash in event metadata** — all task/build lifecycle events now include the git commit hash for traceability (critical for resumed builds at different commits).

#### Improvements

- All serializers are now hashable for use in `Annotated` type params (Pydantic generic cache compatibility).
- `Annotated` wrappers are stripped in `_is_type_compatible`, fixing `TaskLoads[Annotated[T, ...]]` validation.
- `artifacts()` / `artifacts_aio()` return `Sequence` instead of `list`; fixed `artifacts_aio` missing `async` keyword.
- `Task.from_registry(id)` accepts `str | UUID` (previously `UUID` only).
- `ResourceProvider.is_initialized()` added; `_target_roots_override` no longer triggers config loading prematurely.
- Registry provider used consistently in build modules (enabling test overrides).
- Removed unnecessary generic `_FileTargetType` from `DirectoryTarget`.

#### Bug fixes

- **FAIL_FAST exception surfacing**: Task exceptions now propagate to caller in FAIL_FAST mode (both sequential and concurrent builds), instead of being silently wrapped.
- **Sequential build registry communication**: Previously-completed tasks now correctly marked complete in the registry (not left PENDING). Registry errors no longer mask original task errors.
- **Deadlock detection** added to sequential builds (matching concurrent build behavior).
- **Dynamic dependency discovery** uses `discover()` function, properly incrementing `task_count.discovered` and recursing sub-dependencies.
- **Artifact errors separated from registry errors** — artifact collection is best-effort with warn semantics, not subject to `on_registry_failure`.
- **Dynamically discovered already-complete deps** now registered immediately in sequential builds.
- Deduplicated sync/async sequential build logic via shared pure helper functions.

### Registry API

- Recursive upstream/downstream traversal on `GET /builds/{build_id}/graph` via optional `upstream_depth`, `downstream_depth`, `max_per_type_per_level`, `max_total_nodes` query params
- New `POST /tasks/graph` endpoint for cross-build DAG queries (used by Task Explorer)
- Graph traversal service with BFS, depth limiting, per-type grouping, and cycle protection
- Task status aggregation across builds for graph nodes
- Edge reconciliation on every `task_register` call (fixes missing edges across builds)
- Phantom task records for unregistered upstream dependencies (upgraded on proper registration)
- `is_phantom` column on tasks table; phantom tasks get `UNREGISTERED` status in graph responses
- Commit hash stored in `event_metadata` on all task/build lifecycle events
- `commit_hash` field in `TaskWithStatusResponse` (extracted from status-determining event)

### UI

- DAG view with configurable upstream/downstream depth controls
- Batch/group nodes for collapsed same-type dependencies (with expand on click)
- Depth-based visual fading for upstream and downstream nodes
- Task Explorer: DAG view works across multiple builds (removed single-build restriction)
- Task Explorer: refactored into focused sub-components (`TaskExplorerSearch`, `TaskExplorerTable`)
- Breadcrumb navigation system in global header
- Dashed border styling for phantom/unregistered task nodes
- Task Detail: commit hash from status-determining event; Event Log: per-event commit column
- DAG dependency node click fetches full task data via API
- Layout density improvements (compact sizing across all views)

## [0.4.0] — 2026-03-06

### SDK (breaking)

Target & serializer type hierarchy restructure. Directory target support added.
See [release notes](RELEASE_NOTES.md#v040--breaking-target--serializer-type-hierarchy-restructure) for migration guide.

### Registry API

- Task artifacts support (`POST /builds/{build_id}/tasks/{task_id}/artifacts`, `GET /tasks/{task_id}/artifacts`)
- Task metadata endpoint (`GET /tasks/{task_id}/metadata`) for `AliasTask.from_registry`
- Build graph endpoint (`GET /builds/{build_id}/graph`)

### UI

- Task Explorer with search, filtering, and column management
- Build view with DAG visualization
- Task detail panel with artifacts and events

## [0.3.0] — 2026-03-03

### SDK (breaking)

Task class hierarchy rename + `LoadableTask` + `TaskLoads` update.
See [release notes](RELEASE_NOTES.md#v030--breaking-task-class-hierarchy-rename--loadabletask--taskloads-update) for migration guide.

### Registry API

- Initial task registry service (builds, tasks, events, dependencies)
- API key and JWT authentication
- Workspace and environment management

### UI

- Initial React frontend with auth, workspace selection, build list

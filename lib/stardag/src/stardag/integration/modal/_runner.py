"""Default ``RunFunction`` implementation — what runs inside a worker container.

:class:`Runner` executes one task (sync, async, or dynamic-deps generator) and,
when an orchestrator forwarded a build id, reports that task's whole lifecycle
to the registry from inside the worker via :class:`_WorkerLifecycleReporter`.
Reporting from here rather than from the orchestrator is what makes the events
independent of the orchestrator's lifetime — and it is the only option under
reactive scheduling, where there is no resident orchestrator at all.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import threading
import time
import typing
from uuid import UUID

import modal

from stardag import BaseTask, TaskStruct, flatten_task_struct
from stardag._core.base_task import _has_custom_run, _has_custom_run_aio
from stardag.build import BuildTaskStore, discover_and_register_aio
from stardag.build._task_modules import (
    declared_task_module_patterns,
    format_uncovered_message,
    plan_pickle_elision,
    uncovered_task_classes,
)
from stardag.integration.modal._limit_keys import deployed_limit_key_selector
from stardag.integration.modal._logging import _setup_logging
from stardag.integration.modal._metadata import (
    MODAL_EXECUTOR_NAME,
    STARDAG_BUILD_ID_ENV,
    STARDAG_CLAIM_TTL_SECONDS_ENV,
    STARDAG_MODAL_APP_ID_ENV,
    STARDAG_MODAL_APP_NAME_ENV,
    STARDAG_MODAL_ENVIRONMENT_ENV,
    STARDAG_MODAL_FUNCTION_ID_ENV,
    STARDAG_MODAL_FUNCTION_NAME_ENV,
    STARDAG_MODAL_FUNCTION_TIMEOUT_ENV,
    STARDAG_MODAL_WORKSPACE_ENV,
    STARDAG_REACTIVE_ENV,
)
from stardag.integration.modal._protocols import RunFunction
from stardag.integration.modal._spawn import spawn_tick
from stardag.exceptions import ResumableInterruption
from stardag.registry._base import NoOpRegistry, registry_provider
from stardag.utils.env import temp_env_vars

_T = typing.TypeVar("_T")

# Modal's own "this input was cancelled" BaseException, imported so that a
# client that predates or renames it degrades to "we cannot recognise a
# cancellation" instead of failing every worker import.
#
# Import the name out of the submodule rather than reaching for
# ``modal.exception`` off a bare ``import modal``. The attribute form works
# on every modal release we support, but only as a side effect: ``exception``
# is not in modal's ``__all__``, and modal's package ``__getattr__`` raises
# for anything it does not export — it resolves purely because modal's own
# ``__init__`` imports the submodule early, which binds it on the package.
# Depending on that is depending on a private detail of modal's import
# graph. The from-import never consults the parent attribute at all.
try:
    from modal.exception import InputCancellation as _InputCancellationImpl

    _InputCancellation: type[BaseException] | None = _InputCancellationImpl
except ImportError:  # pragma: no cover - depends on the installed modal
    _InputCancellation = None

MODAL_INTERRUPTIONS: tuple[type[BaseException], ...] = tuple(
    e for e in (KeyboardInterrupt, _InputCancellation) if e is not None
)
"""The exceptions Modal ends an execution with when the platform, not the
task, decided — catch these to checkpoint.

``KeyboardInterrupt`` is the preemption signal (Modal reclaiming the
container); ``modal.exception.InputCancellation`` is what a function
**timeout** raises — and also what an explicit ``FunctionCall.cancel()``
raises, which is why stardag tells them apart by elapsed time rather than
by type.

Provided as a tuple so a task can catch both without importing from
``modal.exception`` itself, and **so it is easy to be specific**::

    from stardag.integration.modal import MODAL_INTERRUPTIONS

    try:
        train(...)
    except MODAL_INTERRUPTIONS:
        save_checkpoint(...)
        raise sd.ResumableInterruption("checkpointed") from None

Never substitute ``except BaseException``. Both members are
``BaseException`` subclasses precisely so an ordinary ``except Exception``
cannot swallow them — but a blanket catch sweeps up real bugs too (a
``NameError`` is a ``BaseException``), and re-raising
``ResumableInterruption`` for one of those turns a deterministic failure
into a task that resumes until its budget runs out.

Note this is Modal-specific by design and lives here rather than in the
core: it is the set *this backend* uses, and another backend would signal
differently.
"""

logger = logging.getLogger(__name__)

# How long the worker will wait for its interruption report to land before
# giving up and letting the container die. Small relative to the ~60s
# window Modal leaves between the timeout signal and SIGKILL: the report is
# one HTTP call, and the rest of the window belongs to the task's own
# checkpointing.
_INTERRUPT_REPORT_TIMEOUT_SECONDS = 10.0

# Slack when deciding whether an InputCancellation arrived *at* the
# function's declared timeout (a timeout) or before it (a cancellation).
#
# Modal delivers the timeout signal at the declared timeout to the
# millisecond, so this is not compensating for jitter in the signal. It
# covers the two clocks disagreeing, and they disagree in BOTH directions:
#
#   - ours starts later than Modal's (container startup and input
#     deserialisation are inside its window, outside ours), which
#     understates elapsed;
#   - but it is read after the task's own ``except`` block has run, and
#     that block exists to write a checkpoint, which overstates it.
#
# The second dominates in practice. Measured live against a 15s worker:
# 17.1s, 17.2s, 17.7s — a checkpoint write to a mounted volume, on top of
# a timeout that had already fired.
#
# Which is why the tolerance only has to forgive the *understating*
# direction: overstating already reads as timed out, and reporting is the
# recoverable answer anyway (see ``_classify_interruption``).
_TIMEOUT_DETECTION_SLACK_SECONDS = 5.0

# What a caught BaseException meant, and therefore what the worker does.
_PREEMPTION = "preemption"
_TIMEOUT = "timeout"
_CANCELLATION = "cancellation"


def _declared_function_timeout(env_overrides: dict[str, str] | None) -> float | None:
    """The worker function's ``timeout``, as forwarded by the orchestrator.

    Unparseable or non-positive values are dropped rather than raised on:
    this feeds a heuristic whose failure mode is "behave as before", and no
    task should die over a malformed diagnostic.
    """
    raw = (env_overrides or {}).get(
        STARDAG_MODAL_FUNCTION_TIMEOUT_ENV
    ) or os.environ.get(STARDAG_MODAL_FUNCTION_TIMEOUT_ENV)
    if not raw:
        return None
    try:
        timeout = float(raw)
    except ValueError:
        logger.warning(f"Invalid {STARDAG_MODAL_FUNCTION_TIMEOUT_ENV}: {raw!r}")
        return None
    return timeout if timeout > 0 else None


def _classify_interruption(
    exception: BaseException,
    *,
    elapsed_seconds: float,
    function_timeout_seconds: float | None,
) -> str | None:
    """What ended this execution, and therefore who recovers it.

    **The task decides whether it is resumable; the clock decides who
    resumes it.** Two questions, in that order:

    1. *Did the task ask to be resumed?* Only ``ResumableInterruption``
       says yes. An interruption the task let propagate is not a request —
       it means the task had no plan for being interrupted, so either it
       hung or the worker's timeout is too small for the work. Both want
       the same answer, and it is not "run it twenty more times": it is a
       failure under the ordinary attempt budget. There is deliberately no
       configuration overriding this, because the task already answered by
       raising or not raising.
    2. *Is anything already going to restart it?* Before the function
       timeout fires, an escaping ``BaseException`` reads to Modal as a
       crashed container and the input is restarted on the same call id in
       a few seconds — better than a scheduler respawn on every axis (the
       claim is kept, no attempt is spent, no round-trip). Once the timeout
       has fired nothing is coming (verified live: returning cleanly,
       re-raising an interrupt and raising an ordinary exception all
       resolve to ``FunctionTimeoutError`` with no restart), so the
       registry event is the only way back.

    Hence the return values:

    - ``_TIMEOUT`` — the task asked to be resumed and no restart is coming.
      The one case that reports ``TASK_INTERRUPTED``.
    - ``_PREEMPTION`` — the task asked to be resumed and the backend will
      restart it. Report nothing, get out of the way.
    - ``_CANCELLATION`` — a raw platform interruption. Report nothing: on a
      preemption the backend restarts it, and on a timeout the execution
      dies and a later scheduler pass records the failure, which is exactly
      what should happen to a task that did not plan for this.
    - ``None`` — an ordinary exception. A failure, reported as one.

    **With no declared timeout, a resumption request is reported anyway.**
    The orchestrator forwards the worker function's ``timeout`` only when
    the app declares one — but the backend applies its own default
    regardless (Modal's is 300s), so "no declared timeout" does not mean
    "no timeout fired". Guessing *preemption* there is the dangerous guess:
    if no restart is coming the task sits RUNNING until its claim lapses,
    which is the exact stall this path exists to remove. Guessing *timeout*
    is recoverable either way — if the backend does restart the input, the
    scheduler's probe finds the ref still live and leaves it alone. So the
    unknown case reports.
    """
    timed_out = function_timeout_seconds is None or (
        elapsed_seconds >= function_timeout_seconds - _TIMEOUT_DETECTION_SLACK_SECONDS
    )
    if isinstance(exception, ResumableInterruption):
        return _TIMEOUT if timed_out else _PREEMPTION
    if isinstance(exception, (KeyboardInterrupt, SystemExit)):
        return _CANCELLATION
    if _InputCancellation is not None and isinstance(exception, _InputCancellation):
        return _CANCELLATION
    return None


class _WorkerLifecycleReporter:
    """Reports a task's lifecycle events from inside a Modal worker.

    Created by :class:`Runner` when a build id was forwarded (see
    ``STARDAG_BUILD_ID_ENV``) and the container has a configured registry.
    Reporting from the worker makes the events independent of the
    orchestrator's lifetime: completion/failure land even if the build
    function died mid-await, and each (re-)invocation's TASK_STARTED
    carries its own function call id for re-attach.

    All reporting is best-effort: a registry hiccup must never fail a task
    whose actual work succeeded — failures are logged loudly and the
    engine-side self-heal (target-existence check on the next build) covers
    a lost completion event.
    """

    def __init__(
        self,
        registry: typing.Any,
        build_id: UUID,
        task: BaseTask,
        *,
        reactive: bool = False,
        app_name: str | None = None,
        executor_metadata: dict[str, typing.Any] | None = None,
        claim_ttl_seconds: int | None = None,
    ):
        self.registry = registry
        self.build_id = build_id
        self.task = task
        self.reactive = reactive
        self.app_name = app_name
        self.executor_metadata = executor_metadata
        self.claim_ttl_seconds = claim_ttl_seconds

    @classmethod
    def create(
        cls, task: BaseTask, env_overrides: dict[str, str] | None
    ) -> "_WorkerLifecycleReporter | None":
        def _get(key: str) -> str | None:
            return (env_overrides or {}).get(key) or os.environ.get(key)

        raw_build_id = _get(STARDAG_BUILD_ID_ENV)
        if not raw_build_id:
            return None
        try:
            build_id = UUID(raw_build_id)
        except ValueError:
            logger.warning(f"Invalid {STARDAG_BUILD_ID_ENV}: {raw_build_id!r}")
            return None
        registry = registry_provider.get()
        # Exact-type check: only the literal do-nothing default suppresses
        # reporting — NoOpRegistry *subclasses* may implement real behavior.
        if type(registry) is NoOpRegistry:
            return None
        app_name = _get(STARDAG_MODAL_APP_NAME_ENV)
        # Executor metadata forwarded by the orchestrator's executor (same
        # dict it records on its own starts). Values missing on older
        # orchestrators are simply omitted.
        executor_metadata: dict[str, typing.Any] = {"kind": MODAL_EXECUTOR_NAME}
        if app_name:
            executor_metadata["app_name"] = app_name
        for key, env_name in (
            ("workspace", STARDAG_MODAL_WORKSPACE_ENV),
            ("environment", STARDAG_MODAL_ENVIRONMENT_ENV),
            ("function_name", STARDAG_MODAL_FUNCTION_NAME_ENV),
            ("app_id", STARDAG_MODAL_APP_ID_ENV),
            ("function_id", STARDAG_MODAL_FUNCTION_ID_ENV),
        ):
            value = _get(env_name)
            if value:
                executor_metadata[key] = value
        # The orchestrator's derived claim TTL, if it sent one. Malformed
        # values are ignored rather than raised on: this is a bound on an
        # expiry, and no worker should fail to report its own start over it.
        raw_ttl = _get(STARDAG_CLAIM_TTL_SECONDS_ENV)
        try:
            ttl_seconds = int(raw_ttl) if raw_ttl else None
        except ValueError:
            logger.warning(f"Invalid {STARDAG_CLAIM_TTL_SECONDS_ENV}: {raw_ttl!r}")
            ttl_seconds = None
        # A syntactically valid but out-of-range value is the same problem
        # as a malformed one, and worse in effect: the server rejects it
        # (422) on `task_start`, so the worker loses its whole lifecycle
        # report over a bound on an expiry. Drop it and let the server pick
        # its default.
        if ttl_seconds is not None and ttl_seconds <= 0:
            logger.warning(
                f"Ignoring {STARDAG_CLAIM_TTL_SECONDS_ENV}={raw_ttl!r}: a claim "
                "TTL must be positive. The server's default applies instead."
            )
            ttl_seconds = None
        return cls(
            registry,
            build_id,
            task,
            reactive=_get(STARDAG_REACTIVE_ENV) == "1",
            app_name=app_name,
            executor_metadata=executor_metadata,
            claim_ttl_seconds=ttl_seconds,
        )

    def _guard(self, fn: typing.Callable[[], None], what: str) -> None:
        self._guard_value(fn, what)

    def _guard_value(self, fn: typing.Callable[[], _T], what: str) -> "_T | None":
        """``_guard`` for a call whose *answer* the caller wants.

        ``None`` on failure, which every caller must already handle as
        "the registry did not say" — a lifecycle report that raised tells
        us nothing about the state it was reporting on.
        """
        try:
            return fn()
        except Exception:
            logger.exception(
                f"Worker lifecycle report ({what}) failed for task {self.task.id}"
            )
            return None

    def started(self) -> None:
        def _do() -> None:
            ref: str | None = None
            try:
                ref = modal.current_function_call_id()
            except Exception:
                pass
            self.registry.task_start(
                self.build_id,
                self.task,
                executor=MODAL_EXECUTOR_NAME,
                executor_ref=ref,
                executor_metadata=self.executor_metadata,
                claim_ttl_seconds=self.claim_ttl_seconds,
            )

        self._guard(_do, "start")

    def completed(self) -> None:
        self._guard(
            lambda: self.registry.task_complete(self.build_id, self.task),
            "complete",
        )

        def _artifacts() -> None:
            artifacts = self.task.artifacts()
            if artifacts:
                self.registry.task_upload_artifacts(self.build_id, self.task, artifacts)

        self._guard(_artifacts, "artifacts")
        self._guard(self._wake_scheduler, "wake")

    def suspended(self, task_struct: TaskStruct | None = None) -> None:
        if self.reactive and task_struct is not None:
            # No resident orchestrator to pick up the yielded deps: register
            # them (with their requires() subtrees), persist their pickles
            # for the scheduler, and record the dynamic edges — BEFORE the
            # suspend event, so the frontier is consistent when a tick runs.
            self._guard(
                lambda: self._register_dynamic_deps(task_struct), "dynamic-deps"
            )
        self._guard(
            lambda: self.registry.task_suspend(self.build_id, self.task),
            "suspend",
        )
        self._guard(self._wake_scheduler, "wake")

    def failed(self, exception: BaseException) -> None:
        self._guard(
            lambda: self.registry.task_fail(
                self.build_id, self.task, error_message=str(exception)
            ),
            "fail",
        )
        self._guard(self._wake_scheduler, "wake")

    def interrupted(self, reason: str) -> None:
        """Report that the platform ended this execution — not a failure.

        Deliberately never ``task_fail``: a worker-recorded failure lands in
        the next frontier snapshot and, under FAIL_FAST, kills the build
        before any scheduler can retry it. A tick gets away with
        record-then-retry only because both halves happen inside one pass.

        **Bounded, because this runs in a dying container.** Modal gives
        roughly 60s between the timeout signal and the hard kill, shared
        with whatever the task did to checkpoint. A registry that hangs
        must not spend the rest of it — the whole value here is promptness,
        and the fallback (report nothing, let a later tick discover the
        dead execution) is exactly the behaviour that predates this method.
        """
        done = threading.Event()

        def _report() -> None:
            self._guard(
                lambda: self.registry.task_interrupt(
                    self.build_id, self.task, reason=reason
                ),
                "interrupt",
            )
            self._guard(self._wake_scheduler, "interrupt-wake")
            done.set()

        thread = threading.Thread(target=_report, daemon=True)
        thread.start()
        if not done.wait(_INTERRUPT_REPORT_TIMEOUT_SECONDS):
            logger.error(
                f"Reporting the interruption of task {self.task.id} did not "
                f"finish within {_INTERRUPT_REPORT_TIMEOUT_SECONDS}s; giving "
                "up on it so the container can exit. The execution claim "
                "stays held until a scheduler observes the execution is gone."
            )

    def _register_dynamic_deps(self, task_struct: TaskStruct) -> None:
        # Dynamic deps are registered with their concurrency-limit keys
        # too, so a slot release can wake the build queued on them exactly
        # as it does for tasks the bootstrap registered. The selector is
        # the deployed app's, published by the worker wrapper.
        result = asyncio.run(
            discover_and_register_aio(
                self.registry,
                self.build_id,
                task_struct,
                limit_key_selector=deployed_limit_key_selector(),
            )
        )
        store = BuildTaskStore(self.build_id)
        # The trigger's pre-flight structurally cannot see dynamically
        # yielded deps — they don't exist until their parent runs — so the
        # coverage check is re-run here, on the app's patterns as published
        # by the deployed worker wrapper. Once per class per process: this
        # runs on every suspending worker invocation.
        #
        # The same elision applies (see
        # :func:`stardag.integration.modal._bootstrap._persist_discovered_tasks`):
        # without it the pickle-free property would hold only until a task
        # yielded its first dynamic dependency, and a build with dynamic
        # deps would still need target-root write access.
        patterns = declared_task_module_patterns()
        if patterns:
            uncovered = uncovered_task_classes(
                result.incomplete.values(), patterns, only_unwarned=True
            )
            if uncovered:
                logger.warning(
                    format_uncovered_message(
                        uncovered,
                        patterns,
                        remedy=(
                            "These were registered as dynamic dependencies, "
                            "so the trigger's pre-flight could not see them."
                        ),
                    )
                )
            plan = plan_pickle_elision(result.incomplete.values(), patterns)
            store.save_tasks(task for task, _ in plan.pickled)
            logger.info(
                f"Build {self.build_id} dynamic deps of task "
                f"{self.task.id}: {plan.summary()}"
            )
        else:
            store.save_tasks(result.incomplete.values())
        deps = flatten_task_struct(task_struct)
        self.registry.task_add_dependencies(
            self.build_id, self.task, deps, is_dynamic=True
        )

    def _wake_scheduler(self) -> None:
        """Reactive wake-up: flag the build dirty, then spawn a tick — unless
        a scheduler is already live to see the flag.

        Order matters: the flag is set *before* anything else, so the
        answer that decides whether to spawn is evaluated strictly after
        the set. Combined with the tick's exit handshake (see
        ``stardag.build._reactive._run_tick_body_aio``) that makes the skip
        safe: a scheduler still holding the lease has not yet done its
        post-release re-read, so it cannot exit past this flag.

        Why skip at all: on a build whose tasks are short relative to a
        tick container's startup, every completion used to spawn a tick
        that started *after* the resident scheduler had already done the
        work, took the lease or found the build terminal, and exited
        having scheduled nothing. Seven tasks, seven cold starts, no work.

        ``scheduler_live`` unknown — an older registry that does not answer
        it, or a notify that failed outright — always spawns. That is the behaviour
        this had before the flag existed, and it is the safe direction:
        a redundant tick costs a container, a skipped one costs the build
        its progress until the watchdog.
        """
        if not self.reactive:
            return
        app_name = self.app_name
        # Tell the registry whether this caller can spawn at all: it stamps
        # the build as handed out on the assumption that the notifier will,
        # and a notifier that cannot (no app name to reach a tick with) must
        # not block the drainers that can for a whole window.
        notified = self._guard_value(
            lambda: self.registry.build_notify(
                self.build_id, can_spawn=app_name is not None
            ),
            "notify",
        )
        # ``is True``, not truthiness, and still load-bearing: the field is
        # ``bool | None``, and an older server that does not answer it at
        # all leaves it None. Only an explicit yes suppresses the spawn;
        # None — like the ``notified is None`` of a notify that raised —
        # means "unknown", and unknown spawns. The asymmetry decides it: a
        # redundant tick costs one container, a wrongly skipped one costs
        # the build its progress until the watchdog.
        # A build that is no longer RUNNING is not flagged by the notify —
        # it cannot act on a wake-up — and spawning for it would be the
        # cancelled-build loop this closes: a cancelled build's workers keep
        # running until a tick stops them, and every one of them reported
        # its way out through here. The one tick a cancel does want is not
        # lost: its own cancel sets the flag, so ``needs_tick`` stays true
        # until a tick drains it. Unknown (an older server, or a notify that
        # raised) spawns, as before.
        if notified is not None and not notified.needs_tick:
            logger.debug(
                "Build %s wants no tick (it is no longer running, or one "
                "has already been asked for); none spawned.",
                self.build_id,
            )
            return
        if notified is not None and notified.scheduler_live is True:
            logger.debug(
                "Build %s already has a live scheduler; wake-up flag set, "
                "no tick spawned.",
                self.build_id,
            )
            return
        if app_name is None:
            logger.warning(
                "Reactive build without an app name — cannot spawn a "
                "scheduler tick (relying on the watchdog)."
            )
            return

        self._guard(lambda: spawn_tick(self.build_id, app_name), "tick-spawn")


class Runner(RunFunction):
    """Default runner implementation with overridable setup/teardown.

    Override ``setup()``/``teardown()``/``run()`` to customize behavior.
    Pass an instance to ``StardagApp(run_function=MyRunner())``.

    Example:

    .. code-block:: python

        class MyRunner(Runner):
            def setup(self, task):
                super().setup(task)
                torch.cuda.set_device(0)

        stardag_app = StardagApp(
            "my-app",
            run_function=MyRunner(),
            ...
        )
    """

    def __init__(self, *, report_lifecycle: bool = True):
        """Initialize the runner.

        Args:
            report_lifecycle: Report the task's lifecycle events
                (started/completed/suspended/failed + artifacts) to the
                registry from inside the worker, when a build id was
                forwarded by the executor and the container has registry
                credentials. See :class:`_WorkerLifecycleReporter`.
        """
        self.report_lifecycle = report_lifecycle

    def setup(self, task: BaseTask) -> None:
        """Optional setup logic before the task runs.

        Per *task*, so it runs again for every input a worker container
        serves. Setup that need only happen once per container belongs in
        ``StardagApp(container_setup=...)`` instead — it runs before this.
        """
        _setup_logging()

    def teardown(self, task: BaseTask, exception: BaseException | None) -> None:
        """Optional teardown logic after the task runs.

        ``exception`` widened from ``Exception`` when the runner started
        catching ``BaseException``: a task killed by a platform interrupt
        is exactly the case where teardown matters most, and it used to
        bypass this hook entirely. Overrides typed against the narrower
        signature keep working — the value is only ever passed in.
        """
        if exception:
            logger.error(f"Task {repr(task)} raised an exception: {repr(exception)}")

    def __call__(
        self, task: BaseTask, *, env_overrides: dict[str, str] | None = None
    ) -> None | TaskStruct:
        """Core logic to execute a single task.

        Returns ``None`` when the task completed, or a ``TaskStruct`` of
        dynamic dependencies that were not yet complete (idempotent
        re-execution pattern — see ``run``).

        Args:
            task: The task instance to execute.
            env_overrides: Optional environment variable overrides (selected by
                the ``worker_selector`` — see :data:`WorkerSelection`). When
                provided, they are set temporarily around the ``run`` call and
                the previous environment is restored afterwards.
        """
        # getattr: tolerate subclasses overriding __init__ without super()
        result: None | TaskStruct = None
        exception: BaseException | None = None
        # Started here, not just before ``run()``: this is compared against
        # the *function's* timeout, and the backend's clock started earlier
        # still (container startup and input deserialisation are inside its
        # window and outside ours). Everything between here and ``run()``
        # — a user ``setup()`` that loads a model, the start report's HTTP
        # call — would otherwise be time the comparison does not see, and
        # understating elapsed time makes a real timeout read as a
        # preemption.
        started_at = time.monotonic()
        try:
            self.setup(task)
            # All lifecycle reporting happens inside the env-overrides
            # context, so overrides carrying environment-sensitive config
            # apply to reporting exactly as they do to run(). (Caveat:
            # stardag's config/registry providers cache on first access —
            # registry connection settings should come from the container's
            # process environment, i.e. deployment secrets, not overrides.)
            with temp_env_vars(env_overrides or {}):
                # getattr: tolerate subclasses overriding __init__ w/o super()
                reporter: _WorkerLifecycleReporter | None = None
                if getattr(self, "report_lifecycle", True):
                    try:
                        reporter = _WorkerLifecycleReporter.create(task, env_overrides)
                    except Exception:
                        # Best-effort contract covers creation too: a broken
                        # registry config must not fail a task before it runs.
                        logger.exception(
                            "Worker lifecycle reporter creation failed; "
                            "running without lifecycle reporting."
                        )
                if reporter is not None:
                    reporter.started()
                function_timeout = _declared_function_timeout(env_overrides)
                try:
                    result = self.run(task)
                except BaseException as e:
                    kind = self._report_end_of_attempt(
                        task,
                        e,
                        reporter=reporter,
                        elapsed_seconds=time.monotonic() - started_at,
                        function_timeout_seconds=function_timeout,
                    )
                    # _PREEMPTION is returned only for
                    # ResumableInterruption, always an ordinary Exception —
                    # the isinstance is a guard so a future classification
                    # change cannot silently start substituting a
                    # KeyboardInterrupt for some other BaseException.
                    if kind == _PREEMPTION and isinstance(e, Exception):
                        # The task raised ``sd.ResumableInterruption`` — an
                        # ordinary Exception, deliberately, so it does not
                        # slip past the user's own error handling. But an
                        # ordinary exception leaving the container is a
                        # *task failure* to the execution backend, which
                        # will not restart the input for it. A BaseException
                        # escaping reads as a crashed container, which it
                        # will. Translating here is what makes
                        # "raise sd.ResumableInterruption" mean what it says.
                        raise KeyboardInterrupt(f"Task asked to be resumed: {e}") from e
                    raise
                if reporter is not None:
                    if result is None:
                        reporter.completed()
                    else:
                        reporter.suspended(result)
        except BaseException as e:
            exception = e
            raise
        finally:
            self.teardown(task, exception)
        return result

    def _report_end_of_attempt(
        self,
        task: BaseTask,
        exception: BaseException,
        *,
        reporter: "_WorkerLifecycleReporter | None",
        elapsed_seconds: float,
        function_timeout_seconds: float | None,
    ) -> str | None:
        """Record what ended this attempt — or deliberately record nothing.

        Returns the classification, which the caller needs in order to
        choose how the exception leaves the container (see ``__call__``).

        **Only one outcome writes anything**, and the three that do not are
        the substance of the design rather than an omission:

        - **The task asked to be resumed and the backend will restart it**
          (an interruption before the timeout). The exception is on its way
          out; an escaping ``BaseException`` reads as a crashed container,
          which Modal restarts on the same call id in a few seconds. A
          terminal event here would replace that with a slower scheduler
          respawn *and* release a claim the restart is about to need.
        - **A raw platform interruption** the task did not catch. It had no
          plan for being interrupted, so the right end state is a failure
          under the ordinary attempt budget — which is exactly what happens
          if the worker says nothing and the execution dies.
        - **A deliberate cancel.** Whoever cancelled it — usually stardag
          itself on FAIL_FAST or a UI cancel — has already recorded the
          outcome. Reporting here would resurrect a task the build just
          cancelled.

        Only **"the task asked to be resumed and no restart is coming"**
        reports, because that is the only case where nothing else can
        recover the task: after a timeout fires the call is dead whatever
        the container does next.
        """
        kind = _classify_interruption(
            exception,
            elapsed_seconds=elapsed_seconds,
            function_timeout_seconds=function_timeout_seconds,
        )
        if reporter is None:
            return kind
        if kind is None:
            if isinstance(exception, Exception):
                reporter.failed(exception)
            else:
                logger.warning(
                    f"Task {task.id} ended with {type(exception).__name__}, "
                    "which is neither an ordinary failure nor a recognised "
                    "platform interruption; reporting nothing and letting "
                    "it propagate."
                )
            return kind
        if kind == _TIMEOUT:
            # Two very different situations reach here, and the message
            # must not claim the stronger one. With a declared timeout we
            # know the execution really did run out of time. Without one
            # (the worker declares no ``timeout``, so nothing was
            # forwarded) we are reporting *because* we cannot tell — see
            # ``_classify_interruption`` — and naming a timeout that was
            # never observed would be the "Nones timeout" problem in a new
            # costume: an assertion the worker is in no position to make.
            if function_timeout_seconds is not None:
                detail = (
                    f"after hitting its worker function's "
                    f"{function_timeout_seconds}s timeout "
                    f"({elapsed_seconds:.1f}s elapsed)"
                )
            else:
                detail = (
                    f"{elapsed_seconds:.1f}s in; the worker function "
                    "declares no timeout, so whether one fired is unknown "
                    "and the interruption is reported to be safe"
                )
            logger.warning(
                f"Task {task.id} checkpointed and asked to be resumed "
                f"{detail}. Reporting an interruption: nothing else will "
                "restart a timed-out input, so a scheduler tick has to."
            )
            reporter.interrupted(f"Task checkpointed and asked to be resumed {detail}")
            return kind
        if kind == _CANCELLATION:
            # A raw platform interruption the task did not catch, or a
            # deliberate cancel. Recording nothing is right for both: a
            # cancel is already owned by whoever issued it, and an uncaught
            # interruption means the task had no plan for one — so it should
            # end up a failure under the ordinary attempt budget, which is
            # what happens when the execution dies unreported.
            logger.info(
                f"Execution of task {task.id} was interrupted or cancelled "
                f"after {elapsed_seconds:.1f}s without the task asking to be "
                "resumed. Recording nothing; if this task is meant to "
                "survive interruptions, catch MODAL_INTERRUPTIONS and raise "
                "stardag.ResumableInterruption."
            )
            return kind
        logger.warning(
            f"Task {task.id} checkpointed and asked to be resumed "
            f"{elapsed_seconds:.1f}s in, before its timeout. Recording "
            "nothing and letting the interrupt propagate, so the execution "
            "backend restarts this input itself — faster than a reschedule, "
            "and it keeps the execution claim."
        )
        return kind

    def run(self, task: BaseTask) -> None | TaskStruct:
        """Default run logic — handles sync, async, and dynamic deps tasks.

        Dispatch policy:

        - **Async-only** (``run_aio`` defined, ``run`` not overridden):
          async generator ``run_aio`` is driven via ``_drive_async_generator``;
          otherwise ``asyncio.run(task.run_aio())``.
        - **Sync-only and dual** (``run`` defined, with or without ``run_aio``):
          ``task.run()`` is called. If it returns a sync generator it is
          driven via ``_drive_sync_generator``. Dual tasks intentionally
          prefer the sync path here because the Modal worker invocation is
          itself synchronous — if you need async execution for a dual task,
          implement it in ``run()`` (e.g. via ``asyncio.run`` internally).

        Generators cannot be serialized across the Modal boundary, so we
        mirror ``_run_task_in_process``: drive forward while yielded batches
        are fully complete, and at the first yield with any incomplete dep
        return the entire yielded ``TaskStruct``. The ``ModalTaskExecutor``
        builds those deps (filtering for incomplete ones) and re-invokes this
        function — on re-execution the generator advances past the
        now-complete batch.
        """
        has_run_aio = _has_custom_run_aio(task)
        has_run = _has_custom_run(task)

        if has_run_aio and not has_run:
            # Async-only task
            if inspect.isasyncgenfunction(type(task).run_aio):
                return asyncio.run(_drive_async_generator(task))
            asyncio.run(task.run_aio())
            return None

        # Sync (or dual) task — run and drive generator if returned.
        # Dual tasks deliberately take the sync path; see method docstring.
        return _drive_sync_generator(task.run())


def _drive_sync_generator(
    result: None | typing.Generator[TaskStruct, None, None] | TaskStruct,
) -> None | TaskStruct:
    """Drive a sync generator result for idempotent re-execution.

    Advances the generator past yield batches whose deps are all complete.
    Stops at the first yield with any incomplete dep and returns **all** of
    that yield's deps — including the already-complete ones, which the
    caller is expected to filter out when scheduling. If the generator
    completes, returns ``None``.

    The returned ``TaskStruct`` is the yielded one **flattened** to a tuple
    of tasks, not its original shape: a task that yields a nested structure
    (a list of lists, a dict of tasks) gets a flat tuple of the same tasks
    back. Nothing downstream needs the nesting — the build engine flattens
    to schedule — and the flat form is what survives the Modal boundary.

    If ``result`` is ``None`` (no dynamic deps) returns ``None``. If
    ``result`` is already a ``TaskStruct`` (unusual but possible when a
    user's ``run()`` returns deps directly) it is returned as-is.
    """
    if result is None:
        return None
    if not hasattr(result, "__next__"):
        # Already a TaskStruct (unusual, but handle it)
        return typing.cast(TaskStruct, result)

    gen = typing.cast(typing.Generator[TaskStruct, None, None], result)
    try:
        while True:
            yielded = next(gen)
            deps = flatten_task_struct(yielded)
            incomplete = [dep for dep in deps if not dep.complete()]
            if incomplete:
                return tuple(deps)
    except StopIteration:
        return None


async def _drive_async_generator(task: BaseTask) -> None | TaskStruct:
    """Drive an async generator ``run_aio`` for idempotent re-execution.

    Same contract as ``_drive_sync_generator``: advances past fully-complete
    yield batches and returns the first batch that contains any incomplete
    dep, flattened to a tuple of tasks and including the already-complete
    ones. Returns ``None`` when the generator finishes.
    """
    agen = typing.cast(
        typing.AsyncGenerator[TaskStruct, None],
        task.run_aio(),  # type: ignore[assignment]
    )
    # No `except StopAsyncIteration` here, unlike the sync driver's
    # `except StopIteration`: `async for` consumes the generator's
    # exhaustion itself and never propagates it, so such a handler could
    # not do the job it appears to do. The only way to reach it would be a
    # StopAsyncIteration raised by the loop *body* — `complete()`, say —
    # and swallowing that would return None, which the caller reads as
    # "task completed" and reports as such. An error must propagate
    # instead. (A generator that raises it internally already surfaces as
    # RuntimeError, so that path never reached the handler either.)
    async for yielded in agen:
        deps = flatten_task_struct(yielded)
        incomplete = [dep for dep in deps if not dep.complete()]
        if incomplete:
            return tuple(deps)
    return None


_default_run = Runner()

"""Tasks for the registry-live scenarios.

In their own module, and that is load-bearing rather than tidy: the app
declares ``task_modules=[...]`` covering exactly this module, so a scheduler
tick running in a container that has imported nothing else can still rebuild
these tasks from what the registry knows about them. If that stopped
working, a tick would have no way to reconstruct the plan and the scenarios
would fail at the first hand-off -- which is the point of exercising it.

The durations are not padding. Each one sizes a *window* some scenario needs
to exist, and the comments say which; shortening one to make a run faster is
how a scenario silently stops testing anything.
"""

from __future__ import annotations

import stardag as sd


@sd.task(name="Range")
def get_range(limit: int, salt: str) -> list[int]:
    """The chain's leaf, and the only place a run's identity enters it.

    ``salt`` does nothing to the result and everything to the *task id*.
    Task ids are derived from parameters, so a fresh salt makes this task
    and every task downstream of it new -- which is what stops a rerun
    finding the previous run's outputs already on the target root, calling
    every task complete, and passing having scheduled nothing.

    It is a parameter of the leaf rather than of each task so that the
    number of tasks in the plan is the same on every run, in a fresh
    environment or a reused one. Scenarios assert on how many tasks were
    spawned, and an assertion that depends on what a previous run happened
    to leave behind is not an assertion.
    """
    del salt
    return list(range(limit))


@sd.task(name="Square")
def square(values: sd.Depends[list[int]], offset: int) -> list[int]:
    return [(value + offset) ** 2 for value in values]


@sd.task(name="Sum")
def get_sum(integers: sd.Depends[list[int]]) -> int:
    return sum(integers)


@sd.task(name="Slow")
def slow(values: sd.Depends[list[int]], seconds: int, limit_key: str = "") -> list[int]:
    """Runs long enough to still be RUNNING when someone else asks for it.

    Two scenarios need that. The cross-build ones need a task genuinely
    in-flight rather than one that races to completion before the second
    build's first tick even fetches a frontier; and the wake-up scenario
    needs it to outlive the waiting build's tick, so that when it finishes
    there is provably no scheduler left anywhere to notice on its own.

    ``limit_key`` is **opt-in and empty by default**, and that default is
    load-bearing rather than tidy. A task carrying a concurrency-limit key
    is not merely accounted against a limiter: on every transition out of
    RUNNING the registry flags *every build in the environment* holding a
    PENDING task under the same key, whether or not a limit is configured
    for it. With a key applied unconditionally and the scenarios running
    concurrently in one environment, one scenario's ``Slow`` finishing
    would wake another scenario's build -- and a build that gets woken by
    a neighbour is a build whose own wake-up path was never tested. So a
    scenario asks for a key only when the key is what it is testing.
    """
    import time

    del limit_key  # read off the task by the app's limit-key selector
    time.sleep(seconds)
    return values


@sd.task(name="Fails")
def fails(values: sd.Depends[list[int]], seconds: int) -> list[int]:
    """Runs for ``seconds``, then fails deterministically.

    For the scenario where a shared task's status is a *result* rather than
    a revocation. The sleep is what makes the decision observable: the
    second build has to register while this task is still RUNNING, because
    RUNNING is the one status a trigger will not reset -- and a trigger
    resetting it is the (correct) behaviour that would hide the mid-flight
    decision under test.
    """
    import time

    time.sleep(seconds)
    del values
    raise RuntimeError("deliberate failure: this task exists to fail")


class SuspendingParent(sd.Task[list[int]]):
    """Registers dynamic children, yields, and so sits SUSPENDED while they run.

    The shape one scenario needs and nothing else here produces: a shared
    task whose worker registered its children, yielded and returned. It
    holds **no claim** -- so nothing but its owning build's liveness says
    whether anyone is still progressing it, and a second build that reset
    it would redo every bit of the pre-yield work for nothing while the
    first build is legitimately mid-flight.

    Note which duration bounds that window. It is the *worker* timeout, not
    the claim TTL: the TTL is derived from the timeout plus a grace, so it
    is strictly the looser of the two, and a fixture sized against it would
    be guaranteed to outlive its own execution budget.

    ``salt`` reaches the leaf, and through it this task's own id and its
    children's -- see the note on ``get_range``.
    """

    salt: str
    children: int = 2
    child_seconds: int = 60
    pre_yield_seconds: int = 20

    def requires(self):
        return get_range(limit=self.children, salt=self.salt)

    def run(self):
        import time

        indices = self.requires().load()
        # A RUNNING window before the yield, and it is load-bearing rather
        # than padding: the second build has to *register* while this task
        # is still RUNNING. Register any later and plan closure follows the
        # freshly-written dynamic edges, pulls the children into that
        # build's plan too, and it is then simply a build with running
        # tasks of its own -- never stalled, so never classifying a blocker
        # at all. Correct behaviour; just not the state under test.
        time.sleep(self.pre_yield_seconds)
        # The children hang off the leaf this task already required, which
        # is complete by the time they are registered -- so the suspended
        # window costs one level of container start rather than two.
        #
        # Their ids are made distinct by the one parameter that was going
        # to differ anyway. A task id is derived from its parameters, so
        # two children given identical ones would be a single task and the
        # fan-out would be imaginary.
        kids = [
            slow(values=self.requires(), seconds=self.child_seconds + index)
            for index in indices
        ]
        yield kids
        self._save([len(kid.load()) for kid in kids])


# Modal Dict holding the fan-out each ``GenerationalParent`` should yield,
# keyed by that scenario's salt. Created on first use and read inside the
# worker, which is the point: it is the one input a scenario can change
# *between* two builds of the same task id.
FANOUT_DICT_NAME = "registry-live-fanout"


class GenerationalParent(sd.Task[list[int]]):
    """Yields whichever dynamic children the scenario currently asks for.

    Written for the one shape a fixed parameter cannot express: the same
    task id yielding a *different* generation of children on a later
    attempt. That is what an abandoned fan-out looks like once the code has
    moved on -- the partition size changed, the task still promises the
    same output, so its id is unchanged and correctly so.

    The generation comes from a Modal Dict rather than from a parameter
    because a parameter cannot carry it. A hashed one changes the task id,
    which makes the two attempts two different tasks and the scenario
    imaginary. A ``hash_exclude=True`` one does not -- but the registry
    stores a task's ``task_data`` at first registration and never updates
    it, and this app declares ``require_pickle_free``, so every later build
    rehydrates from that first copy: the second phase would silently run at
    the first phase's width and the scenario would pass having tested
    nothing.

    Children are distinguished by their ``seconds``, which is a real
    parameter of ``slow``, so each generation is a genuinely different set
    of task ids -- as two partitionings of the same data are.
    """

    salt: str
    pre_yield_seconds: int = 10

    def requires(self):
        return get_range(limit=1, salt=self.salt)

    def run(self):
        import time

        import modal

        # A RUNNING window before the yield, so a scenario can catch this
        # task in flight if it needs to.
        time.sleep(self.pre_yield_seconds)
        generation = modal.Dict.from_name(FANOUT_DICT_NAME, create_if_missing=True)[
            self.salt
        ]
        kids = [slow(values=self.requires(), seconds=seconds) for seconds in generation]
        yield kids
        self._save([len(kid.load()) for kid in kids])


class FanIn(sd.Task[int]):
    """A single root over ``width`` independent leaves: one wide layer.

    Flat rather than a binary tree, and that is the whole design. What the
    fan-out scenario exercises is how many actionable tasks one tick pass
    can put on workers, so the quantity that matters is the width of a
    *single* layer. A tree over the same leaves would nearly double the
    task count -- and every extra task is another real Modal container --
    while making no layer any wider than this one.

    Distinct ``limit`` values give the leaves distinct ids, so these really
    are N independent tasks rather than one task referenced N times, which
    is what makes it a fan-out rather than a deduplication test.
    """

    salt: str
    width: int = 24

    def requires(self):
        return [get_range(limit=index, salt=self.salt) for index in range(self.width)]

    def run(self):
        self._save(sum(len(leaf.load()) for leaf in self.requires()))

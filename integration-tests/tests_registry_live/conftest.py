"""Session setup for the registry-live tier.

This file **deploys nothing**. The stack is brought up separately, by
``registry_live.provision``, and the scenarios are pure consumers of it.

That split is what makes the tier concurrent. Under ``pytest-xdist`` every
worker process runs its own session hooks, so a session-scoped deployment
would be built once per worker -- several registries, several databases,
and scenarios talking to whichever one their worker happened to create.
Provisioning outside pytest means each worker instead reads the same
coordinates off disk. The scenarios spend nearly all their wall clock
asleep waiting on Modal containers, so running them together costs almost
nothing and the tier's runtime stops being the sum of its parts.

It also means a developer keeps a stack between runs, which turns
iterating on one scenario from a two-minute cycle into a twenty-second
one. See ``provision``'s docstring and the DEV_README section.

Deliberately a *separate* test root from ``tests/``: that directory's
conftest brings up docker-compose and Playwright, which this tier needs
none of, and ``testpaths`` in ``pyproject.toml`` still points only at
``tests``, so a bare ``pytest`` never reaches the live tier.
"""

from __future__ import annotations

import os

import pytest

from stardag_integration_tests.registry_live._guard import ENV_API_URL, is_enabled
from stardag_integration_tests.registry_live._harness import Deployment
from stardag_integration_tests.registry_live.provision import (
    default_environment_name,
    load_coordinates,
    sdk_environment,
)

ENV_MODAL_ENVIRONMENT = "MODAL_ENVIRONMENT"

_deployment: Deployment | None = None


def pytest_configure(config: pytest.Config) -> None:
    """Point this process at the provisioned stack, before collection.

    Runs in the xdist controller *and* in every worker, which is exactly
    right: each process needs these environment variables set before it
    imports a scenario module, and reading one small file is cheap enough
    to do several times.

    Before collection matters because each scenario module calls
    ``registry_live_guard()`` at import, so a stack that is missing, or
    pointed somewhere unexpected, is a collection error rather than a
    scenario that quietly ran against the wrong registry.
    """
    global _deployment
    if not is_enabled():
        return

    modal_environment = (
        os.environ.get(ENV_MODAL_ENVIRONMENT, "").strip() or default_environment_name()
    )
    deployment = load_coordinates(modal_environment)
    if deployment is None:
        raise pytest.UsageError(
            f"No provisioned stack for Modal environment "
            f"{modal_environment!r}. This tier deploys nothing itself. "
            "Bring one up first:\n\n"
            "    python -m stardag_integration_tests.registry_live.provision "
            f"up --modal-env {modal_environment}\n\n"
            "and tear it down with `provision down` when you are finished "
            "with it."
        )

    # Direct overrides rather than a profile: profile resolution walks the
    # working directory's parents for a config file as well as reading
    # ~/.stardag, so a checkout under the developer's home finds their real
    # config -- whose default profile may be a registry other people depend
    # on. The guard asserts the resulting URL regardless.
    os.environ.update(sdk_environment(deployment))
    os.environ.pop("STARDAG_PROFILE", None)
    # What the guard compares the resolved registry against. Kept separate
    # from the SDK variables above on purpose: those *configure* the SDK,
    # this one records what the answer is supposed to be, and a check that
    # reads its expectation from the thing it is checking proves nothing.
    os.environ[ENV_API_URL] = deployment.api_url
    # Pin the Modal environment for this process too, not just for the
    # lookup above. A scenario that reaches Modal directly -- to set up
    # state its tasks read at run time -- would otherwise resolve whatever
    # the ambient profile says, and land in a different environment than
    # the one its own workers run in, which is a very quiet way to get a
    # scenario that tests nothing.
    os.environ[ENV_MODAL_ENVIRONMENT] = modal_environment
    _deployment = deployment


@pytest.fixture(autouse=True)
def _registry_survived(deployment: Deployment):
    """Check after every scenario that the database still exists.

    A finaliser rather than a line at the end of each test, and the
    difference is the whole point of the check. A recycled container takes
    the database with it, and the symptoms -- tasks reverted to
    unregistered, a build that cannot find its own plan -- fail one of the
    scenario's own assertions *first*. A trailing call therefore runs
    exactly never in the case it was written for, leaving behind a very
    convincing impression of a scheduling bug.

    Raising here on an already-failed test adds an error rather than
    replacing the failure, which is right: both are true, and the second
    explains the first.
    """
    yield
    deployment.assert_same_container()


@pytest.fixture(scope="session")
def deployment() -> Deployment:
    """The provisioned registry for this session."""
    if _deployment is None:
        pytest.fail(
            "No deployment: pytest_configure did not complete. The error "
            "that stopped it is above this line."
        )
    return _deployment

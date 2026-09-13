"""Gating for the registry-live tier.

The failure this exists to prevent is specific and quiet. With no registry
configured the SDK falls back to a ``NoOpRegistry``: builds still run,
workers still execute, scenarios still pass -- and nothing whatsoever has
been checked about the registry, which is the entire subject of the tier.
A tier that can pass while testing nothing is worse than no tier, because
it is read as coverage.

So the guard asserts the two things a green run has to mean, and asserts
them at module import, before any scenario runs:

- the resolved registry is a real ``APIRegistry``, not any ``NoOp`` flavour;
- that registry answers an authenticated request over the network.

**On or off, with no ``auto`` in between**, unlike ``STARDAG_MODAL_LIVE_TESTS``.
That switch has three states because it can cheaply ask "are there
credentials?" and skip politely when there are not. Here there is no such
question: the registry does not exist until this tier deploys one, so
"detect and decide" would mean building the deployment in order to find out
whether to build it. Enabled means every problem is a failure; unset means
the modules skip -- as does living outside the default ``testpaths``, so a
bare ``pytest`` never reaches this tier either way.

The Modal half of the gating is not reimplemented here.
``stardag.testing.modal.live_modal_guard`` already owns it -- credentials,
require mode, and the workspace assertion that checks the workspace the
*token* resolves to rather than a self-asserted profile name. This module
calls it.
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Callable, NoReturn

if TYPE_CHECKING:
    from stardag.registry import APIRegistry

# Either fails or skips -- both raise, which is what lets the callers below
# treat a refusal as the end of the line rather than testing for it.
Refuse = Callable[[str], NoReturn]

ENV_ENABLED = "STARDAG_REGISTRY_LIVE_TESTS"
ENV_API_URL = "STARDAG_REGISTRY_LIVE_API_URL"

_TRUE = ("1", "true", "yes", "require")


def is_enabled() -> bool:
    """Whether this tier should run at all.

    Read by the session hook that deploys as well as by the guard below, so
    the decision to spend Modal compute is made in exactly one place.
    """
    return os.environ.get(ENV_ENABLED, "").strip().lower() in _TRUE


def registry_live_guard() -> str:
    """Assert a real, reachable registry is configured. Returns its URL.

    Called at module import in every scenario module, so a misconfiguration
    is a collection error rather than a scenario that ran against nothing.
    """
    import pytest

    if not is_enabled():
        pytest.skip(
            f"{ENV_ENABLED} is not set. This tier deploys a registry and a "
            "DAG app onto Modal, so it never runs by default.",
            allow_module_level=True,
        )

    def refuse(message: str) -> NoReturn:
        pytest.fail(message)

    api_url = os.environ.get(ENV_API_URL, "").strip()
    if not api_url:
        refuse(
            f"{ENV_API_URL} is not set, so no registry was deployed for this "
            "session -- the failure will be in the session-start hook above. "
            "This tier cannot fall back to a local or in-process registry: a "
            "registry a Modal container cannot reach is the one thing it "
            "exists to cover."
        )

    registry = _assert_registry_is_real(refuse)
    _assert_registry_points_at(registry, api_url, refuse)
    _assert_registry_answers(registry, api_url, refuse)
    return api_url


def _assert_registry_is_real(refuse: Refuse) -> "APIRegistry":
    """The configured registry must be an ``APIRegistry``.

    Checked by *type* and not by behaviour, deliberately. A ``NoOpRegistry``
    accepts every call and returns plausible emptiness, so there is no
    request whose response distinguishes it from a registry that is merely
    idle. The type is the only honest test.
    """
    from stardag.registry import APIRegistry, NoOpRegistry, registry_provider

    registry = registry_provider.get()
    if isinstance(registry, NoOpRegistry):
        refuse(
            f"The configured registry is {type(registry).__name__}, a NoOp "
            "registry: it would accept every call in these scenarios and "
            "report nothing, and they would pass. This tier requires an "
            "APIRegistry pointed at a deployed API."
        )
    if not isinstance(registry, APIRegistry):
        refuse(
            f"The configured registry is {type(registry).__name__}, not an "
            "APIRegistry. These scenarios assert against the deployed API's "
            "own behaviour -- claim arbitration in a real transaction, wake "
            "candidates flagged on a real status write -- so a stand-in "
            "cannot stand in."
        )
    return registry


def _assert_registry_points_at(
    registry: "APIRegistry", api_url: str, refuse: Refuse
) -> None:
    """It must be *this session's* registry, not merely some registry.

    Added after the check below caught the SDK resolved to the production
    registry on a developer machine. The type check above passes happily in
    that case -- production is a perfectly real ``APIRegistry`` -- and the
    round-trip check would too, given valid credentials. What stops it is
    asserting the URL.

    Worth stating plainly, because it is the sharp edge of this whole tier:
    these scenarios trigger builds, race claims and cancel things. Pointed
    at a registry someone depends on, they are not a test.

    What this does *not* do, despite an earlier comment here claiming
    otherwise: cross-check two independent sources. Both the expectation
    and the SDK's configuration are set from the same provisioned
    ``Deployment``, so they cannot disagree about which stack was
    provisioned. What it catches is the SDK ignoring that configuration and
    resolving somewhere else entirely -- from an ambient profile, or a
    config file found by walking the working directory's parents. That is
    narrower than "two sources agree", and it is the failure that actually
    happened.
    """
    expected = api_url.rstrip("/")
    actual = (registry.api_url or "").rstrip("/")
    if actual != expected:
        refuse(
            f"The configured registry points at {actual!r}, not at the "
            f"deployment created for this session ({expected!r}). These "
            "scenarios trigger builds and race claims; they must never run "
            "against a registry that outlives the session. Configure the SDK "
            "from STARDAG_API_URL / STARDAG_WORKSPACE_ID / "
            "STARDAG_ENVIRONMENT_ID / STARDAG_API_KEY -- note that moving "
            "HOME is not sufficient, because config discovery also walks the "
            "working directory's parents."
        )


# How many times the reachability round-trip is attempted, and how long to
# back off between them. Sized for a Modal cold start rather than for a
# broken deployment: a stack that is genuinely down fails all of them in a
# few seconds, which is still a fast, clear failure.
_REACHABILITY_ATTEMPTS = 4
_REACHABILITY_BACKOFF_SECONDS = 3.0


def _assert_registry_answers(
    registry: "APIRegistry", api_url: str, refuse: Refuse
) -> None:
    """One authenticated round-trip, so credentials are proven, not assumed.

    Made through the registry's *own* client, not a fresh httpx call: that
    is the code path every scenario will use, complete with its auth, its
    retry transport and its environment scoping, so this proves the thing
    that is about to be relied on rather than something adjacent.

    ``/health`` alone would not do -- it answers before any credential is
    looked at, so a revoked or absent API key sails past it and fails later
    inside a scenario, where it reads as a scheduling problem.
    """
    import httpx

    if not registry.environment_id:
        refuse(
            "The configured APIRegistry has no environment_id, so its "
            "requests are not scoped to the deployed environment and no "
            "meaningful round-trip can be made."
        )

    # Retried, because the expected first state of a freshly provisioned
    # stack is a *cold* container: the registry scales to zero, twelve xdist
    # workers each run this guard at import, and the one that arrives first
    # pays the cold start. A single attempt turns that into a collection
    # error for the whole tier — and the message it produces ("the deployed
    # registry is unreachable") sends the reader to the deployment, which is
    # fine. Seen twice in CI before this was added.
    response = None
    for attempt in range(_REACHABILITY_ATTEMPTS):
        try:
            response = registry.client.get(
                f"{registry.api_url}/api/v1/builds",
                params={"environment_id": registry.environment_id, "limit": 1},
            )
            break
        except httpx.HTTPError as error:
            if attempt == _REACHABILITY_ATTEMPTS - 1:
                refuse(
                    f"The deployed registry at {api_url} is unreachable after "
                    f"{_REACHABILITY_ATTEMPTS} attempts: {error!r}"
                )
            time.sleep(_REACHABILITY_BACKOFF_SECONDS * (attempt + 1))
    assert response is not None  # refuse() above never returns

    if response.status_code != 200:
        # Name the credential and the scope, because "401" on its own sends
        # people looking at the deployment when the answer is almost always
        # which key was resolved, or which environment it was minted for.
        env_key = os.environ.get("STARDAG_API_KEY", "")
        refuse(
            f"An authenticated request to the deployed registry at {api_url} "
            f"returned {response.status_code}: {response.text[:200]!r}. The "
            "deployment is up, but this tier's credentials do not work "
            "against it.\n"
            f"  registry.api_url        = {registry.api_url}\n"
            f"  registry.environment_id = {registry.environment_id}\n"
            f"  auth                    = {type(registry._auth).__name__}\n"
            f"  STARDAG_API_KEY prefix  = {env_key[:12]!r} "
            f"(len {len(env_key)})"
        )

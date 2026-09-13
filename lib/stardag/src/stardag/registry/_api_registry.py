"""API-based registry that communicates with the stardag-api service."""

import asyncio
import gzip
import json as _json
import logging
import time
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Mapping
from urllib.parse import quote
from uuid import UUID

import httpx
from httpx_retries import Retry, RetryTransport

from stardag.config import DEFAULT_API_TIMEOUT, config_provider
from stardag.registry._auth import StardagAPIKeyAuth, StardagTokenAuth
from stardag.registry._http_client import (
    SDK_CLIENT_HEADERS,
    sdk_version_unsupported_from_detail,
)
from stardag.exceptions import (
    APIError,
    AuthorizationError,
    EnvironmentAccessError,
    InvalidAPIKeyError,
    InvalidTokenError,
    NotAuthenticatedError,
    NotFoundError,
    QuotaExceededError,
    RateLimitError,
    TokenExpiredError,
    is_missing_route_error,
)
from stardag.registry._base import (
    SchedulerLeaseResult,
    StartClaimResult,
    BuildCancelResult,
    BuildExecutions,
    BuildFrontier,
    BuildInfo,
    BuildListPage,
    BuildNotifyResult,
    WakeCandidate,
    BuildSummary,
    BulkCancelResult,
    RegisteredTaskInfo,
    RegistryABC,
    TaskListPage,
    TaskMetadata,
    TickSummaryRecord,
    get_git_commit_hash,
)
from stardag.artifact import Artifact

if TYPE_CHECKING:
    from stardag._core.base_task import BaseTask

logger = logging.getLogger(__name__)


# Latched once per process when the registry answers ``GET
# /builds/{id}/notify`` with a missing-route error: an older server, where
# the linger poll goes back to reading the flag off the frontier. Latched
# rather than re-probed because the poll runs every few seconds for every
# lingering build, and a doomed request per poll is the exact cost the
# endpoint was added to remove. (Same shape as ``_wakeups``' route latch.)
_notify_read_route_missing = False


# Connection-pool limits for the async client. Sized for a *process*, not
# for one caller, which is the thing that changed: the client is cached per
# event loop on a registry that is itself a process-wide singleton, so
# whatever shares a loop shares this pool. A Modal container now serves up
# to ``_TICK_CONCURRENCY`` scheduler ticks on one loop, each of which may
# have ``TickConfig.max_concurrent_actions`` registry calls in flight.
#
# The old values — 10 and 5 — were a per-tick budget by accident, because a
# tick used to own its container. Leaving them would have quietly divided
# that budget by the number of co-resident ticks, i.e. turned a packing win
# into a latency regression on wide frontiers. These restore roughly the
# per-tick concurrency a tick had when it ran alone.
#
# ``keepalive_expiry`` is deliberately left where it was; it guards against
# connections going stale behind a load balancer, and that has nothing to
# do with how many callers share the pool.
_ASYNC_MAX_CONNECTIONS = 100
_ASYNC_MAX_KEEPALIVE_CONNECTIONS = 50


def _latch_notify_read_missing(error: Exception) -> bool:
    """Whether ``error`` says the server predates ``GET .../notify``.

    Two shapes, because the path itself exists on such a server for POST and
    DELETE: a router with no GET on it answers **405**, while a server
    predating the notify endpoints entirely answers the missing-route 404.
    """
    global _notify_read_route_missing
    unsupported = (
        is_missing_route_error(error)
        if isinstance(error, NotFoundError)
        else isinstance(error, APIError) and error.status_code == 405
    )
    if unsupported:
        _notify_read_route_missing = True
        logger.debug(
            "Registry API has no GET /builds/{id}/notify; the linger poll "
            "reads the wake-up flag off the frontier in this process. "
            "Upgrade stardag-api to make that poll one row."
        )
    return unsupported


# Retry configuration for transient errors (connection issues, timeouts, etc.)
# Retries on: TimeoutException, NetworkError (includes ReadError), RemoteProtocolError
_RETRY_CONFIG = Retry(
    total=3,
    backoff_factor=0.5,
    # Also retry POST since our API calls are idempotent (task state transitions)
    allowed_methods=["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "TRACE"],
)

# Rate limit retry configuration
_MAX_RATE_LIMIT_RETRIES = 5
_MAX_RETRY_WAIT = 60  # Cap wait time at 60 seconds

# Hard cap on tasks per ``task_register_bulk[_aio]`` HTTP call. Mirrors
# the server's per-call cap.
#
# **Asymmetry intentional**: the build engine's
# ``_BULK_REGISTER_CHUNK_SIZE`` (50) is the *working* size — small
# enough to keep DB transactions short and request bodies friendly.
# The 1000 here is the *defensive* ceiling for direct external callers
# of ``APIRegistry`` who haven't been told about the lower working
# size. Don't reduce this thinking 50 is the real limit — it isn't.
_MAX_BULK_REGISTER_TASKS = 1000

# Threshold above which JSON request bodies are gzipped before sending.
# Below this, gzip's headers + per-call CPU make compression a net loss;
# above it, repeated keys / structure in our payloads (especially
# ``task_register_bulk``) compress 5–10×, so the wire savings dominate.
# The server transparently decompresses via ``GZipRequestMiddleware``.
_GZIP_REQUEST_THRESHOLD_BYTES = 1024


def _maybe_gzip_json_body(
    body: object,
) -> tuple[bytes | None, dict[str, str]]:
    """Serialize a JSON body and gzip it when worthwhile.

    Returns a ``(content_bytes, extra_headers)`` pair suitable for httpx's
    ``content=`` + ``headers=`` kwargs:

    - ``body is None`` -> ``(None, {})`` (caller skips the JSON body
      entirely).
    - body small (< ``_GZIP_REQUEST_THRESHOLD_BYTES``) -> raw JSON bytes
      + ``Content-Type: application/json``.
    - body big -> gzipped JSON bytes + ``Content-Type`` and
      ``Content-Encoding: gzip``.

    Always serialises with ``separators=(",", ":")`` so the size
    threshold doesn't depend on whitespace.
    """
    if body is None:
        return None, {}
    encoded = _json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if len(encoded) < _GZIP_REQUEST_THRESHOLD_BYTES:
        return encoded, headers
    headers["Content-Encoding"] = "gzip"
    return gzip.compress(encoded), headers


# Query params as accepted by the request helpers. A mapping covers every
# call but one: ``GET /tasks?status=`` is *repeatable*, and a dict cannot
# express two values for one key — hence the sequence-of-pairs alternative,
# which httpx encodes as repeated params.
_QueryParams = dict[str, str] | Sequence[tuple[str, str]]


def _parse_bulk_register_response(payload: object) -> "list[RegisteredTaskInfo] | None":
    """Parse the ``/tasks/bulk?id_only=true`` response into RegisteredTaskInfo.

    Tolerates older servers whose slim response carries only
    ``{id, task_id}`` — the execution-state fields then default to None and
    the build engine simply has nothing to re-attach to.
    """
    if not isinstance(payload, dict):
        return None
    items = payload.get("tasks")
    if not isinstance(items, list):
        return None
    infos: list[RegisteredTaskInfo] = []
    for item in items:
        if not isinstance(item, dict) or "task_id" not in item:
            return None
        infos.append(
            RegisteredTaskInfo(
                task_id=item["task_id"],
                latest_status=item.get("latest_status"),
                latest_executor=item.get("latest_executor"),
                latest_executor_ref=item.get("latest_executor_ref"),
                latest_executor_metadata=item.get("latest_executor_metadata"),
            )
        )
    return infos


class APIRegistry(RegistryABC):
    """Registry that stores task information via the stardag-api REST service.

    This registry is stateless with respect to build_id - the build_id is passed
    explicitly to all methods that need it. This allows a single registry instance
    to be reused across multiple builds (via registry_provider).

    Usage:
        build_id = await registry.build_start_aio(root_tasks=tasks)
        await registry.task_register_aio(build_id, task)
        await registry.task_start_aio(build_id, task)
        # ... execute task ...
        await registry.task_complete_aio(build_id, task)
        await registry.build_complete_aio(build_id)

    Authentication:
    - API key can be provided directly or via STARDAG_API_KEY env var
    - JWT token from browser login (stored in registry credentials)

    Configuration is loaded from the central config module (stardag.config).
    """

    def __init__(
        self,
        api_url: str | None = None,
        timeout: float | None = None,
        environment_id: str | None = None,
        api_key: str | None = None,
    ):
        # Load central config
        config = config_provider.get()
        reg = config.registry

        # Resolve API URL: explicit > config
        resolved_url = api_url or (reg.url if reg else None)
        if not resolved_url:
            raise ValueError(
                "APIRegistry requires a registry URL. "
                "Set STARDAG_API_URL or configure a profile with a registry."
            )
        self.api_url = resolved_url.rstrip("/")

        # Latched when this registry answers a scheduler-lease call with a
        # missing-route error. Latched rather than re-probed because a tick
        # asks on acquire, on every renew while it lingers, and on release,
        # so a doomed request each time would be the whole cost of the
        # feature for deployments that do not have it.
        #
        # Per instance, not per process: two registries in one process may
        # point at different servers, and a global would let one of them
        # decide the other has no lease routes. It also keeps the flag out
        # of test isolation.
        self._scheduler_lease_route_missing = False

        # Timeout: explicit > config
        self.timeout = (
            timeout
            if timeout is not None
            else (reg.timeout if reg else DEFAULT_API_TIMEOUT)
        )

        # Environment ID: explicit > config
        self.environment_id = environment_id or (reg.environment_id if reg else None)

        # Build auth object
        resolved_api_key = api_key or (
            reg.auth.api_key.get_secret_value() if reg and reg.auth.api_key else None
        )
        if resolved_api_key:
            self._auth: httpx.Auth = StardagAPIKeyAuth(resolved_api_key)
            logger.debug("APIRegistry initialized with API key authentication")
        elif reg and reg.auth.access_token:
            # registry_name is optional — StardagTokenAuth can derive a
            # credential key from the URL when no profile is configured.
            self._auth = StardagTokenAuth(
                access_token=reg.auth.access_token.get_secret_value(),
                workspace_id=reg.workspace_id,
                user_email=reg.auth.user_email,
                registry_url=reg.url,
                registry_name=config.context.registry_name,
            )
            if not self.environment_id:
                logger.warning(
                    "APIRegistry: JWT auth requires environment_id. "
                    "Run 'stardag config set environment <id>' to set it."
                )
            else:
                logger.debug(
                    "APIRegistry initialized with browser login (JWT) authentication"
                )
        else:
            # No auth - pass None; httpx handles this gracefully
            self._auth = None  # type: ignore[assignment]
            logger.warning(
                "APIRegistry initialized without authentication. "
                "Run 'stardag auth login' or set STARDAG_API_KEY env var."
            )

        self._client = None
        self._async_client = None
        self._async_client_loop = (
            None  # Track which event loop the async client belongs to
        )

    def _handle_response_error(self, response, operation: str = "API call") -> None:
        """Check response for errors and raise appropriate exceptions.

        Args:
            response: httpx Response object
            operation: Description of the operation for error messages

        Raises:
            TokenExpiredError: If token has expired
            InvalidTokenError: If token is invalid
            InvalidAPIKeyError: If API key is invalid
            NotAuthenticatedError: If no auth provided
            EnvironmentAccessError: If environment access denied
            AuthorizationError: If other 403 error
            NotFoundError: If resource not found
            SDKVersionUnsupportedError: If this SDK is older than the
                server's configured minimum (426)
            RateLimitError: If per-minute rate limit exceeded (retryable)
            QuotaExceededError: If 24h quota exceeded (not retryable)
            APIError: For other HTTP errors
        """
        if response.status_code < 400:
            return  # No error

        # Try to extract detail from response JSON
        detail = None
        error_code = None
        raw_detail = None
        try:
            data = response.json()
            raw_detail = data.get("detail")
            if isinstance(raw_detail, dict):
                error_code = raw_detail.get("error_code")
                detail = raw_detail.get("message", str(raw_detail))
            else:
                detail = raw_detail if raw_detail else str(data)
        except Exception:
            detail = response.text[:200] if response.text else None

        status_code = response.status_code

        if status_code == 401:
            # Authentication error - determine specific type
            detail_lower = (detail or "").lower()
            if "expired" in detail_lower:
                raise TokenExpiredError(detail)
            elif "api key" in detail_lower:
                raise InvalidAPIKeyError(detail)
            elif "not authenticated" in detail_lower or not detail:
                raise NotAuthenticatedError(detail)
            else:
                raise InvalidTokenError(detail)

        elif status_code == 403:
            # Authorization error
            detail_lower = (detail or "").lower()
            if "environment" in detail_lower:
                raise EnvironmentAccessError(
                    environment_id=self.environment_id, detail=detail
                )
            else:
                raise AuthorizationError(f"{operation} access denied", detail=detail)

        elif status_code == 404:
            raise NotFoundError(f"{operation}: resource not found", detail=detail)

        elif status_code == 426:
            # The server compared the ``X-Stardag-SDK-Version`` we send
            # against its configured minimum and rejected us. Its message
            # names both versions and the upgrade command — surface it,
            # don't restate it.
            # `raw_detail` is None when the body was not JSON — a
            # proxy-generated 426 is the realistic case, and it is exactly
            # when the upstream's own words matter most. Fall back to the
            # text detail rather than losing it to the generic message.
            raise sdk_version_unsupported_from_detail(
                raw_detail if raw_detail is not None else detail
            )

        elif status_code == 429:
            if error_code == "RATE_LIMIT":
                retry_after = int(response.headers.get("Retry-After", 1))
                raise RateLimitError(retry_after=retry_after, detail=detail)
            else:
                raise QuotaExceededError(detail=detail)

        else:
            raise APIError(
                f"{operation} failed",
                status_code=status_code,
                detail=detail,
                payload=raw_detail if isinstance(raw_detail, dict) else None,
            )

    @property
    def client(self):
        if self._client is None:
            transport = RetryTransport(retry=_RETRY_CONFIG)
            # Client-level headers, not per-call: every request the registry
            # ever makes must carry the SDK version, and a default on the
            # client is the one place that cannot be forgotten by a new call
            # site. httpx merges these under any per-request headers, so the
            # gzip/content-type headers on POST bodies still win.
            self._client = httpx.Client(
                timeout=self.timeout,
                auth=self._auth,
                transport=transport,
                headers=SDK_CLIENT_HEADERS,
            )
        return self._client

    def _get_params(self) -> dict[str, str]:
        """Get query params for API requests.

        When using JWT auth, environment_id must be passed as a query param.
        """
        if isinstance(self._auth, StardagTokenAuth) and self.environment_id:
            return {"environment_id": self.environment_id}
        return {}

    # -------------------------------------------------------------------------
    # Request helpers with rate-limit retry
    # -------------------------------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        *,
        json: object = None,
        params: _QueryParams | None = None,
        operation: str = "API call",
    ) -> httpx.Response:
        """Make a sync HTTP request with automatic rate-limit retry.

        JSON bodies above ``_GZIP_REQUEST_THRESHOLD_BYTES`` are gzipped
        before sending; the server's ``GZipRequestMiddleware`` handles
        the decode transparently.

        Rate limit 429s (RATE_LIMIT error_code) are retried with backoff
        respecting the Retry-After header. Quota 429s (24h limits) are
        raised immediately as QuotaExceededError.
        """
        content, body_headers = _maybe_gzip_json_body(json)
        request_kwargs: dict[str, Any] = {"params": params}
        if content is not None:
            request_kwargs["content"] = content
            request_kwargs["headers"] = body_headers

        for attempt in range(_MAX_RATE_LIMIT_RETRIES + 1):
            response = self.client.request(method, url, **request_kwargs)
            try:
                self._handle_response_error(response, operation)
                return response
            except RateLimitError as e:
                if attempt >= _MAX_RATE_LIMIT_RETRIES:
                    logger.error(
                        "Rate limit retry budget exhausted for %s after %d attempts",
                        operation,
                        attempt + 1,
                    )
                    raise
                wait = min(e.retry_after, _MAX_RETRY_WAIT)
                logger.warning(
                    "Rate limited on %s (attempt %d/%d), retrying in %ds...",
                    operation,
                    attempt + 1,
                    _MAX_RATE_LIMIT_RETRIES,
                    wait,
                )
                time.sleep(wait)
        # Unreachable, but satisfies type checker
        return response  # type: ignore[possibly-undefined]

    async def _arequest(
        self,
        method: str,
        url: str,
        *,
        json: object = None,
        params: _QueryParams | None = None,
        operation: str = "API call",
    ) -> httpx.Response:
        """Make an async HTTP request with automatic rate-limit retry.

        JSON bodies above ``_GZIP_REQUEST_THRESHOLD_BYTES`` are gzipped
        before sending; the server's ``GZipRequestMiddleware`` handles
        the decode transparently.

        Rate limit 429s (RATE_LIMIT error_code) are retried with backoff
        respecting the Retry-After header. Quota 429s (24h limits) are
        raised immediately as QuotaExceededError.
        """
        content, body_headers = _maybe_gzip_json_body(json)
        request_kwargs: dict[str, Any] = {"params": params}
        if content is not None:
            request_kwargs["content"] = content
            request_kwargs["headers"] = body_headers

        for attempt in range(_MAX_RATE_LIMIT_RETRIES + 1):
            response = await self.async_client.request(method, url, **request_kwargs)
            try:
                self._handle_response_error(response, operation)
                return response
            except RateLimitError as e:
                if attempt >= _MAX_RATE_LIMIT_RETRIES:
                    logger.error(
                        "Rate limit retry budget exhausted for %s after %d attempts",
                        operation,
                        attempt + 1,
                    )
                    raise
                wait = min(e.retry_after, _MAX_RETRY_WAIT)
                logger.warning(
                    "Rate limited on %s (attempt %d/%d), retrying in %ds...",
                    operation,
                    attempt + 1,
                    _MAX_RATE_LIMIT_RETRIES,
                    wait,
                )
                await asyncio.sleep(wait)
        # Unreachable, but satisfies type checker
        return response  # type: ignore[possibly-undefined]

    # -------------------------------------------------------------------------
    # Sync build methods
    # -------------------------------------------------------------------------

    def build_start(
        self,
        root_tasks: list["BaseTask"] | None = None,
        description: str | None = None,
        executor_metadata: dict[str, Any] | None = None,
    ) -> UUID:
        """Start a new build and return its ID."""
        build_data: dict[str, Any] = {
            "commit_hash": get_git_commit_hash(),
            "root_task_ids": [str(task.id) for task in (root_tasks or [])],
            "description": description,
        }
        if executor_metadata is not None:
            # Only included when present — older servers ignore unknown
            # body fields anyway, but this keeps the payload minimal.
            build_data["executor_metadata"] = executor_metadata

        response = self._request(
            "POST",
            f"{self.api_url}/api/v1/builds",
            json=build_data,
            params=self._get_params(),
            operation="Start build",
        )
        data = response.json()
        build_id = UUID(data["id"])
        logger.info(f"Started build: {data['name']} (ID: {build_id})")
        return build_id

    def build_resume(
        self,
        build_id: UUID,
        executor_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Mark an existing build as resumed.

        Emits a BUILD_RESUMED event server-side so a build that previously
        terminated (FAILED / COMPLETED / CANCELLED / EXIT_EARLY) flips
        back to RUNNING. The endpoint is new in the post-resume API; on
        older servers the request 404s with FastAPI's missing-route body,
        which we swallow with a warning so the SDK keeps working against
        an un-upgraded registry. Resource-level 404s (build does not
        exist) are re-raised.
        """
        params = self._get_event_params()
        if executor_metadata is not None:
            params["executor_metadata"] = _json.dumps(
                executor_metadata, separators=(",", ":")
            )
        try:
            self._request(
                "POST",
                f"{self.api_url}/api/v1/builds/{build_id}/resume",
                params=params,
                operation="Resume build",
            )
        except NotFoundError as e:
            if not is_missing_route_error(e):
                raise
            logger.warning(
                "Registry API does not support POST /builds/%s/resume; "
                "the resumed build will keep its previous status in the "
                "registry until you upgrade the API. Build will still run "
                "to completion locally.",
                build_id,
            )
            return
        logger.info(f"Resumed build: {build_id}")

    def build_complete(self, build_id: UUID) -> None:
        """Mark a build as completed."""
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/complete",
            params=self._get_event_params(),
            operation="Complete build",
        )
        logger.info(f"Completed build: {build_id}")

    def build_fail(self, build_id: UUID, error_message: str | None = None) -> None:
        """Mark a build as failed."""
        params = self._get_event_params()
        if error_message:
            params["error_message"] = error_message
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/fail",
            params=params,
            operation="Fail build",
        )
        logger.info(f"Marked build as failed: {build_id}")

    def build_cancel(
        self, build_id: UUID, *, cascade: bool = False
    ) -> BuildCancelResult | None:
        """Cancel a build, optionally cascading to the claims its tasks hold.

        ``cascade=True`` additionally cancels the build's RUNNING /
        SUSPENDED tasks, releasing their execution claims and
        concurrency-limit slots (see :meth:`RegistryABC.build_cancel`).

        Returns the cancelled build. ``cascade`` and the cascade fields
        are ignored by servers predating them, in which case the returned
        record simply reports nothing cascaded.
        """
        params = self._get_event_params()
        if cascade:
            params["cascade"] = "true"
        response = self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/cancel",
            params=params,
            operation="Cancel build",
        )
        logger.info(f"Cancelled build: {build_id}")
        return BuildCancelResult.model_validate(response.json())

    def build_exit_early(self, build_id: UUID, reason: str | None = None) -> None:
        """Mark a build as exited early."""
        params = self._get_event_params()
        if reason:
            params["reason"] = reason
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/exit-early",
            params=params,
            operation="Exit early",
        )
        logger.info(f"Build exited early: {build_id}")

    # -------------------------------------------------------------------------
    # Sync task methods
    # -------------------------------------------------------------------------

    def task_register(self, build_id: UUID, task: "BaseTask") -> None:
        """Register a task within a build."""
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks",
            json=_get_task_data_for_registration(task),
            params=self._get_params(),
            operation=f"Register task {task.id}",
        )

    def task_register_bulk(
        self,
        build_id: UUID,
        tasks: Sequence["BaseTask"],
        *,
        limit_keys: Mapping[UUID, Sequence[str]] | None = None,
    ) -> list[RegisteredTaskInfo] | None:
        """Bulk-register tasks via the ``/tasks/bulk`` endpoint.

        Falls back to per-task ``task_register`` if the API doesn't
        support the endpoint (older deployments) — same backwards-compat
        pattern as ``task_add_dependencies``.

        Raises ``ValueError`` if the batch exceeds
        ``_MAX_BULK_REGISTER_TASKS`` (mirrors the server cap). The build
        engine chunks above this method; external callers of
        ``APIRegistry`` get an explicit client-side error rather than a
        400 from the server.

        Passes ``?id_only=true`` so the server returns only the
        ``{id, task_id}`` mapping rather than echoing back full
        ``TaskResponse`` rows we'd discard anyway. Cuts response size
        by ~10× for batches with rich task_data.
        """
        if not tasks:
            return None
        if len(tasks) > _MAX_BULK_REGISTER_TASKS:
            raise ValueError(
                f"task_register_bulk supports at most {_MAX_BULK_REGISTER_TASKS} "
                f"tasks per call (got {len(tasks)}). Chunk the input on the "
                f"caller side."
            )
        try:
            response = self._request(
                "POST",
                f"{self.api_url}/api/v1/builds/{build_id}/tasks/bulk",
                json={
                    "tasks": [
                        _get_task_data_for_registration(t, limit_keys) for t in tasks
                    ]
                },
                params={**self._get_params(), "id_only": "true"},
                operation=f"Bulk-register {len(tasks)} tasks",
            )
        except NotFoundError as e:
            if not is_missing_route_error(e):
                raise
            logger.warning(
                "Registry API does not support POST /tasks/bulk; "
                "falling back to per-task registration. "
                "Upgrade the Registry API for batched registration."
            )
            for t in tasks:
                self.task_register(build_id, t)
            return None
        return _parse_bulk_register_response(response.json())

    def _get_event_params(self) -> dict[str, str]:
        """Get query params for event endpoints, including commit_hash."""
        params = self._get_params()
        try:
            params["commit_hash"] = get_git_commit_hash()
        except Exception:
            pass  # Git not available, skip commit_hash
        return params

    def task_start(
        self,
        build_id: UUID,
        task: "BaseTask",
        executor: str | None = None,
        executor_ref: str | None = None,
        executor_metadata: dict[str, Any] | None = None,
        claim_ttl_seconds: int | None = None,
    ) -> None:
        """Mark a task as started.

        Caller must have already registered the task (via ``task_register`` or
        as a side effect of a parent's static-deps reconciliation). The /start
        endpoint will 404 otherwise.
        """
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/start",
            params=self._get_start_params(
                executor, executor_ref, executor_metadata, claim_ttl_seconds
            ),
            operation=f"Start task {task.id}",
        )

    def _get_start_params(
        self,
        executor: str | None,
        executor_ref: str | None,
        executor_metadata: dict[str, Any] | None = None,
        claim_ttl_seconds: int | None = None,
    ) -> dict[str, str]:
        """Event params plus optional detached-execution reference.

        ``executor_metadata`` rides as a JSON-encoded query param (the
        start endpoint has no body); older servers ignore the unknown
        param, so no version gating is needed. The same goes for
        ``claim_ttl_seconds``: a server predating claim expiry ignores it
        and the recorded claim simply never lapses, which is that server's
        behaviour anyway.
        """
        params = self._get_event_params()
        if executor is not None:
            params["executor"] = executor
        if executor_ref is not None:
            params["executor_ref"] = executor_ref
        if executor_metadata is not None:
            params["executor_metadata"] = _json.dumps(
                executor_metadata, separators=(",", ":")
            )
        if claim_ttl_seconds is not None:
            params["claim_ttl_seconds"] = str(claim_ttl_seconds)
        return params

    def task_complete(self, build_id: UUID, task: "BaseTask") -> None:
        """Mark a task as completed."""
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/complete",
            params=self._get_event_params(),
            operation=f"Complete task {task.id}",
        )

    def task_fail(
        self, build_id: UUID, task: "BaseTask", error_message: str | None = None
    ) -> None:
        """Mark a task as failed."""
        params = self._get_event_params()
        if error_message:
            params["error_message"] = error_message
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/fail",
            params=params,
            operation=f"Fail task {task.id}",
        )

    def task_interrupt(
        self, build_id: UUID, task: "BaseTask", reason: str | None = None
    ) -> None:
        """Record that the platform interrupted this task's execution.

        **A server without the route is not an error, and must not fall
        back to ``task_fail``.** Recording nothing is exactly how this SDK
        behaved before interruptions existed: the task stays RUNNING and a
        later scheduler pass discovers the dead execution and retries it.
        Recording a *failure* instead would turn a version skew into
        permanently failed builds — the precise footgun this whole feature
        exists to remove. So the degradation is silence, loudly logged.
        """
        params = self._get_event_params()
        if reason:
            params["reason"] = reason
        try:
            self._request(
                "POST",
                f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/interrupt",
                params=params,
                operation=f"Interrupt task {task.id}",
            )
        except NotFoundError as e:
            if not is_missing_route_error(e):
                raise
            logger.warning(
                "Registry API does not support POST /interrupt; the "
                "interruption of task %s will not be recorded, and its "
                "execution claim stays held until a scheduler observes the "
                "execution is gone. Upgrade the Registry API for immediate "
                "rescheduling of interrupted tasks.",
                task.id,
            )

    def task_suspend(self, build_id: UUID, task: "BaseTask") -> None:
        """Mark a task as suspended (waiting for dynamic dependencies)."""
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/suspend",
            params=self._get_event_params(),
            operation=f"Suspend task {task.id}",
        )

    def task_add_dependencies(
        self,
        build_id: UUID,
        task: "BaseTask",
        upstream_tasks: Sequence["BaseTask"],
        is_dynamic: bool = True,
    ) -> None:
        """Record dependency edges for a task.

        Backward-compat: an older Registry API that lacks the
        ``/dependencies`` endpoint returns FastAPI's default 404 with the
        generic ``"Not Found"`` detail. We swallow that specific response
        with a warning so builds don't break on version skew. All other
        404s (e.g. our endpoint's explicit ``"Build not found"`` or
        ``"Task … not registered …"`` responses) re-raise normally.
        """
        if not upstream_tasks:
            return
        try:
            self._request(
                "POST",
                f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/dependencies",
                json={
                    "upstream_task_ids": [str(u.id) for u in upstream_tasks],
                    "is_dynamic": is_dynamic,
                },
                params=self._get_params(),
                operation=f"Add dependencies for task {task.id}",
            )
        except NotFoundError as e:
            if not is_missing_route_error(e):
                raise
            logger.warning(
                "Registry API does not support POST /dependencies; "
                "dynamic-dep edges for task %s will not be recorded. "
                "Upgrade the Registry API to see dynamic deps in the DAG view.",
                task.id,
            )

    def task_resume(self, build_id: UUID, task: "BaseTask") -> None:
        """Mark a task as resumed (dynamic dependencies completed)."""
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/resume",
            params=self._get_event_params(),
            operation=f"Resume task {task.id}",
        )

    def task_cancel(self, build_id: UUID, task: "BaseTask") -> None:
        """Cancel a task."""
        self.task_cancel_by_id(build_id, str(task.id))

    def task_cancel_by_id(self, build_id: UUID, task_id: str) -> None:
        """Cancel a task addressed by id (see the ABC for why this exists)."""
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task_id}/cancel",
            params=self._get_event_params(),
            operation=f"Cancel task {task_id}",
        )

    def task_skip(self, build_id: UUID, task: "BaseTask") -> None:
        """Skip a task whose dependency failed or was cancelled.

        Backward-compat: an older Registry API that lacks the ``/skip``
        endpoint returns FastAPI's default 404 with the generic
        ``"Not Found"`` detail. We swallow that specific response with a
        warning so a new SDK against an old API doesn't fail builds on
        every fail-fast / blocked-dep path. All other 404s (e.g.
        ``"Build not found"``) re-raise normally.
        """
        try:
            self._request(
                "POST",
                f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/skip",
                params=self._get_event_params(),
                operation=f"Skip task {task.id}",
            )
        except NotFoundError as e:
            if not is_missing_route_error(e):
                raise
            logger.warning(
                "Registry API does not support POST /skip; task %s will "
                "remain PENDING in the registry. Upgrade the Registry API "
                "to see SKIPPED status for tasks blocked by failed deps.",
                task.id,
            )

    def task_waiting_for_lock(
        self, build_id: UUID, task: "BaseTask", lock_owner: str | None = None
    ) -> None:
        """Record that a task is waiting for a global lock."""
        params = self._get_event_params()
        if lock_owner:
            params["lock_owner"] = lock_owner
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/waiting-for-lock",
            params=params,
            operation=f"Task {task.id} waiting for lock",
        )

    def task_upload_artifacts(
        self, build_id: UUID, task: "BaseTask", artifacts: Sequence[Artifact]
    ) -> None:
        """Upload artifacts for a completed task."""
        if not artifacts:
            return

        # Serialize artifacts to API format
        # For all artifact types, body is stored as a dict in body_json
        # - markdown: {"content": "<markdown string>"}
        # - json: the actual JSON data dict
        artifacts_data = []
        for artifact in artifacts:
            data = artifact.model_dump(mode="json")
            if artifact.type == "markdown":
                # Wrap markdown body string in {"content": ...} dict
                data["body"] = {"content": data["body"]}
            artifacts_data.append(data)

        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/artifacts",
            json=artifacts_data,
            params=self._get_params(),
            operation=f"Upload artifacts for task {task.id}",
        )
        logger.debug(f"Uploaded {len(artifacts)} artifacts for task {task.id}")

    # -------------------------------------------------------------------------
    # Named concurrency limits (per-environment)
    # -------------------------------------------------------------------------

    def concurrency_limit_list(self) -> list[dict[str, Any]]:
        """List the environment's named concurrency limits.

        Returns a list of ``{"key": str, "max_concurrent": int}`` dicts,
        ordered by key.
        """
        response = self._request(
            "GET",
            f"{self.api_url}/api/v1/concurrency-limits",
            params=self._get_params(),
            operation="List concurrency limits",
        )
        return list(response.json().get("limits", []))

    def concurrency_limit_set(self, key: str, max_concurrent: int) -> dict[str, Any]:
        """Create or update a named concurrency limit (PUT upsert).

        Returns the resulting ``{"key", "max_concurrent"}`` dict.
        """
        response = self._request(
            "PUT",
            f"{self.api_url}/api/v1/concurrency-limits/{quote(key, safe='')}",
            json={"max_concurrent": max_concurrent},
            params=self._get_params(),
            operation=f"Set concurrency limit {key!r}",
        )
        return response.json()

    def concurrency_limit_delete(self, key: str) -> None:
        """Delete a named concurrency limit (the key becomes unlimited)."""
        self._request(
            "DELETE",
            f"{self.api_url}/api/v1/concurrency-limits/{quote(key, safe='')}",
            params=self._get_params(),
            operation=f"Delete concurrency limit {key!r}",
        )

    def concurrency_limit_holders(self, key: str, limit: int = 100) -> dict[str, Any]:
        """List the RUNNING tasks currently holding slots of ``key``.

        Returns ``{"key", "holders": [...], "total": int}`` where each
        holder carries task id/name, ``latest_status_at`` (running since)
        and executor info. ``total`` is the full holder count (``holders``
        is capped by ``limit``, oldest-running first).
        """
        response = self._request(
            "GET",
            f"{self.api_url}/api/v1/concurrency-limits/{quote(key, safe='')}/holders",
            params={**self._get_params(), "limit": str(limit)},
            operation=f"List holders of concurrency limit {key!r}",
        )
        return response.json()

    def concurrency_limit_evict(self, key: str, task_id: str) -> dict[str, Any]:
        """Evict a RUNNING slot holder of ``key`` (records TASK_FAILED).

        Recovery path for slots leaked by a dead build process. Returns the
        ``{"task_id", "status"}`` event response.
        """
        response = self._request(
            "POST",
            f"{self.api_url}/api/v1/concurrency-limits/{quote(key, safe='')}"
            f"/holders/{quote(task_id, safe='')}/evict",
            params=self._get_params(),
            operation=f"Evict {task_id} from concurrency limit {key!r}",
        )
        return response.json()

    def task_get_metadata(self, task_id: UUID) -> TaskMetadata:
        """Get metadata for a registered task.

        Args:
            task_id: The UUID of the task to get metadata for.

        Returns:
            A TaskMetadata object containing task metadata.
        """

        response = self._request(
            "GET",
            f"{self.api_url}/api/v1/tasks/{task_id}/metadata",
            params=self._get_params(),
            operation=f"Get metadata for task {task_id}",
        )
        data = response.json()

        return TaskMetadata.model_validate(data)

    # -------------------------------------------------------------------------
    # Client lifecycle
    # -------------------------------------------------------------------------

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            self._client.close()
            self._client = None

    async def aclose(self) -> None:
        """Close the async HTTP client."""
        if self._async_client is not None:
            await self._async_client.aclose()
            self._async_client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()
        return False

    # -------------------------------------------------------------------------
    # Async client and methods
    # -------------------------------------------------------------------------

    @property
    def async_client(self):
        """Lazy-initialized async HTTP client with retry transport.

        The client is recreated if the event loop changes, which can happen
        when running in frameworks like Prefect that create new event loops
        for task execution.
        """

        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        # Recreate client if loop changed or client doesn't exist
        if self._async_client is None or self._async_client_loop != current_loop:
            # Close old client if it exists
            old_client = self._async_client
            if old_client is not None:
                # Schedule close on the old loop if possible, otherwise just discard
                try:
                    if self._async_client_loop and self._async_client_loop.is_running():
                        self._async_client_loop.call_soon_threadsafe(
                            lambda c=old_client: asyncio.create_task(c.aclose())
                        )
                except Exception:
                    pass  # Best effort cleanup

            # Use limits to prevent stale connection issues
            # keepalive_expiry=5 closes idle connections after 5 seconds
            limits = httpx.Limits(
                max_connections=_ASYNC_MAX_CONNECTIONS,
                max_keepalive_connections=_ASYNC_MAX_KEEPALIVE_CONNECTIONS,
                keepalive_expiry=5,
            )
            transport = RetryTransport(retry=_RETRY_CONFIG)
            self._async_client = httpx.AsyncClient(
                timeout=self.timeout,
                auth=self._auth,
                limits=limits,
                transport=transport,
                headers=SDK_CLIENT_HEADERS,
            )
            self._async_client_loop = current_loop
        return self._async_client

    async def build_start_aio(
        self,
        root_tasks: list["BaseTask"] | None = None,
        description: str | None = None,
        executor_metadata: dict[str, Any] | None = None,
    ) -> UUID:
        """Async version - start a new build and return its ID."""
        build_data: dict[str, Any] = {
            "commit_hash": get_git_commit_hash(),
            "root_task_ids": [str(task.id) for task in (root_tasks or [])],
            "description": description,
        }
        if executor_metadata is not None:
            build_data["executor_metadata"] = executor_metadata

        response = await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds",
            json=build_data,
            params=self._get_params(),
            operation="Start build",
        )
        data = response.json()
        build_id = UUID(data["id"])
        logger.info(f"Started build: {data['name']} (ID: {build_id})")
        return build_id

    async def build_resume_aio(
        self,
        build_id: UUID,
        executor_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Async version - mark an existing build as resumed.

        See :meth:`build_resume` for the backward-compat 404 handling.
        """
        params = self._get_event_params()
        if executor_metadata is not None:
            params["executor_metadata"] = _json.dumps(
                executor_metadata, separators=(",", ":")
            )
        try:
            await self._arequest(
                "POST",
                f"{self.api_url}/api/v1/builds/{build_id}/resume",
                params=params,
                operation="Resume build",
            )
        except NotFoundError as e:
            if not is_missing_route_error(e):
                raise
            logger.warning(
                "Registry API does not support POST /builds/%s/resume; "
                "the resumed build will keep its previous status in the "
                "registry until you upgrade the API. Build will still run "
                "to completion locally.",
                build_id,
            )
            return
        logger.info(f"Resumed build: {build_id}")

    async def build_complete_aio(self, build_id: UUID) -> None:
        """Async version - mark a build as completed."""
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/complete",
            params=self._get_event_params(),
            operation="Complete build",
        )
        logger.info(f"Completed build: {build_id}")

    async def build_fail_aio(
        self, build_id: UUID, error_message: str | None = None
    ) -> None:
        """Async version - mark a build as failed."""
        params = self._get_event_params()
        if error_message:
            params["error_message"] = error_message
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/fail",
            params=params,
            operation="Fail build",
        )
        logger.info(f"Marked build as failed: {build_id}")

    async def build_cancel_aio(
        self, build_id: UUID, *, cascade: bool = False
    ) -> BuildCancelResult | None:
        """Async version - cancel a build (see :meth:`build_cancel`)."""
        params = self._get_event_params()
        if cascade:
            params["cascade"] = "true"
        response = await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/cancel",
            params=params,
            operation="Cancel build",
        )
        logger.info(f"Cancelled build: {build_id}")
        return BuildCancelResult.model_validate(response.json())

    async def build_exit_early_aio(
        self, build_id: UUID, reason: str | None = None
    ) -> None:
        """Async version - mark build as exited early."""
        params = self._get_event_params()
        if reason:
            params["reason"] = reason
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/exit-early",
            params=params,
            operation="Exit early",
        )
        logger.info(f"Build exited early: {build_id}")

    def build_list(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status: str | None = None,
        reactive_app_name: str | None = None,
        idle_for_seconds: int | None = None,
    ) -> BuildListPage:
        """One page of ``GET /builds`` (see :meth:`RegistryABC.build_list`).

        All filters are server-side. Older servers ignore query params
        they don't know, so a filter this server predates comes back
        *unapplied* rather than as an error, and silently: the rows look
        like a filtered answer.

        Callers that must not act on an unfiltered list have to detect
        this themselves. Note that re-filtering the page locally does not
        fix it — ``total`` and the page boundaries were computed by the
        server over the unfiltered set, so a locally-trimmed page reports
        a count that is wrong and paginates over the wrong population.
        ``stardag builds list --older-than`` therefore warns rather than
        trimming.
        """
        params = {
            **self._get_params(),
            "page": str(page),
            "page_size": str(page_size),
        }
        if status is not None:
            params["status"] = status
        if reactive_app_name is not None:
            params["reactive_app_name"] = reactive_app_name
        if idle_for_seconds is not None:
            params["idle_for_seconds"] = str(idle_for_seconds)
        response = self._request(
            "GET",
            f"{self.api_url}/api/v1/builds",
            params=params,
            operation="List builds",
        )
        return BuildListPage.model_validate(response.json())

    # Bound on the watchdog sweep in ``build_list_running``: builds come
    # back ordered by last_active_at desc, so RUNNING ones cluster early
    # and paging the entire history to find stragglers would make the
    # sweep cost grow with total build count. Truncation is logged.
    _RUNNING_SWEEP_MAX_PAGES = 10

    def build_list_running(
        self, limit: int = 100, reactive_app_name: str | None = None
    ) -> list[UUID]:
        """List ids of running builds (most recently active first).

        Expressed in terms of :meth:`build_list` with ``status="running"``
        so the derived-status filter is applied *server-side*. It used to
        page the list unfiltered and match on the status client-side,
        which meant an environment holding more non-running builds than
        the page budget could starve the watchdog of the running ones it
        exists to find. Nothing else here changed: same ordering, same
        page budget, same truncation warning.
        """
        running: list[UUID] = []
        page = 1
        page_size = 100
        while len(running) < limit:
            result = self.build_list(
                page=page,
                page_size=page_size,
                status="running",
                reactive_app_name=reactive_app_name,
            )
            for build in result.builds:
                running.append(build.id)
                if len(running) >= limit:
                    break
            if len(result.builds) < page_size:
                break
            if page >= self._RUNNING_SWEEP_MAX_PAGES:
                logger.warning(
                    f"build_list_running: stopped after "
                    f"{self._RUNNING_SWEEP_MAX_PAGES} pages "
                    f"({page * page_size} running builds scanned); older "
                    "running builds (if any) are not included."
                )
                break
            page += 1
        return running

    def build_bulk_cancel(
        self,
        *,
        build_ids: Sequence[UUID | str] | None = None,
        idle_for_seconds: int | None = None,
        reactive_app_name: str | None = None,
        include_reactive: bool = False,
        cascade: bool = True,
        dry_run: bool = False,
        limit: int = 100,
        reason: str | None = None,
    ) -> BulkCancelResult:
        """Bulk-cancel / reap RUNNING builds.

        See :meth:`RegistryABC.build_bulk_cancel` for the semantics. Only
        the filters the caller actually set are put on the wire, so the
        server's own defaults stay authoritative for the rest.
        """
        body: dict[str, Any] = {
            "include_reactive": include_reactive,
            "cascade": cascade,
            "dry_run": dry_run,
            "limit": limit,
        }
        if build_ids is not None:
            body["build_ids"] = [str(b) for b in build_ids]
        if idle_for_seconds is not None:
            body["idle_for_seconds"] = idle_for_seconds
        if reactive_app_name is not None:
            body["reactive_app_name"] = reactive_app_name
        if reason is not None:
            body["reason"] = reason
        response = self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/bulk-cancel",
            json=body,
            params=self._get_params(),
            operation="Bulk-cancel builds",
        )
        return BulkCancelResult.model_validate(response.json())

    def task_list(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status: Sequence[str] | None = None,
        status_older_than: datetime | None = None,
        task_name: str | None = None,
        task_namespace: str | None = None,
    ) -> TaskListPage:
        """One page of ``GET /tasks`` (see :meth:`RegistryABC.task_list`).

        ``status`` rides as a repeated query param, which is why the
        params dict is built as a list of pairs rather than a mapping.
        ``status_older_than`` is sent as an ISO-8601 timestamp.
        """
        # httpx accepts a sequence of (key, value) pairs, which is the only
        # way to express the repeated ``status=`` the server matches on.
        params: list[tuple[str, str]] = [
            *self._get_params().items(),
            ("page", str(page)),
            ("page_size", str(page_size)),
        ]
        for value in status or ():
            params.append(("status", value))
        if status_older_than is not None:
            params.append(("status_older_than", status_older_than.isoformat()))
        if task_name is not None:
            params.append(("task_name", task_name))
        if task_namespace is not None:
            params.append(("task_namespace", task_namespace))
        response = self._request(
            "GET",
            f"{self.api_url}/api/v1/tasks",
            params=params,
            operation="List tasks",
        )
        return TaskListPage.model_validate(response.json())

    def build_report_tick_summary(
        self, build_id: UUID, summary: dict[str, Any]
    ) -> None:
        """Record one reactive scheduler tick's summary against a build.

        Errors propagate — the *caller* owns the best-effort contract (see
        ``stardag.build._reactive``), because only the caller knows whether
        it is on a hot path. Swallowing here would also hide a genuinely
        broken registry from ops tooling that wants to know.
        """
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tick-summaries",
            json=summary,
            params=self._get_params(),
            operation=f"Report tick summary for build {build_id}",
        )

    async def build_report_tick_summary_aio(
        self, build_id: UUID, summary: dict[str, Any]
    ) -> None:
        """Async version - record a tick summary (see the sync variant)."""
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tick-summaries",
            json=summary,
            params=self._get_params(),
            operation=f"Report tick summary for build {build_id}",
        )

    def build_list_tick_summaries(
        self, build_id: UUID, limit: int = 20
    ) -> list[TickSummaryRecord]:
        """List a build's retained tick summaries, newest first."""
        response = self._request(
            "GET",
            f"{self.api_url}/api/v1/builds/{build_id}/tick-summaries",
            params={**self._get_params(), "limit": str(limit)},
            operation=f"List tick summaries for build {build_id}",
        )
        payload = response.json()
        return [
            TickSummaryRecord.model_validate(item)
            for item in payload.get("summaries", [])
        ]

    def build_add_roots(self, build_id: UUID, root_task_ids: list[str]) -> None:
        """Append root task ids to a build."""
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/roots",
            json={"root_task_ids": root_task_ids},
            params=self._get_params(),
            operation=f"Add roots to build {build_id}",
        )

    async def build_add_roots_aio(
        self, build_id: UUID, root_task_ids: list[str]
    ) -> None:
        """Async version - append root task ids to a build."""
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/roots",
            json={"root_task_ids": root_task_ids},
            params=self._get_params(),
            operation=f"Add roots to build {build_id}",
        )

    def task_retry(self, build_id: UUID, task: "BaseTask") -> None:
        """Reset a retryable task to pending (retry)."""
        self.task_retry_by_id(build_id, str(task.id))

    async def task_retry_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version - reset a retryable task to pending."""
        await self.task_retry_by_id_aio(build_id, str(task.id))

    def task_retry_by_id(self, build_id: UUID, task_id: str) -> None:
        """Reset a retryable task to pending, addressed by id."""
        self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task_id}/retry",
            params=self._get_event_params(),
            operation=f"Retry task {task_id}",
        )

    async def task_retry_by_id_aio(self, build_id: UUID, task_id: str) -> None:
        """Async version - reset a retryable task to pending, by id."""
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task_id}/retry",
            params=self._get_event_params(),
            operation=f"Retry task {task_id}",
        )

    def build_skip_blocked(self, build_id: UUID) -> list[str]:
        """Mark tasks transitively blocked by failures as skipped."""
        response = self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/skip-blocked",
            params=self._get_event_params(),
            operation=f"Skip blocked tasks in build {build_id}",
        )
        return list(response.json().get("skipped_task_ids", []))

    async def build_skip_blocked_aio(self, build_id: UUID) -> list[str]:
        """Async version - mark blocked tasks as skipped."""
        response = await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/skip-blocked",
            params=self._get_event_params(),
            operation=f"Skip blocked tasks in build {build_id}",
        )
        return list(response.json().get("skipped_task_ids", []))

    def build_notify(
        self, build_id: UUID, *, can_spawn: bool = True
    ) -> BuildNotifyResult:
        """Set the build's scheduler wake-up flag.

        ``can_spawn=False`` tells the server this caller cannot spawn a
        tick, so it must not mark the build as handed out on the caller's
        behalf (see ``POST /builds/{id}/notify``). Sent only when False, so
        an older server sees the request it always saw.
        """
        response = self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/notify",
            params=self._notify_params(can_spawn),
            operation=f"Notify build {build_id}",
        )
        return self._parse_notify_response(build_id, response)

    async def build_notify_aio(
        self, build_id: UUID, *, can_spawn: bool = True
    ) -> BuildNotifyResult:
        """Async version - set the build's scheduler wake-up flag."""
        response = await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/notify",
            params=self._notify_params(can_spawn),
            operation=f"Notify build {build_id}",
        )
        return self._parse_notify_response(build_id, response)

    def _notify_params(self, can_spawn: bool) -> dict[str, str]:
        params = self._get_params()
        if not can_spawn:
            params = {**params, "can_spawn": "false"}
        return params

    @staticmethod
    def _parse_notify_response(build_id: UUID, response: Any) -> BuildNotifyResult:
        """Parse a notify response, tolerating anything unexpected.

        A wake-up must not fail because the *answer* to it was malformed:
        the flag is already set by the time this runs, and the only thing
        the body adds is an optimisation hint. A server predating
        ``scheduler_live`` — or one that answered with something
        unparseable — therefore yields ``scheduler_live=None``, which
        callers read as "unknown" and handle by spawning as they always
        did.
        """
        try:
            return BuildNotifyResult.model_validate(response.json())
        except Exception:  # pragma: no cover - defensive
            return BuildNotifyResult(build_id=build_id)

    @staticmethod
    def _parse_notify_read(build_id: UUID, response: Any) -> BuildNotifyResult:
        """Parse a *read* of the flag. Unparseable means "no", not "yes".

        Deliberately not :meth:`_parse_notify_response`, whose tolerance
        defaults ``needs_tick`` to True. That is right for the setter — the
        flag really was just set, so the answer only refines an
        optimisation hint — and exactly wrong here, where nothing was set
        and a fabricated "yes" is a claim that something changed.

        The cost of getting this backwards is not a wasted pass. The linger
        loop would clear the flag, re-read the frontier, poll, be told
        "yes" again, and never exit — while holding the build's scheduler
        lease and renewing it, so nothing else can drive that build for the
        life of the container. A silent, non-terminating spin, where the
        pre-endpoint code would have failed loudly on the frontier's
        required fields.

        Note the trigger is wider than "corrupt": every field of
        ``BuildNotifyResult`` has a default, so ``{}`` — or a response whose
        shape drifts — validates cleanly to the model default. Hence an
        explicit False rather than relying on one.
        """
        try:
            payload = response.json()
            value = payload["needs_tick"]
            if not isinstance(value, bool):
                # Not coerced. ``bool("no")`` is True, and every other read
                # of a wake-up answer in this codebase insists on a real
                # bool for the same reason: a truthiness test turns any
                # unexpected shape into the answer that spins the tick.
                raise TypeError(f"needs_tick is {type(value).__name__}, not bool")
            return BuildNotifyResult(build_id=build_id, needs_tick=value)
        except Exception:
            logger.warning(
                "Unparseable wake-up flag for build %s; treating it as unset. "
                "The build is still reached by its next wake-up or the "
                "watchdog — claiming a change nobody made would spin this "
                "tick without ever releasing the scheduler lease.",
                build_id,
            )
            return BuildNotifyResult(build_id=build_id, needs_tick=False)

    def build_wake_candidates(self, limit: int = 20) -> list[WakeCandidate]:
        """Hand out flagged reactive builds with no live scheduler."""
        response = self._request(
            "POST",
            f"{self.api_url}/api/v1/builds/wake-candidates",
            params={**self._get_params(), "limit": str(limit)},
            operation="Fetch wake candidates",
        )
        return self._parse_wake_candidates(response)

    async def build_wake_candidates_aio(self, limit: int = 20) -> list[WakeCandidate]:
        """Async version - hand out flagged reactive builds with no live scheduler."""
        response = await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/wake-candidates",
            params={**self._get_params(), "limit": str(limit)},
            operation="Fetch wake candidates",
        )
        return self._parse_wake_candidates(response)

    @staticmethod
    def _parse_wake_candidates(response: Any) -> list[WakeCandidate]:
        """Parse a wake-candidates response, tolerating anything unexpected.

        Same stance as ``_parse_notify_response``: the answer is a list of
        builds to help, and a malformed one must degrade to "nobody", not
        to a failed scheduler pass. Entries that fail validation are
        dropped individually so one bad row cannot hide the rest.
        """
        try:
            rows = response.json().get("builds") or []
        except Exception:  # pragma: no cover - defensive
            return []
        candidates: list[WakeCandidate] = []
        for row in rows:
            try:
                candidates.append(WakeCandidate.model_validate(row))
            except Exception:
                logger.debug("Ignoring malformed wake candidate: %r", row)
        return candidates

    def build_get_notify(self, build_id: UUID) -> BuildNotifyResult:
        """Read the wake-up flag (``GET /builds/{id}/notify``). One row.

        Falls back to the frontier read against a server that predates the
        route, which is what this poll used to do. The fallback is latched
        per process: the route sits under ``/builds/{build_id}/notify``,
        which such a server *does* serve for POST and DELETE, so the miss
        comes back as a 405 rather than a 404 — and re-paying for it on
        every poll of every lingering build is precisely the cost this
        endpoint exists to remove.
        """
        if _notify_read_route_missing:
            return self._notify_from_frontier(
                build_id, self.build_get_frontier(build_id)
            )
        try:
            response = self._request(
                "GET",
                f"{self.api_url}/api/v1/builds/{build_id}/notify",
                params=self._get_params(),
                operation=f"Read notify flag for build {build_id}",
            )
        except Exception as e:
            if not _latch_notify_read_missing(e):
                raise
            return self._notify_from_frontier(
                build_id, self.build_get_frontier(build_id)
            )
        return self._parse_notify_read(build_id, response)

    async def build_get_notify_aio(self, build_id: UUID) -> BuildNotifyResult:
        """Async version - read the build's scheduler wake-up flag."""
        if _notify_read_route_missing:
            return self._notify_from_frontier(
                build_id, await self.build_get_frontier_aio(build_id)
            )
        try:
            response = await self._arequest(
                "GET",
                f"{self.api_url}/api/v1/builds/{build_id}/notify",
                params=self._get_params(),
                operation=f"Read notify flag for build {build_id}",
            )
        except Exception as e:
            if not _latch_notify_read_missing(e):
                raise
            return self._notify_from_frontier(
                build_id, await self.build_get_frontier_aio(build_id)
            )
        return self._parse_notify_read(build_id, response)

    @staticmethod
    def _notify_from_frontier(
        build_id: UUID, frontier: BuildFrontier
    ) -> BuildNotifyResult:
        """The flag as the fallback reads it: off the frontier, as before.

        Typed rather than ``Any`` so a rename of ``BuildFrontier.needs_tick``
        is a type error here. This path only runs against a server without
        the route, which is the one nothing exercises in practice — so the
        static check is the only check it gets.
        """
        return BuildNotifyResult(build_id=build_id, needs_tick=frontier.needs_tick)

    async def build_acquire_scheduler_lease_aio(
        self, build_id: UUID, *, owner_id: str, ttl_seconds: int
    ) -> SchedulerLeaseResult:
        """Take the build's scheduler lease (``POST .../scheduler-lease``).

        Against a server predating the route, grants unconditionally and
        says so once. That is the pre-lease behaviour — every tick runs —
        which costs duplicate containers on a build being woken repeatedly
        and nothing else: ticks are idempotent, and two of them cannot both
        start a task, because the execution claim arbitrates that
        separately. The same server also reports ``scheduler_live=False``
        to every worker (it reads a lock table this SDK no longer writes),
        so wake-ups spawn unconditionally too — the two halves degrade
        together, which is what makes the combination coherent rather than
        merely survivable.
        """
        return await self._lease_call(
            "POST", build_id, {"owner_id": owner_id, "ttl_seconds": ttl_seconds}
        )

    async def build_renew_scheduler_lease_aio(
        self, build_id: UUID, *, owner_id: str, ttl_seconds: int
    ) -> SchedulerLeaseResult:
        """Extend the lease (``PUT .../scheduler-lease``)."""
        return await self._lease_call(
            "PUT", build_id, {"owner_id": owner_id, "ttl_seconds": ttl_seconds}
        )

    async def build_release_scheduler_lease_aio(
        self, build_id: UUID, *, owner_id: str
    ) -> SchedulerLeaseResult:
        """Drop the lease (``DELETE .../scheduler-lease``)."""
        return await self._lease_call("DELETE", build_id, {"owner_id": owner_id})

    async def _lease_call(
        self, method: str, build_id: UUID, params: dict[str, Any]
    ) -> SchedulerLeaseResult:
        if self._scheduler_lease_route_missing:
            return SchedulerLeaseResult(build_id=build_id, held=True)
        try:
            response = await self._arequest(
                method,
                f"{self.api_url}/api/v1/builds/{build_id}/scheduler-lease",
                params={**self._get_params(), **params},
                operation=f"{method} scheduler lease for build {build_id}",
            )
        except (NotFoundError, APIError) as e:
            # 405: the path exists for other verbs. 404 (missing route): the
            # server predates it entirely. A resource 404 — no such build —
            # must still propagate, which is what is_missing_route_error
            # separates.
            unsupported = (
                is_missing_route_error(e)
                if isinstance(e, NotFoundError)
                else e.status_code == 405
            )
            if not unsupported:
                raise
            self._scheduler_lease_route_missing = True
            logger.warning(
                "Registry API has no scheduler-lease route; reactive ticks "
                "run without a single-flight lease in this process. Duplicate "
                "ticks are possible (idempotent, and task starts are still "
                "arbitrated by the execution claim). Upgrade stardag-api to "
                "restore single-flighting."
            )
            return SchedulerLeaseResult(build_id=build_id, held=True)
        try:
            payload = response.json()
            # ``held`` must be present and an actual boolean — anything
            # else is a malformed answer and belongs in the grant path
            # below. Defaulting a missing key, or coercing (``bool(0)``,
            # ``bool("no")``), would quietly turn a shape error into a
            # denial — the opposite of the degradation this guards.
            held = payload["held"]
            if not isinstance(held, bool):
                raise TypeError(f"held is {held!r}, not a boolean")
            return SchedulerLeaseResult(
                build_id=build_id,
                held=held,
                expires_at=payload.get("expires_at"),
            )
        except Exception:
            # A malformed answer must not decide that nobody may drive the
            # build: unknown grants, exactly like a missing route.
            logger.warning(
                "Unparseable scheduler-lease response for build %s; "
                "proceeding without single-flighting.",
                build_id,
            )
            return SchedulerLeaseResult(build_id=build_id, held=True)

    def build_clear_notify(self, build_id: UUID) -> None:
        """Clear the build's scheduler wake-up flag."""
        self._request(
            "DELETE",
            f"{self.api_url}/api/v1/builds/{build_id}/notify",
            params=self._get_params(),
            operation=f"Clear notify for build {build_id}",
        )

    async def build_clear_notify_aio(self, build_id: UUID) -> None:
        """Async version - clear the build's scheduler wake-up flag."""
        await self._arequest(
            "DELETE",
            f"{self.api_url}/api/v1/builds/{build_id}/notify",
            params=self._get_params(),
            operation=f"Clear notify for build {build_id}",
        )

    def build_get_frontier(self, build_id: UUID) -> BuildFrontier:
        """Return the build's scheduling frontier."""
        response = self._request(
            "GET",
            f"{self.api_url}/api/v1/builds/{build_id}/frontier",
            params=self._get_params(),
            operation=f"Get frontier for build {build_id}",
        )
        return BuildFrontier.model_validate(response.json())

    async def build_get_frontier_aio(self, build_id: UUID) -> BuildFrontier:
        """Async version - return the build's scheduling frontier."""
        response = await self._arequest(
            "GET",
            f"{self.api_url}/api/v1/builds/{build_id}/frontier",
            params=self._get_params(),
            operation=f"Get frontier for build {build_id}",
        )
        return BuildFrontier.model_validate(response.json())

    def build_get_executions(self, build_id: UUID) -> BuildExecutions:
        """Detached executions this build must stop."""
        response = self._request(
            "GET",
            f"{self.api_url}/api/v1/builds/{build_id}/executions",
            params=self._get_params(),
            operation=f"Get executions for build {build_id}",
        )
        return BuildExecutions.model_validate(response.json())

    async def build_get_executions_aio(self, build_id: UUID) -> BuildExecutions:
        """Async version - detached executions this build must stop."""
        response = await self._arequest(
            "GET",
            f"{self.api_url}/api/v1/builds/{build_id}/executions",
            params=self._get_params(),
            operation=f"Get executions for build {build_id}",
        )
        return BuildExecutions.model_validate(response.json())

    def build_get(self, build_id: UUID) -> BuildInfo:
        """Return a slim build record (lighter than the frontier)."""
        response = self._request(
            "GET",
            f"{self.api_url}/api/v1/builds/{build_id}",
            params=self._get_params(),
            operation=f"Get build {build_id}",
        )
        return BuildInfo.model_validate(response.json())

    def build_get_summary(self, build_id: UUID) -> BuildSummary:
        """Return the full build record (see :meth:`RegistryABC.build_get_summary`)."""
        response = self._request(
            "GET",
            f"{self.api_url}/api/v1/builds/{build_id}",
            params=self._get_params(),
            operation=f"Get build {build_id}",
        )
        return BuildSummary.model_validate(response.json())

    async def build_get_aio(self, build_id: UUID) -> BuildInfo:
        """Async version - return a slim build record."""
        response = await self._arequest(
            "GET",
            f"{self.api_url}/api/v1/builds/{build_id}",
            params=self._get_params(),
            operation=f"Get build {build_id}",
        )
        return BuildInfo.model_validate(response.json())

    def build_set_reactive_meta(
        self,
        build_id: UUID,
        *,
        app_name: str,
        tick_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Mark a build reactively scheduled and store its tick config (upsert).

        ``tick_kwargs=None`` (a bare re-trigger) is omitted from the request
        so the server preserves the stored config; pass a dict to update it.
        """
        try:
            self._request(
                "PUT",
                f"{self.api_url}/api/v1/builds/{build_id}/reactive-meta",
                json=self._reactive_meta_body(app_name, tick_kwargs),
                params=self._get_params(),
                operation=f"Set reactive meta for build {build_id}",
            )
        except NotFoundError as e:
            raise self._reactive_meta_unsupported_error(e)

    async def build_set_reactive_meta_aio(
        self,
        build_id: UUID,
        *,
        app_name: str,
        tick_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Async version - mark a build reactively scheduled (upsert)."""
        try:
            await self._arequest(
                "PUT",
                f"{self.api_url}/api/v1/builds/{build_id}/reactive-meta",
                json=self._reactive_meta_body(app_name, tick_kwargs),
                params=self._get_params(),
                operation=f"Set reactive meta for build {build_id}",
            )
        except NotFoundError as e:
            raise self._reactive_meta_unsupported_error(e)

    @staticmethod
    def _reactive_meta_body(
        app_name: str, tick_kwargs: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Request body for PUT /reactive-meta; omit tick_kwargs when None.

        A None ``tick_kwargs`` means "leave the stored config untouched" — a
        bare re-trigger preserves the existing tick_kwargs rather than
        wiping them.
        """
        body: dict[str, Any] = {"app_name": app_name}
        if tick_kwargs is not None:
            body["tick_kwargs"] = tick_kwargs
        return body

    @staticmethod
    def _reactive_meta_unsupported_error(err: NotFoundError) -> Exception:
        """Turn a missing-route 404 into a clear reactive-unsupported error.

        Reactive scheduling requires a matching stardag-api version. When the
        server predates the reactive-meta endpoint the PUT 404s with the
        missing-route body — surface it as a clear error at the trigger (like
        the frontier/notify contract) rather than silently degrading. A
        resource-level 404 (build does not exist) is returned as-is.
        """
        if not is_missing_route_error(err):
            return err
        return RuntimeError(
            "The registry server does not support reactive scheduling "
            "(reactive-meta endpoint missing). Upgrade stardag-api to a "
            "version matching this SDK."
        )

    async def task_register_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version - register a task within a build."""
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks",
            json=_get_task_data_for_registration(task),
            params=self._get_params(),
            operation=f"Register task {task.id}",
        )

    async def task_register_bulk_aio(
        self,
        build_id: UUID,
        tasks: Sequence["BaseTask"],
        *,
        limit_keys: Mapping[UUID, Sequence[str]] | None = None,
    ) -> list[RegisteredTaskInfo] | None:
        """Async bulk-register via ``/tasks/bulk`` (one HTTP call instead of N).

        Falls back to per-task ``task_register_aio`` if the API doesn't
        support the endpoint (older deployments).

        Raises ``ValueError`` if the batch exceeds
        ``_MAX_BULK_REGISTER_TASKS`` (mirrors the server cap). The build
        engine chunks above this method; external callers get an
        explicit client-side error rather than a 400 from the server.

        Passes ``?id_only=true`` so the server returns only the
        ``{id, task_id}`` mapping rather than echoing full ``TaskResponse``
        rows that we discard. Cuts response size by ~10× for batches
        with rich task_data.
        """
        if not tasks:
            return None
        if len(tasks) > _MAX_BULK_REGISTER_TASKS:
            raise ValueError(
                f"task_register_bulk_aio supports at most {_MAX_BULK_REGISTER_TASKS} "
                f"tasks per call (got {len(tasks)}). Chunk the input on the "
                f"caller side."
            )
        try:
            response = await self._arequest(
                "POST",
                f"{self.api_url}/api/v1/builds/{build_id}/tasks/bulk",
                json={
                    "tasks": [
                        _get_task_data_for_registration(t, limit_keys) for t in tasks
                    ]
                },
                params={**self._get_params(), "id_only": "true"},
                operation=f"Bulk-register {len(tasks)} tasks",
            )
        except NotFoundError as e:
            if not is_missing_route_error(e):
                raise
            logger.warning(
                "Registry API does not support POST /tasks/bulk; "
                "falling back to per-task registration. "
                "Upgrade the Registry API for batched registration."
            )
            for t in tasks:
                await self.task_register_aio(build_id, t)
            return None
        return _parse_bulk_register_response(response.json())

    async def task_start_claim_aio(
        self,
        build_id: UUID,
        task: "BaseTask",
        executor: str | None = None,
        executor_ref: str | None = None,
        executor_metadata: dict[str, Any] | None = None,
        limit_keys: Sequence[str] | None = None,
        claim_ttl_seconds: int | None = None,
        *,
        claim: bool = True,
    ) -> StartClaimResult:
        """Arbitrated start against the API's one ``/start`` endpoint.

        ``claim`` and ``enforce_limits`` are orthogonal flags on that
        endpoint, and this method is how both are reached: ``claim=True``
        (the default) arbitrates the execution claim, ``limit_keys``
        enforces the named concurrency limits, and a caller wanting only
        the second passes ``claim=False``.

        Maps the structured 409 denials (``task_already_running`` /
        ``task_already_completed`` / ``concurrency_limit_reached``) to a
        :class:`StartClaimResult`. Against an older server the ``claim``
        parameter is ignored and the start behaves like a plain (tolerated-
        duplicate) start — i.e. graceful degradation to pre-claim behavior;
        ``claim_ttl_seconds`` degrades the same way, to a claim that never
        lapses.
        """
        params: dict[str, Any] = self._get_start_params(
            executor, executor_ref, executor_metadata, claim_ttl_seconds
        )
        if claim:
            params["claim"] = "true"
        if limit_keys:
            params["limit_key"] = list(limit_keys)
            params["enforce_limits"] = "true"
        # Name both flags, not one: the two compose, and a log line saying
        # only "claim" for a start that also acquired slots sends a reader
        # looking for the wrong denial.
        arbitration = (
            "+".join(k for k, on in (("claim", claim), ("limits", limit_keys)) if on)
            or "plain"
        )
        try:
            await self._arequest(
                "POST",
                f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/start",
                params=params,
                operation=f"Start task {task.id} ({arbitration})",
            )
        except APIError as e:
            payload = e.payload or {}
            error_code = payload.get("error_code")
            if e.status_code == 409 and error_code == "task_already_running":
                return StartClaimResult(
                    started=False,
                    denied_reason="already_running",
                    executor=payload.get("executor"),
                    executor_ref=payload.get("executor_ref"),
                    latest_status_expires_at=payload.get("latest_status_expires_at"),
                )
            if e.status_code == 409 and error_code == "task_already_completed":
                return StartClaimResult(
                    started=False, denied_reason="already_completed"
                )
            if e.status_code == 409 and error_code == "concurrency_limit_reached":
                return StartClaimResult(
                    started=False,
                    denied_reason="limit",
                    denied_keys=list(payload.get("denied_keys") or []),
                )
            raise
        return StartClaimResult(started=True)

    async def task_start_aio(
        self,
        build_id: UUID,
        task: "BaseTask",
        executor: str | None = None,
        executor_ref: str | None = None,
        executor_metadata: dict[str, Any] | None = None,
        claim_ttl_seconds: int | None = None,
    ) -> None:
        """Async version - mark a task as started.

        Caller must have already registered the task (via ``task_register_aio``
        or as a side effect of a parent's static-deps reconciliation). The
        /start endpoint will 404 otherwise.
        """
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/start",
            params=self._get_start_params(
                executor, executor_ref, executor_metadata, claim_ttl_seconds
            ),
            operation=f"Start task {task.id}",
        )

    async def task_complete_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version - mark a task as completed."""
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/complete",
            params=self._get_event_params(),
            operation=f"Complete task {task.id}",
        )

    async def task_fail_aio(
        self, build_id: UUID, task: "BaseTask", error_message: str | None = None
    ) -> None:
        """Async version - mark a task as failed."""
        params = self._get_event_params()
        if error_message:
            params["error_message"] = error_message
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/fail",
            params=params,
            operation=f"Fail task {task.id}",
        )

    async def task_interrupt_aio(
        self, build_id: UUID, task: "BaseTask", reason: str | None = None
    ) -> None:
        """Async version - record a platform interruption.

        Same old-server behaviour as the sync version: silence, never a
        fallback to ``task_fail``.
        """
        params = self._get_event_params()
        if reason:
            params["reason"] = reason
        try:
            await self._arequest(
                "POST",
                f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/interrupt",
                params=params,
                operation=f"Interrupt task {task.id}",
            )
        except NotFoundError as e:
            if not is_missing_route_error(e):
                raise
            logger.warning(
                "Registry API does not support POST /interrupt; the "
                "interruption of task %s will not be recorded. Upgrade the "
                "Registry API for immediate rescheduling of interrupted "
                "tasks.",
                task.id,
            )

    async def task_suspend_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version - mark a task as suspended."""
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/suspend",
            params=self._get_event_params(),
            operation=f"Suspend task {task.id}",
        )

    async def task_add_dependencies_aio(
        self,
        build_id: UUID,
        task: "BaseTask",
        upstream_tasks: Sequence["BaseTask"],
        is_dynamic: bool = True,
    ) -> None:
        """Async version - record dependency edges for a task.

        Same backward-compat behavior as the sync version: only swallow
        the specific "missing route" 404 (FastAPI default ``"Not Found"``);
        re-raise genuine resource-not-found 404s.
        """
        if not upstream_tasks:
            return
        try:
            await self._arequest(
                "POST",
                f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/dependencies",
                json={
                    "upstream_task_ids": [str(u.id) for u in upstream_tasks],
                    "is_dynamic": is_dynamic,
                },
                params=self._get_params(),
                operation=f"Add dependencies for task {task.id}",
            )
        except NotFoundError as e:
            if not is_missing_route_error(e):
                raise
            logger.warning(
                "Registry API does not support POST /dependencies; "
                "dynamic-dep edges for task %s will not be recorded. "
                "Upgrade the Registry API to see dynamic deps in the DAG view.",
                task.id,
            )

    async def task_resume_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version - mark a task as resumed."""
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/resume",
            params=self._get_event_params(),
            operation=f"Resume task {task.id}",
        )

    async def task_cancel_aio(
        self, build_id: UUID, task: "BaseTask", *, only_if_held: bool = False
    ) -> None:
        """Async version - cancel a task."""
        await self.task_cancel_by_id_aio(
            build_id, str(task.id), only_if_held=only_if_held
        )

    async def task_cancel_by_id_aio(
        self, build_id: UUID, task_id: str, *, only_if_held: bool = False
    ) -> None:
        """Async version - cancel a task addressed by id."""
        params = self._get_event_params()
        if only_if_held:
            params["only_if_held"] = "true"
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task_id}/cancel",
            params=params,
            operation=f"Cancel task {task_id}",
        )

    async def task_skip_aio(self, build_id: UUID, task: "BaseTask") -> None:
        """Async version - skip a task whose dep failed or was cancelled.

        See :meth:`task_skip` for the backward-compat 404 handling.
        """
        try:
            await self._arequest(
                "POST",
                f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/skip",
                params=self._get_event_params(),
                operation=f"Skip task {task.id}",
            )
        except NotFoundError as e:
            if not is_missing_route_error(e):
                raise
            logger.warning(
                "Registry API does not support POST /skip; task %s will "
                "remain PENDING in the registry. Upgrade the Registry API "
                "to see SKIPPED status for tasks blocked by failed deps.",
                task.id,
            )

    async def task_waiting_for_lock_aio(
        self, build_id: UUID, task: "BaseTask", lock_owner: str | None = None
    ) -> None:
        """Async version - record that task is waiting for global lock."""
        params = self._get_event_params()
        if lock_owner:
            params["lock_owner"] = lock_owner
        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/waiting-for-lock",
            params=params,
            operation=f"Task {task.id} waiting for lock",
        )

    async def task_upload_artifacts_aio(
        self, build_id: UUID, task: "BaseTask", artifacts: Sequence[Artifact]
    ) -> None:
        """Async version - upload artifacts for a completed task."""
        if not artifacts:
            return

        artifacts_data = []
        for artifact in artifacts:
            data = artifact.model_dump(mode="json")
            if artifact.type == "markdown":
                data["body"] = {"content": data["body"]}
            artifacts_data.append(data)

        await self._arequest(
            "POST",
            f"{self.api_url}/api/v1/builds/{build_id}/tasks/{task.id}/artifacts",
            json=artifacts_data,
            params=self._get_params(),
            operation=f"Upload artifacts for task {task.id}",
        )
        logger.debug(f"Uploaded {len(artifacts)} artifacts for task {task.id}")

    async def task_get_metadata_aio(self, task_id: UUID) -> TaskMetadata:
        """Async version of task_get_metadata."""

        response = await self._arequest(
            "GET",
            f"{self.api_url}/api/v1/tasks/{task_id}/metadata",
            params=self._get_params(),
            operation=f"Get metadata for task {task_id}",
        )
        data = response.json()

        return TaskMetadata.model_validate(data)


def _get_task_data_for_registration(
    task: "BaseTask", limit_keys: Mapping[UUID, Sequence[str]] | None = None
) -> dict:
    """Helper to serialize task data for registration API call.

    ``limit_keys`` maps task ids to the named concurrency-limit keys the
    task runs under; when the task has an entry it is sent, so the registry
    records the keys at plan time. An absent entry sends nothing, which
    leaves any recorded keys alone.
    """
    # Avoid circular import:
    from stardag._core.base_task import flatten_task_struct  # noqa: F401

    # Extract output_uri if the task has a FileSystemTarget target with a uri
    output_uri: str | None = None
    try:
        target_method = getattr(task, "target", None)
        if target_method is not None:
            target = target_method()
            if hasattr(target, "uri"):
                output_uri = target.uri
    except Exception as e:
        # Log but don't fail - task may not have target() or it may fail
        logger.debug(f"Could not extract output_uri for task {task.id}: {e}")

    return {
        "task_id": str(task.id),
        "task_namespace": task.get_namespace(),
        "task_name": task.get_name(),
        "task_data": task.model_dump(mode="json"),
        "version": task.version,
        "output_uri": output_uri,
        **(
            {"limit_keys": list(limit_keys[task.id])}
            if limit_keys is not None and task.id in limit_keys
            else {}
        ),
        "dependency_task_ids": [
            str(dep.id) for dep in flatten_task_struct(task.requires())
        ],
    }

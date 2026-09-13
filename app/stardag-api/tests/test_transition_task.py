"""``transition_task`` is the only way a task's ``latest_status`` moves.

Every path that records a task event used to pair ``_apply_event_to_task``
with the post-transition hooks by hand. When the cross-build wake-up hook
was added, two were missed, so skip-blocked and the lock release flagged
nobody. ``tests/test_wakeups.py`` is the behavioural regression net for
that; this module guards the structure that makes the next hook a one-line
change instead of a search — deliberately without naming a count, since the
whole point is that new callers may appear.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "stardag_api"

# The module that owns the transition. Everything else must go through it.
_OWNER = SRC / "services" / "status.py"


def _python_files() -> list[pathlib.Path]:
    return sorted(p for p in SRC.rglob("*.py") if p != _OWNER)


def _called_name(node: ast.Call) -> str | None:
    """The bare name a call resolves to, however it was reached.

    Both ``flag(...)`` and ``wakeups.flag(...)`` have to count. Matching only
    ``ast.Name`` misses the module-qualified form — and
    ``from stardag_api.services import wakeups`` is the more natural of the
    two import styles, so the guard would have been blind to the likelier
    spelling.
    """
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _imported_names(node: ast.ImportFrom) -> list[str]:
    """The names an ``import ... from`` binds, ignoring any ``as`` rename.

    The rename is the point: ``import flag_after_task_transition as flag``
    would otherwise slip past a check that only looks at the local binding.
    """
    return [alias.name for alias in node.names]


def test_nothing_outside_status_applies_an_event_to_a_task() -> None:
    """No route or service may call ``_apply_event_to_task`` itself.

    Calling it directly is exactly the bug this guards: it moves the status
    and skips every post-transition hook, silently and without failing a
    test that only checks the status came out right.
    """
    offenders: list[str] = []
    for path in _python_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if _called_name(node) == "_apply_event_to_task":
                    offenders.append(f"{path.relative_to(SRC)}:{node.lineno}")
            elif isinstance(node, ast.ImportFrom):
                if "_apply_event_to_task" in _imported_names(node):
                    offenders.append(f"{path.relative_to(SRC)}:{node.lineno}")

    assert not offenders, (
        "these call or import _apply_event_to_task directly, bypassing the "
        "post-transition hooks (use services.status.transition_task): "
        + ", ".join(offenders)
    )


# Every post-transition hook, by the name it is called under. Each reads
# the status as it was *before* the apply, so each can only be correct from
# inside ``transition_task`` — a second call site means either a path that
# transitions a task without the helper (the shape the helper exists to
# prevent) or the hook running twice for one transition.
_POST_TRANSITION_HOOKS = (
    "flag_after_task_transition",
    "retract_dynamic_edges_if_new_attempt",
)


@pytest.mark.parametrize("hook", _POST_TRANSITION_HOOKS)
def test_each_post_transition_hook_has_exactly_one_call_site(hook: str) -> None:
    call_sites: list[str] = []
    for path in [*_python_files(), _OWNER]:
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _called_name(node) == hook:
                call_sites.append(f"{path.relative_to(SRC)}:{node.lineno}")

    assert len(call_sites) == 1, (
        f"expected exactly one caller of {hook} "
        f"(services/status.py's transition_task), found: {call_sites}"
    )
    assert call_sites[0].startswith("services/status.py"), call_sites

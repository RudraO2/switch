"""`AgentCore.edit_message`: an agent redraws a message it posted."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from switch_core.bridges.agent.protocol.agent_core import AgentCore
from switch_core.trust.client import GuardrailBlockedError, TrustCheckResult

AGENT_USER = "@agent-scout:switch.local"


class _FakeTransport:
    def __init__(self, events: dict[str, Any]) -> None:
        self.events = events
        self.lookups: list[tuple[str, str]] = []

    async def get_event(self, room_id: str, event_id: str) -> Any:
        self.lookups.append((room_id, event_id))
        return self.events.get(event_id)


class _FakeCollab:
    def __init__(self, *, edited: bool = True) -> None:
        self.edited = edited
        self.calls: list[tuple[str, str, str, str]] = []

    async def handle_outbound_edit(
        self, room_id: str, transport_event_id: str, sender_name: str, new_body: str
    ) -> bool:
        self.calls.append((room_id, transport_event_id, sender_name, new_body))
        return self.edited


def _build(
    *,
    events: dict[str, Any],
    bridge_id: str | None = "bridge-1",
    collab: _FakeCollab | None = None,
    trust: Any = None,
) -> tuple[AgentCore, list[tuple[Any, ...]]]:
    alerts: list[tuple[Any, ...]] = []

    async def _require(agent_id: str, room_id: str) -> Any:
        return SimpleNamespace(
            id=room_id, transport_room_id="!room", bridge_id=bridge_id
        )

    async def _enforce_trust(
        room: Any, content: str, thread_root_id: str | None, client: Any
    ) -> tuple[str, Any]:
        if trust is not None:
            return await trust(content)
        return content, None

    async def _post_trust_alert_notice(
        room: Any, event_id: str, annotation: str, outcome: str
    ) -> None:
        alerts.append((room.id, event_id, annotation, outcome))

    client = SimpleNamespace(
        transport=_FakeTransport(events),
        transport_user_id=AGENT_USER,
        display_name="Scout",
    )
    svc = object.__new__(AgentCore)
    svc.require_room_poster = _require  # type: ignore[assignment]
    svc.client_lifecycle = SimpleNamespace(  # type: ignore[assignment]
        get_by_agent_id=lambda agent_id: client
    )
    svc.collab_lifecycle = SimpleNamespace(  # type: ignore[assignment]
        get=lambda bridge: collab
    )
    svc._enforce_trust = _enforce_trust  # type: ignore[assignment]
    svc._post_trust_alert_notice = _post_trust_alert_notice  # type: ignore[assignment]
    return svc, alerts


def _own(event_id: str) -> dict[str, Any]:
    return {event_id: SimpleNamespace(sender_id=AGENT_USER)}


async def test_an_agent_edits_its_own_message_through_the_bridge() -> None:
    collab = _FakeCollab()
    svc, alerts = _build(events=_own("$post"), collab=collab)

    edited = await svc.edit_message("agent-1", "room-1", "$post", "now at 1:45")

    assert edited is True
    assert collab.calls == [("!room", "$post", "Scout", "now at 1:45")]
    assert alerts == []


async def test_an_agent_cannot_edit_someone_elses_message() -> None:
    collab = _FakeCollab()
    events = {"$post": SimpleNamespace(sender_id="@meera:switch.local")}
    svc, _ = _build(events=events, collab=collab)

    with pytest.raises(PermissionError):
        await svc.edit_message("agent-1", "room-1", "$post", "hijacked")
    assert collab.calls == []


async def test_an_event_that_is_not_in_the_room_is_not_found() -> None:
    collab = _FakeCollab()
    svc, _ = _build(events={}, collab=collab)

    with pytest.raises(ValueError, match="event not found"):
        await svc.edit_message("agent-1", "room-1", "$missing", "text")
    assert collab.calls == []


async def test_a_room_without_a_bridge_reports_nothing_edited() -> None:
    checked: list[str] = []

    async def _trust(content: str) -> tuple[str, Any]:
        checked.append(content)
        return content, None

    svc, _ = _build(events=_own("$post"), bridge_id=None, trust=_trust)

    assert await svc.edit_message("agent-1", "room-1", "$post", "text") is False
    # The new text is still checked: an edit is a post like any other.
    assert checked == ["text"]


async def test_the_edited_text_goes_through_switch_trust() -> None:
    async def _redact(content: str) -> tuple[str, Any]:
        return content.replace("sk-secret", "[redacted]"), None

    collab = _FakeCollab()
    svc, _ = _build(events=_own("$post"), collab=collab, trust=_redact)

    await svc.edit_message("agent-1", "room-1", "$post", "key sk-secret")

    assert collab.calls[0][3] == "key [redacted]"


async def test_a_blocked_edit_never_reaches_the_platform() -> None:
    async def _block(content: str) -> tuple[str, Any]:
        raise GuardrailBlockedError(
            TrustCheckResult(outcome="blocked", policy_id="p", policy_name="P")
        )

    collab = _FakeCollab()
    svc, _ = _build(events=_own("$post"), collab=collab, trust=_block)

    with pytest.raises(GuardrailBlockedError):
        await svc.edit_message("agent-1", "room-1", "$post", "bad link")
    assert collab.calls == []


async def test_an_alerted_edit_is_redrawn_and_the_alert_posted() -> None:
    async def _alert(content: str) -> tuple[str, Any]:
        return content, ("flagged for review", "alerted")

    collab = _FakeCollab()
    svc, alerts = _build(events=_own("$post"), collab=collab, trust=_alert)

    assert await svc.edit_message("agent-1", "room-1", "$post", "text") is True
    assert alerts == [("room-1", "$post", "flagged for review", "alerted")]


async def test_a_bridge_that_is_not_running_is_an_error() -> None:
    svc, _ = _build(events=_own("$post"), collab=None)

    with pytest.raises(ValueError, match="is not running"):
        await svc.edit_message("agent-1", "room-1", "$post", "text")

"""`CollaborationCore.handle_outbound_edit`: redraw the platform post a room
message became, found through the bridge's message map."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from switch_core.bridges.collaboration.collaboration_core import CollaborationCore


class _NullSession:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *_a: object) -> bool:
        return False


class _FakeAdapter:
    def __init__(self) -> None:
        self.edits: list[tuple[str, str, str, str]] = []

    def translate_outbound(self, text: str) -> str:
        return f"<{text}>"

    async def edit_agent_message(
        self, channel_id: str, message_ref: str, sender_name: str, content: str
    ) -> None:
        self.edits.append((channel_id, message_ref, sender_name, content))


def _bridge(*, channel: str | None, mapping: Any) -> tuple[Any, _FakeAdapter]:
    adapter = _FakeAdapter()

    async def _channel_for_room(transport_room_id: str) -> str | None:
        return channel

    async def _get(session: object, bridge_id: str, transport_event_id: str) -> Any:
        assert bridge_id == "bridge-1"
        return mapping

    async def _room_tenant(room_id: str) -> str:
        return "tenant-1"

    bridge = SimpleNamespace(
        _channel_for_room=_channel_for_room,
        _session_factory=lambda: _NullSession(),
        _bridge_tenant_id="tenant-1",
        _bridge_id="bridge-1",
        _bridge_message_map_store=SimpleNamespace(get_by_transport_event_id=_get),
        _channel_to_room={"chan-1": ("room-uuid", "!room")},
        _room_tenant=_room_tenant,
        _adapter=adapter,
    )
    return bridge, adapter


async def test_the_mapped_post_is_redrawn_with_translated_text() -> None:
    mapping = SimpleNamespace(external_channel_id="chan-1", external_post_id="post-9")
    bridge, adapter = _bridge(channel="chan-1", mapping=mapping)

    edited = await CollaborationCore.handle_outbound_edit(
        bridge, "!room", "$post", "Scout", "now at 1:45"
    )

    assert edited is True
    assert adapter.edits == [("chan-1", "post-9", "Scout", "<now at 1:45>")]


async def test_a_room_with_no_channel_reports_nothing_edited() -> None:
    bridge, adapter = _bridge(channel=None, mapping=None)

    assert (
        await CollaborationCore.handle_outbound_edit(
            bridge, "!room", "$post", "Scout", "text"
        )
        is False
    )
    assert adapter.edits == []


async def test_a_message_the_bridge_never_posted_reports_nothing_edited() -> None:
    bridge, adapter = _bridge(channel="chan-1", mapping=None)

    assert (
        await CollaborationCore.handle_outbound_edit(
            bridge, "!room", "$post", "Scout", "text"
        )
        is False
    )
    assert adapter.edits == []


async def test_a_mapping_without_a_platform_post_reports_nothing_edited() -> None:
    mapping = SimpleNamespace(external_channel_id="chan-1", external_post_id=None)
    bridge, adapter = _bridge(channel="chan-1", mapping=mapping)

    assert (
        await CollaborationCore.handle_outbound_edit(
            bridge, "!room", "$post", "Scout", "text"
        )
        is False
    )
    assert adapter.edits == []

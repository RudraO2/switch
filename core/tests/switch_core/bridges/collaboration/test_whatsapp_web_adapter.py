"""The whatsapp_web adapter: config, markup, pacing, which chats it bridges,
inbound events, and the sidecar calls it makes. The sidecar's HTTP API is
mocked at the transport, as the platform guide asks."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from switch_core.bridges.collaboration.models import InboundCommand, InboundMessage
from switch_core.bridges.collaboration.whatsapp_web import adapter as wa
from switch_core.bridges.collaboration.whatsapp_web.adapter import (
    Pacer,
    PostingCapReached,
    WhatsAppWebAdapter,
    WhatsAppWebConnectionConfig,
    render_whatsapp,
    translate_whatsapp,
)

GROUP = "120363000000000001@g.us"
OTHER = "120363000000000002@g.us"
PERSON = "919800000001@c.us"


def config(**overrides: Any) -> WhatsAppWebConnectionConfig:
    return WhatsAppWebConnectionConfig(
        **{"api_key": "secret", "chats": ["Lantern Build Night"], **overrides}
    )


class Sidecar:
    """The WAHA HTTP API, recorded."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Any]] = []
        self.subjects = {GROUP: "Lantern Build Night", OTHER: "Family"}

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        self.calls.append((request.method, request.url.path, body))
        assert request.headers["X-Api-Key"] == "secret"
        path = request.url.path
        if path.startswith("/api/default/groups/"):
            gid = path.rsplit("/", 1)[1]
            return httpx.Response(
                200, json={"id": gid, "subject": self.subjects.get(gid, "?")}
            )
        if path in ("/api/sendText", "/api/sendImage", "/api/sendFile"):
            return httpx.Response(
                201, json={"id": f"true_{body['chatId']}_SENT{len(self.calls)}"}
            )
        if path.startswith("/api/files/"):
            return httpx.Response(200, content=b"PNGDATA")
        return httpx.Response(200, json={})

    def posts(self) -> list[tuple[str, Any]]:
        return [(p, b) for (m, p, b) in self.calls if p.startswith("/api/send")]


def make(
    cfg: WhatsAppWebConnectionConfig | None = None,
) -> tuple[WhatsAppWebAdapter, Sidecar]:
    side = Sidecar()
    adapter = WhatsAppWebAdapter(config=cfg or config())
    adapter._http = httpx.AsyncClient(
        base_url=adapter.config.waha_url,
        headers={"X-Api-Key": adapter.config.api_key},
        transport=httpx.MockTransport(side.handler),
    )
    return adapter, side


@pytest.fixture(autouse=True)
def no_pauses(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(wa.random, "uniform", lambda a, b: 0.0)


def message(**fields: Any) -> dict[str, Any]:
    base = {
        "id": "false_120363000000000001@g.us_ABC_919800000001@c.us",
        "from": GROUP,
        "to": "919800000000@c.us",
        "fromMe": False,
        "source": "app",
        "participant": PERSON,
        "body": "@ferri when does hacking start?",
        "hasMedia": False,
        "_data": {"Info": {"PushName": "Meera"}},
    }
    base.update(fields)
    return base


async def collect(
    adapter: WhatsAppWebAdapter,
) -> tuple[list[InboundMessage], list[InboundCommand]]:
    got: list[InboundMessage] = []
    cmds: list[InboundCommand] = []

    async def on_message(m: InboundMessage) -> None:
        got.append(m)

    async def on_command(c: InboundCommand) -> None:
        cmds.append(c)

    async def nothing(_: Any) -> None:
        return None

    adapter.set_listening(False)  # no stream in tests
    await adapter.start(on_message, on_command, nothing, nothing, nothing)
    return got, cmds


# ── Config ─────────────────────────────────────────────────────────────────


def test_config_defaults_and_cleanup() -> None:
    c = config(pairing_phone="+91 98000-00000", chats=[" Lantern Build Night ", ""])
    assert c.waha_url == "http://waha:3000"
    assert c.pairing_phone == "919800000000"
    assert c.chats == ["Lantern Build Night"]
    assert c.direct_chats is False


@pytest.mark.parametrize(
    "bad",
    [
        {"waha_url": "waha:3000"},
        {"session": "has space"},
        {"pairing_phone": "call me"},
        {"min_seconds_between_posts": 0.1},
        {"max_posts_per_hour": 0},
    ],
)
def test_config_refuses(bad: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        config(**bad)


# ── Markup ─────────────────────────────────────────────────────────────────


def test_markdown_becomes_whatsapp_markup() -> None:
    assert render_whatsapp("**Bold** ~~gone~~") == "*Bold* ~gone~"
    assert render_whatsapp("**Bold** and *it*") == "*Bold* and _it_"
    assert render_whatsapp("just *this*") == "just _this_"
    assert render_whatsapp("[the site](https://x.dev)") == "the site (https://x.dev)"
    assert render_whatsapp("# Title") == "*Title*"
    assert render_whatsapp("```\n**kept**\n```") == "```\n**kept**\n```"


def test_whatsapp_markup_becomes_markdown() -> None:
    assert translate_whatsapp("*bold* and ~gone~") == "**bold** and ~~gone~~"
    assert translate_whatsapp("2*3*4 stays") == "2*3*4 stays"


# ── Pacing ─────────────────────────────────────────────────────────────────


def test_pacer_spaces_posts_in_a_chat_and_caps_the_hour() -> None:
    now = [1000.0]
    pacer = Pacer(min_gap=3.0, per_hour=2, clock=lambda: now[0])
    assert pacer.wait_for(GROUP) == 0.0
    pacer.record(GROUP)
    now[0] += 1.0
    assert pacer.wait_for(GROUP) == pytest.approx(2.0)
    assert pacer.wait_for(OTHER) == 0.0
    pacer.record(OTHER)
    with pytest.raises(PostingCapReached):
        pacer.wait_for(GROUP)
    now[0] += 3600
    assert pacer.wait_for(GROUP) == 0.0


# ── Which chats ────────────────────────────────────────────────────────────


async def test_only_named_groups_are_bridged() -> None:
    adapter, _ = make()
    assert await adapter.bridges(GROUP) is True
    assert await adapter.bridges(OTHER) is False
    assert await adapter.bridges(PERSON) is False
    assert await adapter.bridges("status@broadcast") is False
    everything, _ = make(config(chats=["*"], direct_chats=True))
    assert await everything.bridges(OTHER) is True
    assert await everything.bridges(PERSON) is True
    by_id, _ = make(config(chats=[OTHER]))
    assert await by_id.bridges(OTHER) is True


# ── Inbound ────────────────────────────────────────────────────────────────


async def test_a_group_message_becomes_an_inbound_message() -> None:
    adapter, _ = make()
    got, _ = await collect(adapter)
    await adapter.handle_event(
        {"event": "message.any", "payload": message(replyTo={"id": "false_x_ROOT"})}
    )
    assert len(got) == 1
    m = got[0]
    assert (m.channel_id, m.channel_type, m.channel_name) == (
        GROUP,
        "group",
        "Lantern Build Night",
    )
    assert (m.sender_id, m.sender_name) == (PERSON, "Meera")
    assert m.content == "@ferri when does hacking start?"
    assert m.message_ref == "false_120363000000000001@g.us_ABC_919800000001@c.us"
    assert m.root_id == "false_x_ROOT"


async def test_other_chats_and_our_own_posts_are_not_relayed() -> None:
    adapter, _ = make()
    got, _ = await collect(adapter)
    await adapter.handle_event(
        {"event": "message.any", "payload": message(**{"from": OTHER})}
    )
    await adapter.handle_event(
        {
            "event": "message.any",
            "payload": message(**{"from": PERSON, "participant": None}),
        }
    )
    await adapter.handle_event(
        {
            "event": "message.any",
            "payload": message(
                fromMe=True, source="api", to=GROUP, **{"from": "me@c.us"}
            ),
        }
    )
    assert got == []
    # The account owner typing in a bridged group is a person speaking.
    await adapter.handle_event(
        {
            "event": "message.any",
            "payload": message(
                fromMe=True, source="app", to=GROUP, **{"from": "me@c.us"}
            ),
        }
    )
    assert len(got) == 1 and got[0].channel_id == GROUP


async def test_a_post_the_bridge_sent_is_not_echoed_even_from_the_app() -> None:
    adapter, _ = make()
    got, _ = await collect(adapter)
    ref = await adapter.send_message(GROUP, "ferri", "hello")
    await adapter.handle_event(
        {
            "event": "message.any",
            "payload": message(
                id=ref, fromMe=True, source="app", to=GROUP, **{"from": "me@c.us"}
            ),
        }
    )
    assert got == []


async def test_a_bang_command_is_a_command() -> None:
    adapter, _ = make()
    got, cmds = await collect(adapter)
    await adapter.handle_event(
        {"event": "message.any", "payload": message(body="!invite-agent @ferri")}
    )
    assert got == []
    assert (cmds[0].command, cmds[0].args, cmds[0].channel_id) == (
        "invite-agent",
        "@ferri",
        GROUP,
    )


async def test_media_is_fetched_from_the_sidecar() -> None:
    adapter, side = make()
    got, _ = await collect(adapter)
    media = {
        "url": "http://localhost:3000/api/files/default/abc.png",
        "mimetype": "image/png",
        "filename": "abc.png",
    }
    await adapter.handle_event(
        {
            "event": "message.any",
            "payload": message(body="", hasMedia=True, media=media),
        }
    )
    assert got[0].attachments[0].data == b"PNGDATA"
    assert ("GET", "/api/files/default/abc.png", None) in side.calls


async def test_media_the_sidecar_could_not_download_is_disclosed() -> None:
    adapter, _ = make()
    got, _ = await collect(adapter)
    media = {
        "mimetype": "image/png",
        "filename": "big.png",
        "error": {"message": "too big"},
    }
    await adapter.handle_event(
        {
            "event": "message.any",
            "payload": message(body="", hasMedia=True, media=media),
        }
    )
    assert got[0].attachment_failures[0].reason == "too big"


async def test_status_events_drive_the_attention_line() -> None:
    adapter, _ = make()
    await adapter.handle_event(
        {"event": "session.status", "payload": {"status": "SCAN_QR_CODE"}}
    )
    assert "not linked yet" in (await adapter.attention() or "")
    await adapter.handle_event(
        {
            "event": "session.status",
            "me": {"id": "919800000000@c.us"},
            "payload": {"status": "WORKING"},
        }
    )
    assert await adapter.attention() is None


# ── Outbound ───────────────────────────────────────────────────────────────


async def test_send_message_names_the_agent_quotes_the_thread_and_types_first() -> None:
    adapter, side = make()
    ref = await adapter.send_message(
        GROUP, "ferri", "Hacking starts at **11 AM**.", thread_root_id="false_x_ROOT"
    )
    ((path, body),) = side.posts()
    assert path == "/api/sendText"
    assert body["chatId"] == GROUP and body["session"] == "default"
    assert body["text"].endswith(":* Hacking starts at *11 AM*.")
    assert body["reply_to"] == "false_x_ROOT"
    order = [p for (_, p, _) in side.calls]
    assert (
        order.index("/api/startTyping")
        < order.index("/api/sendText")
        < order.index("/api/stopTyping")
    )
    assert ref and ref.startswith("true_")


async def test_the_hourly_cap_refuses_and_says_so() -> None:
    adapter, side = make(config(max_posts_per_hour=1))
    await adapter.send_message(GROUP, "ferri", "one")
    with pytest.raises(PostingCapReached):
        await adapter.send_message(GROUP, "ferri", "two")
    assert len(side.posts()) == 1
    assert "limit" in (await adapter.attention() or "")


async def test_edit_and_delete_go_to_the_message() -> None:
    adapter, side = make()
    await adapter.update_message(GROUP, "true_x_ID", "new text")
    await adapter.delete_message(GROUP, "true_x_ID")
    methods = [(m, p) for (m, p, _) in side.calls]
    assert ("PUT", f"/api/default/chats/{GROUP}/messages/true_x_ID") in methods
    assert ("DELETE", f"/api/default/chats/{GROUP}/messages/true_x_ID") in methods


async def test_an_image_goes_as_an_image_with_a_caption() -> None:
    adapter, side = make()
    await adapter.send_attachment(
        GROUP, "ferri", "poster.jpg", "image/jpeg", b"JPG", caption="The poster"
    )
    ((path, body),) = side.posts()
    assert path == "/api/sendImage"
    assert body["file"]["mimetype"] == "image/jpeg" and body["file"]["data"] == "SlBH"
    assert body["caption"].endswith(":* The poster")


async def test_groups_are_not_created_from_switch() -> None:
    adapter, _ = make()
    with pytest.raises(wa.BridgeOperationError):
        await adapter.create_channel("x", "y")


async def test_me_and_named_direct_chats_are_bridged_with_direct_chats_off() -> None:
    adapter, _ = make(config(chats=["me", "919811111111"]))
    adapter._me = "919800000000@c.us"
    assert await adapter.bridges("919800000000@c.us") is True
    assert await adapter.bridges("919811111111@c.us") is True
    assert await adapter.bridges(PERSON) is False


async def test_a_note_to_self_under_the_lid_is_the_me_chat() -> None:
    adapter, _ = make(config(chats=["me"]))
    adapter._own = {"919800000000", "100000000000001"}
    adapter._me = "919800000000@c.us"
    got, _ = await collect(adapter)
    note = message(
        id="true_100000000000001@lid_AC27",
        fromMe=True,
        source="app",
        to=None,
        participant=None,
        body="hello ferri",
        **{"from": "100000000000001@lid"},
    )
    await adapter.handle_event({"event": "message.any", "payload": note})
    assert [(m.channel_id, m.channel_type, m.content) for m in got] == [
        ("100000000000001@lid", "direct", "hello ferri")
    ]


async def test_the_owners_own_group_post_belongs_to_the_group() -> None:
    adapter, _ = make()
    got, _ = await collect(adapter)
    own = message(
        id="true_120363000000000001@g.us_AC3C_100000000000001@lid",
        fromMe=True,
        source="app",
        to="100000000000001@lid",
        participant="100000000000001@lid",
        body="hello everyone",
    )
    await adapter.handle_event({"event": "message.any", "payload": own})
    assert [(m.channel_id, m.content) for m in got] == [(GROUP, "hello everyone")]


async def test_a_gows_group_name_is_read_too() -> None:
    adapter, side = make()
    side.subjects = {}

    def gows(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"JID": GROUP, "Name": "Lantern Build Night"})

    adapter._http = httpx.AsyncClient(
        base_url=adapter.config.waha_url,
        headers={"X-Api-Key": "secret"},
        transport=httpx.MockTransport(gows),
    )
    assert await adapter.bridges(GROUP) is True

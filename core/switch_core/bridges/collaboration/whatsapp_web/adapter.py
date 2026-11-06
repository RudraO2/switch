"""WhatsApp through a linked device: the `whatsapp_web` platform.

The bridge links to an existing WhatsApp account the way WhatsApp Web does
(scan a QR code, or type a pairing code, on the phone), through a WAHA sidecar
(https://waha.devlike.pro, Apache-2.0) running its GOWS engine. The adapter
dials out to the sidecar's WebSocket for events and calls its REST API to post,
so no public address is needed.

This is the unofficial route, kept apart from a Cloud API platform on purpose:
it joins the groups people already use, which the Cloud API cannot, and it
carries the platform's terms risk. So it is careful by construction:

- Only the groups named in the connection's `chats` become rooms. Every other
  chat on the account, and every direct chat unless `direct_chats` is on, never
  reaches Switch.
- Posts are paced: typing first, a short random pause, a minimum gap per chat
  and an hourly cap for the whole connection. Past the cap a post is refused
  with a clear error rather than queued up into a burst.
- What the bridge itself sent is never relayed back as a new message.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import random
import re
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any, ClassVar
from urllib.parse import quote, urlencode, urlsplit

import httpx
import websockets
from pydantic import Field, field_validator

from switch_core.bridges.collaboration.adapter import AgentIdentity, PlatformAdapter
from switch_core.bridges.collaboration.models import (
    Attachment,
    AttachmentFailure,
    BridgeConnectionConfig,
    BridgeCredentialError,
    BridgeOperationError,
    ChannelType,
    FailureReason,
    InboundAgentJoin,
    InboundAppJoin,
    InboundCommand,
    InboundMessage,
    InboundUserJoin,
)

logger = logging.getLogger(__name__)

# WhatsApp ids: groups end in @g.us; people in @c.us, @s.whatsapp.net or @lid.
_GROUP_SUFFIX = "@g.us"
_PERSON_SUFFIXES = ("@c.us", "@s.whatsapp.net", "@lid")
# Statuses, channels and broadcast lists are never chats a room maps to.
_NEVER = ("status@broadcast", "@newsletter", "@broadcast")
_SESSION_NAME = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_COMMAND_PREFIX = "!"
# A WhatsApp text message holds about 65k characters; stay well under it.
_MAX_TEXT = 16_000
_SENT_REMEMBERED = 500


class WhatsAppWebConnectionConfig(BridgeConnectionConfig):
    waha_url: str = Field(
        default="http://waha:3000",
        title="Sidecar address",
        description="Where the WAHA sidecar answers, as switch-core reaches it.",
    )
    api_key: str = Field(
        title="Sidecar API key",
        description="The WAHA_API_KEY the sidecar was started with.",
    )
    session: str = Field(
        default="default",
        title="Session name",
        description="The sidecar's session for this account. One account per session.",
    )
    chats: list[str] = Field(
        default_factory=list,
        title="Groups to bridge",
        description=(
            "Group names or ids (…@g.us) that become rooms. `*` bridges every "
            "group. Nothing else on the account reaches Switch."
        ),
    )
    direct_chats: bool = Field(
        default=False,
        title="Bridge direct chats",
        description="Also bridge one-to-one chats. Off by default.",
    )
    pairing_phone: str | None = Field(
        default=None,
        title="Phone number for a pairing code",
        description=(
            "International format, digits only. When set, linking shows an "
            "8-character code to type on the phone instead of a QR code."
        ),
    )
    min_seconds_between_posts: float = Field(
        default=3.0,
        ge=1.0,
        le=600.0,
        title="Seconds between posts in one chat",
    )
    max_posts_per_hour: int = Field(
        default=30,
        ge=1,
        le=500,
        title="Posts per hour, whole connection",
    )

    @field_validator("waha_url")
    @classmethod
    def _url(cls, value: str) -> str:
        parts = urlsplit(value)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise ValueError("the sidecar address must be an http(s) URL")
        return value.rstrip("/")

    @field_validator("session")
    @classmethod
    def _session(cls, value: str) -> str:
        if not _SESSION_NAME.match(value):
            raise ValueError("the session name may use letters, digits, _ and - only")
        return value

    @field_validator("pairing_phone")
    @classmethod
    def _phone(cls, value: str | None) -> str | None:
        if value is None or not value.strip():
            return None
        digits = re.sub(r"[\s+()-]", "", value)
        if not digits.isdigit() or not 7 <= len(digits) <= 15:
            raise ValueError("the phone number must be digits in international format")
        return digits

    @field_validator("chats")
    @classmethod
    def _chats(cls, value: list[str]) -> list[str]:
        return [c.strip() for c in value if c.strip()]


class PostingCapReached(BridgeOperationError):
    """The connection posted its hourly allowance; WhatsApp is not pushed past it."""


class Pacer:
    """Typing-paced posting: a minimum gap per chat and an hourly cap overall.

    The gap is waited out; the cap is refused, so a backlog never turns into a
    burst the moment the hour rolls over.
    """

    def __init__(
        self,
        *,
        min_gap: float,
        per_hour: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._min_gap = min_gap
        self._per_hour = per_hour
        self._clock = clock
        self._last: dict[str, float] = {}
        self._sent: deque[float] = deque()
        self._lock = asyncio.Lock()

    def wait_for(self, chat_id: str) -> float:
        """Seconds to wait before the next post in `chat_id`; raises at the cap."""
        now = self._clock()
        while self._sent and now - self._sent[0] >= 3600:
            self._sent.popleft()
        if len(self._sent) >= self._per_hour:
            raise PostingCapReached(
                f"This WhatsApp connection already posted {self._per_hour} times in the "
                "last hour, its limit. The post was not sent."
            )
        last = self._last.get(chat_id)
        return 0.0 if last is None else max(0.0, self._min_gap - (now - last))

    def record(self, chat_id: str) -> None:
        now = self._clock()
        self._last[chat_id] = now
        self._sent.append(now)

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock


def render_whatsapp(markdown: str) -> str:
    """Switch Markdown to WhatsApp's own markup.

    WhatsApp knows *bold*, _italic_, ~strike~, `code` and ```blocks```; links
    and headings are not rendered, so they become text a reader can use."""
    text = markdown
    # Fenced code stays as is: WhatsApp renders ``` blocks.
    parts = re.split(r"(```[\s\S]*?```)", text)
    out: list[str] = []
    for part in parts:
        if part.startswith("```"):
            out.append(part)
            continue
        p = part
        # Italics first (a lone *x*), so the bold this pass writes is not read as italics.
        p = re.sub(r"(?<![*\w])\*(?![\s*])([^*\n]+?)(?<![\s*])\*(?![*\w])", r"_\1_", p)
        p = re.sub(r"^#{1,6}\s+(.+)$", r"*\1*", p, flags=re.MULTILINE)
        p = re.sub(r"\[([^\]]+)\]\((https?://[^)\s]+)\)", r"\1 (\2)", p)
        p = re.sub(r"\*\*(.+?)\*\*", r"*\1*", p)
        p = re.sub(r"__(.+?)__", r"*\1*", p)
        p = re.sub(r"~~(.+?)~~", r"~\1~", p)
        out.append(p)
    return "".join(out)


def translate_whatsapp(text: str) -> str:
    """WhatsApp markup to Switch Markdown (bold and strike; the rest reads as is)."""
    t = re.sub(r"(?<![*\w])\*(?!\s)([^*\n]+?)(?<!\s)\*(?![*\w])", r"**\1**", text)
    return re.sub(r"(?<![~\w])~(?!\s)([^~\n]+?)(?<!\s)~(?![~\w])", r"~~\1~~", t)


def chat_of(payload: dict[str, Any]) -> str:
    """The chat a message belongs to: the recipient for our own, else the sender.

    A note to self ("Message yourself") arrives as our own message with no
    recipient, under the account's own id: the sender is then the chat."""
    if payload.get("fromMe"):
        return str(payload.get("to") or payload.get("from") or "")
    return str(payload.get("from") or "")


class WhatsAppWebAdapter(PlatformAdapter):
    display_name: ClassVar[str] = "WhatsApp (linked device)"
    docs_slug: ClassVar[str | None] = None
    keeps_quiet_unless_listening: ClassVar[bool] = True
    supports_channel_creation: ClassVar[bool] = False
    supports_directory_search: ClassVar[bool] = False
    renders_custom_url_schemes: ClassVar[bool] = False
    every_message_notifies_members: ClassVar[bool] = True

    def __init__(self, *, config: WhatsAppWebConnectionConfig) -> None:
        super().__init__()
        self.config = config
        self._http: httpx.AsyncClient | None = None
        self._events: asyncio.Task[None] | None = None
        self._status: str | None = None
        self._me: str | None = None
        # The account's own ids: its phone id (…@c.us) and its WhatsApp LID (…@lid).
        self._own: set[str] = set()
        self._pairing_code: str | None = None
        self._subjects: dict[str, str] = {}
        self._sent: deque[str] = deque(maxlen=_SENT_REMEMBERED)
        self._pacer = Pacer(
            min_gap=config.min_seconds_between_posts,
            per_hour=config.max_posts_per_hour,
        )
        self._capped_at: float | None = None
        self._on_command: Callable[[InboundCommand], Awaitable[None]] | None = None

    # ── Who we are, and what failures mean ────────────────────────────────

    @classmethod
    def classify_failure(cls, exc: BaseException) -> FailureReason | None:
        if isinstance(exc, httpx.HTTPStatusError):
            return (
                "auth_failed" if exc.response.status_code == 401 else "platform_error"
            )
        if isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, OSError)):
            return "network"
        return None

    @classmethod
    def outbound_urls(cls, connection_config: dict[str, object]) -> list[str]:
        url = connection_config.get("waha_url")
        return [str(url)] if url else []

    @classmethod
    async def verify_credentials(cls, connection_config: dict[str, object]) -> None:
        config = WhatsAppWebConnectionConfig.model_validate(connection_config)
        async with httpx.AsyncClient(base_url=config.waha_url, timeout=10) as client:
            try:
                resp = await client.get(
                    "/api/server/version", headers={"X-Api-Key": config.api_key}
                )
            except httpx.HTTPError as exc:
                raise BridgeCredentialError(
                    f"The sidecar at {config.waha_url} did not answer: {exc}"
                ) from exc
        if resp.status_code == 401:
            raise BridgeCredentialError("The sidecar refused the API key.")
        if resp.status_code != 200:
            raise BridgeCredentialError(
                f"The sidecar answered {resp.status_code} to a version check."
            )

    def _client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url=self.config.waha_url,
                headers={"X-Api-Key": self.config.api_key},
                timeout=httpx.Timeout(30.0, connect=10.0),
            )
        return self._http

    async def _call(self, method: str, path: str, **kwargs: Any) -> Any:
        resp = await self._client().request(method, path, **kwargs)
        if resp.status_code == 401:
            raise BridgeCredentialError("The WhatsApp sidecar refused the API key.")
        if resp.status_code >= 400:
            raise BridgeOperationError(
                f"WhatsApp sidecar {method} {path.split('?')[0]} answered {resp.status_code}: {resp.text[:300]}"
            )
        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError:
            return resp.text

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def start(
        self,
        on_message: Callable[[InboundMessage], Awaitable[None]],
        on_command: Callable[[InboundCommand], Awaitable[None]],
        on_agent_joined: Callable[[InboundAgentJoin], Awaitable[None]],
        on_user_joined: Callable[[InboundUserJoin], Awaitable[None]],
        on_app_joined: Callable[[InboundAppJoin], Awaitable[None]],
    ) -> None:
        self._on_message = on_message
        self._on_command = on_command
        if not self._listening:
            # Another pod holds the account: call the API only, hold no stream.
            return
        await self._ensure_session()
        self._events = asyncio.create_task(
            self._event_loop(), name="whatsapp-web-events"
        )

    async def stop(self) -> None:
        if self._events is not None:
            self._events.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._events
            self._events = None
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def _ensure_session(self) -> None:
        name = self.config.session
        resp = await self._client().get(f"/api/sessions/{name}")
        if resp.status_code == 401:
            raise BridgeCredentialError("The WhatsApp sidecar refused the API key.")
        if resp.status_code == 404:
            await self._call(
                "POST", "/api/sessions", json={"name": name, "start": True}
            )
            self._status = "STARTING"
            return
        resp.raise_for_status()
        info = resp.json()
        self._status = info.get("status")
        self._me = ((info.get("me") or {}).get("id")) or None
        if self._status == "WORKING":
            await self._learn_me()
        if self._status in ("STOPPED", "FAILED"):
            await self._call("POST", f"/api/sessions/{name}/start")
            self._status = "STARTING"

    def _events_url(self) -> str:
        base = urlsplit(self.config.waha_url)
        scheme = "wss" if base.scheme == "https" else "ws"
        query = urlencode(
            [
                ("session", self.config.session),
                ("events", "message.any"),
                ("events", "session.status"),
                ("x-api-key", self.config.api_key),
            ]
        )
        return f"{scheme}://{base.netloc}/ws?{query}"

    async def _event_loop(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(
                    self._events_url(), ping_interval=20, max_size=8 * 1024 * 1024
                ) as ws:
                    backoff = 1.0
                    logger.info(
                        "WhatsApp: listening to session %s", self.config.session
                    )
                    async for frame in ws:
                        try:
                            event = json.loads(frame)
                        except ValueError:
                            continue
                        try:
                            await self.handle_event(event)
                        except Exception:
                            logger.exception("WhatsApp: an event could not be handled")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # The URL carries the key: log the reason only.
                logger.warning(
                    "WhatsApp: event stream dropped (%s); again in %.0fs",
                    type(exc).__name__,
                    backoff,
                )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    async def handle_event(self, event: dict[str, Any]) -> None:
        kind = event.get("event")
        payload = event.get("payload") or {}
        if kind == "session.status":
            await self._on_status(event, payload)
        elif kind == "message.any":
            await self._on_message_event(payload)

    async def _on_status(self, event: dict[str, Any], payload: dict[str, Any]) -> None:
        status = payload.get("status")
        if status != self._status:
            logger.info("WhatsApp session %s is %s", self.config.session, status)
        self._status = status
        me = (event.get("me") or {}).get("id")
        if me:
            self._me = me
        if status == "WORKING":
            self._pairing_code = None
            await self._learn_me()
        elif (
            status == "SCAN_QR_CODE"
            and self.config.pairing_phone
            and not self._pairing_code
        ):
            try:
                out = await self._call(
                    "POST",
                    f"/api/{self.config.session}/auth/request-code",
                    json={"phoneNumber": self.config.pairing_phone},
                )
                self._pairing_code = (out or {}).get("code")
            except BridgeOperationError as exc:
                logger.warning("WhatsApp: no pairing code: %s", exc)

    async def _learn_me(self) -> None:
        """Who the linked account is: its phone id and its LID (a note to self uses either)."""
        try:
            me = await self._call("GET", f"/api/sessions/{self.config.session}/me")
        except (BridgeOperationError, httpx.HTTPError) as exc:
            logger.warning("WhatsApp: could not read the linked account: %s", exc)
            return
        if isinstance(me, dict):
            self._me = me.get("id") or self._me
            self._own = {
                str(v).split("@", 1)[0] for v in (me.get("id"), me.get("lid")) if v
            }

    async def attention(self) -> str | None:
        if self._capped_at is not None and time.monotonic() - self._capped_at < 3600:
            return (
                f"WhatsApp posting reached its limit of {self.config.max_posts_per_hour} posts an "
                "hour; posts are refused until the hour passes."
            )
        if self._status == "SCAN_QR_CODE":
            if self._pairing_code:
                return (
                    f"WhatsApp is not linked yet. On the phone: Linked devices, Link with phone "
                    f"number instead, then type {self._pairing_code}."
                )
            return (
                "WhatsApp is not linked yet. Scan the sidecar's QR code with the phone "
                f"(Linked devices): GET /api/{self.config.session}/auth/qr on the sidecar."
            )
        if self._status == "FAILED":
            return "The WhatsApp session failed. Restart it on the sidecar, or link the phone again."
        return None

    # ── Inbound ───────────────────────────────────────────────────────────

    def _is_own_echo(self, payload: dict[str, Any]) -> bool:
        if not payload.get("fromMe"):
            return False
        return payload.get("source") == "api" or str(payload.get("id")) in self._sent

    async def _subject(self, chat_id: str) -> str | None:
        if chat_id in self._subjects:
            return self._subjects[chat_id]
        try:
            info = await self._call(
                "GET", f"/api/{self.config.session}/groups/{quote(chat_id, safe='@.')}"
            )
        except BridgeOperationError:
            return None
        subject = (info or {}).get("subject") if isinstance(info, dict) else None
        if subject:
            self._subjects[chat_id] = str(subject)
        return subject

    async def bridges(self, chat_id: str) -> bool:
        """Whether this chat is one the connection was told to bridge."""
        if not chat_id or any(chat_id == n or chat_id.endswith(n) for n in _NEVER):
            return False
        if chat_id.endswith(_GROUP_SUFFIX):
            wanted = self.config.chats
            if "*" in wanted or chat_id in wanted:
                return True
            if not wanted:
                return False
            subject = await self._subject(chat_id)
            return bool(subject) and subject.casefold() in {
                w.casefold() for w in wanted
            }
        if not chat_id.endswith(_PERSON_SUFFIXES):
            return False
        # A direct chat named in `chats` is bridged even with direct chats off;
        # `me` names the account's own "Message yourself" chat.
        number = chat_id.split("@", 1)[0]
        named = {
            c.split("@", 1)[0]
            for c in self.config.chats
            if not c.endswith(_GROUP_SUFFIX)
        }
        if number in named:
            return True
        if "me" in {c.casefold() for c in self.config.chats}:
            own = self._own or ({self._me.split("@", 1)[0]} if self._me else set())
            if number in own:
                return True
        return self.config.direct_chats

    @staticmethod
    def _sender(payload: dict[str, Any], me: str | None) -> tuple[str, str]:
        raw = payload.get("_data") or {}
        info = raw.get("Info") if isinstance(raw, dict) else None
        push = (info or {}).get("PushName") if isinstance(info, dict) else None
        if payload.get("fromMe"):
            sender = me or "me"
        else:
            sender = str(
                payload.get("participant")
                or payload.get("author")
                or payload.get("from")
                or ""
            )
        name = push or sender.split("@", 1)[0]
        return sender, str(name)

    async def _on_message_event(self, payload: dict[str, Any]) -> None:
        chat_id = chat_of(payload)
        if self._is_own_echo(payload) or not await self.bridges(chat_id):
            return
        group = chat_id.endswith(_GROUP_SUFFIX)
        channel_type: ChannelType = "group" if group else "direct"
        channel_name = await self._subject(chat_id) if group else None
        sender_id, sender_name = self._sender(payload, self._me)
        content = translate_whatsapp(str(payload.get("body") or "")).strip()
        message_ref = str(payload.get("id"))
        reply = payload.get("replyTo") or {}
        root_id = (
            str(reply["id"]) if isinstance(reply, dict) and reply.get("id") else None
        )

        if content.startswith(_COMMAND_PREFIX) and self._on_command is not None:
            parts = content[1:].split(None, 1)
            if parts and parts[0]:
                await self._on_command(
                    InboundCommand(
                        channel_id=chat_id,
                        channel_type=channel_type,
                        sender_id=sender_id,
                        sender_name=sender_name,
                        command=parts[0],
                        args=parts[1].strip() if len(parts) > 1 else "",
                        message_ref=message_ref,
                        root_id=root_id,
                        channel_name=channel_name,
                    )
                )
                return

        attachments, failures = await self._fetch_media(payload)
        if not content and not attachments and not failures:
            return
        if self._on_message is None:
            return
        await self._on_message(
            InboundMessage(
                channel_id=chat_id,
                channel_type=channel_type,
                sender_id=sender_id,
                sender_name=sender_name,
                content=content,
                message_ref=message_ref,
                root_id=root_id,
                channel_name=channel_name,
                attachments=attachments,
                attachment_failures=failures,
            )
        )

    async def _fetch_media(
        self, payload: dict[str, Any]
    ) -> tuple[list[Attachment], list[AttachmentFailure]]:
        if not payload.get("hasMedia"):
            return [], []
        media = payload.get("media") or {}
        filename = str(media.get("filename") or "attachment")
        mimetype = str(media.get("mimetype") or "application/octet-stream")
        url = media.get("url")
        if not url:
            reason = (
                (media.get("error") or {}).get("message")
                if isinstance(media.get("error"), dict)
                else None
            )
            return [], [
                AttachmentFailure(
                    filename=filename,
                    reason=reason or "the sidecar did not download it",
                )
            ]
        # The sidecar names its own address; fetch it from where switch-core reaches the sidecar.
        path = urlsplit(str(url))
        local = f"{path.path}?{path.query}" if path.query else path.path
        try:
            resp = await self._client().get(local)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            return [], [
                AttachmentFailure(
                    filename=filename, reason=f"download failed ({type(exc).__name__})"
                )
            ]
        if len(resp.content) > self._max_attachment_bytes:
            return [], [
                AttachmentFailure(
                    filename=filename, reason="larger than Switch accepts"
                )
            ]
        return [Attachment(filename=filename, mimetype=mimetype, data=resp.content)], []

    def translate_inbound(self, raw: Any) -> str:
        return translate_whatsapp(str(raw))

    # ── Outbound ──────────────────────────────────────────────────────────

    def _render_outbound(self, content: str) -> str:
        return render_whatsapp(content)

    async def _label(self, sender_name: str) -> str:
        try:
            rendering = await self.agent_rendering(sender_name)
            return rendering.field_label or sender_name
        except Exception:
            return sender_name

    def _attribute(self, label: str, body: str) -> str:
        return f"*{label}:* {body}" if body else f"*{label}*"

    async def _paced(self, chat_id: str, send: Callable[[], Awaitable[Any]]) -> Any:
        """Typing, a short human pause and the per-chat gap, then the send."""
        async with self._pacer.lock:
            try:
                wait = self._pacer.wait_for(chat_id)
            except PostingCapReached:
                self._capped_at = time.monotonic()
                raise
            if wait:
                await asyncio.sleep(wait)
            with contextlib.suppress(Exception):
                await self._call(
                    "POST",
                    "/api/startTyping",
                    json={"session": self.config.session, "chatId": chat_id},
                )
            await asyncio.sleep(random.uniform(0.8, 1.8))
            try:
                out = await send()
            finally:
                with contextlib.suppress(Exception):
                    await self._call(
                        "POST",
                        "/api/stopTyping",
                        json={"session": self.config.session, "chatId": chat_id},
                    )
            self._pacer.record(chat_id)
            return out

    def _remember(self, out: Any) -> str | None:
        ref = (out or {}).get("id") if isinstance(out, dict) else None
        if isinstance(ref, dict):
            ref = ref.get("_serialized") or ref.get("id")
        if ref:
            self._sent.append(str(ref))
        return str(ref) if ref else None

    async def send_message(
        self,
        channel_id: str,
        sender_name: str,
        content: str,
        thread_root_id: str | None = None,
        *,
        room_wide_mention: bool = False,
    ) -> str | None:
        label = await self._label(sender_name)
        text = self._attribute(label, self._render_outbound(content))[:_MAX_TEXT]
        body: dict[str, Any] = {
            "session": self.config.session,
            "chatId": channel_id,
            "text": text,
        }
        if thread_root_id:
            body["reply_to"] = thread_root_id
        out = await self._paced(
            channel_id, lambda: self._call("POST", "/api/sendText", json=body)
        )
        return self._remember(out)

    async def edit_agent_message(
        self, channel_id: str, message_ref: str, sender_name: str, content: str
    ) -> None:
        label = await self._label(sender_name)
        await self.update_message(
            channel_id,
            message_ref,
            self._attribute(label, self._render_outbound(content)),
        )

    async def update_message(
        self, channel_id: str, message_ref: str, new_content: str
    ) -> None:
        await self._call(
            "PUT",
            f"/api/{self.config.session}/chats/{quote(channel_id, safe='@.')}/messages/{quote(message_ref, safe='@._')}",
            json={"text": new_content[:_MAX_TEXT]},
        )

    async def delete_message(self, channel_id: str, message_ref: str) -> None:
        await self._call(
            "DELETE",
            f"/api/{self.config.session}/chats/{quote(channel_id, safe='@.')}/messages/{quote(message_ref, safe='@._')}",
        )

    async def send_attachment(
        self,
        channel_id: str,
        sender_name: str,
        filename: str,
        mimetype: str,
        data: bytes,
        caption: str | None = None,
        thread_root_id: str | None = None,
    ) -> str | None:
        label = await self._label(sender_name)
        image = mimetype.startswith("image/") and mimetype != "image/svg+xml"
        body: dict[str, Any] = {
            "session": self.config.session,
            "chatId": channel_id,
            "file": {
                "mimetype": mimetype,
                "filename": filename,
                "data": base64.b64encode(data).decode(),
            },
            "caption": self._attribute(label, self._render_outbound(caption or "")),
        }
        if thread_root_id:
            body["reply_to"] = thread_root_id
        route = "/api/sendImage" if image else "/api/sendFile"
        out = await self._paced(
            channel_id, lambda: self._call("POST", route, json=body)
        )
        return self._remember(out)

    async def send_typing(
        self, channel_id: str, sender_name: str, is_typing: bool
    ) -> None:
        if not await self.bridges(channel_id):
            return
        with contextlib.suppress(BridgeOperationError, httpx.HTTPError):
            await self._call(
                "POST",
                "/api/startTyping" if is_typing else "/api/stopTyping",
                json={"session": self.config.session, "chatId": channel_id},
            )

    # ── Channels and identities: WhatsApp's own ───────────────────────────

    async def create_channel(
        self, name: str, topic: str, *, channel_type: ChannelType = "channel_public"
    ) -> str:
        raise BridgeOperationError(
            "WhatsApp groups are made in WhatsApp. Create the group on the phone and name it in this connection's groups."
        )

    async def get_channel_type(self, channel_id: str) -> ChannelType:
        return "group" if channel_id.endswith(_GROUP_SUFFIX) else "direct"

    async def add_agents_to_channel(
        self, channel_id: str, agent_names: list[str]
    ) -> None:
        return None

    async def add_users_to_channel(
        self,
        channel_id: str,
        user_names: list[str],
        user_external_ids: list[str],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        return None

    async def get_channel_agent_names(self, channel_id: str) -> list[str]:
        return []

    async def create_agent_identity(self, agent: AgentIdentity) -> None:
        # One account posts for every agent; the name goes at the head of each post.
        return None

    async def remove_agent_identity(self, agent: AgentIdentity) -> None:
        return None

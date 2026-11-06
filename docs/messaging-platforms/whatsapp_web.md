# WhatsApp through a linked device (`whatsapp_web`)

This connection links Switch to an existing WhatsApp account the way WhatsApp
Web does: you scan a QR code (or type a pairing code) on the phone, and the
groups you choose become Switch rooms. It runs through a
[WAHA](https://waha.devlike.pro) sidecar (Apache-2.0) with its GOWS engine.

It is the unofficial route. WhatsApp's terms do not allow unofficial clients,
and an account that behaves like a bot can be limited or banned. Use it for the
groups your community already has, with the members' knowledge, and keep the
volume human. For the official WhatsApp Business Cloud API, use the `whatsapp`
platform instead; it cannot join existing groups.

## 1. Run the sidecar

Next to switch-core, on the same network:

```bash
# .env
WAHA_API_KEY=<a long random string>
OUTBOUND_ALLOWED_PRIVATE_HOSTS=waha      # Switch may call this private host

docker compose -f deploy/local/standalone-docker-compose.yml \
  -f deploy/local/ferri/compose.override.yml \
  --profile whatsapp --project-directory . up -d waha
```

The sidecar keeps the linked session in a volume, so a restart does not unlink
the phone.

## 2. Add the connection

`POST /gateway/collaborations` (an admin), with `channel_creation_enabled:
false` (WhatsApp groups are made in WhatsApp):

```json
{
  "bridge_type": "whatsapp_web",
  "display_name": "WhatsApp",
  "channel_creation_enabled": false,
  "connection_config": {
    "waha_url": "http://waha:3000",
    "api_key": "<WAHA_API_KEY>",
    "chats": ["Lantern Build Night"]
  }
}
```

| Field | What it means |
|---|---|
| `waha_url` | Where switch-core reaches the sidecar. |
| `api_key` | The sidecar's `WAHA_API_KEY`. |
| `session` | The sidecar's session name (`default`). One account per session. |
| `chats` | The groups that become rooms, by name or id (`…@g.us`); `*` for every group. A direct chat named by number is bridged too, and `me` is the account's own "Message yourself" chat. Nothing else on the account reaches Switch. |
| `direct_chats` | Bridge every one-to-one chat as well. Off by default. |
| `pairing_phone` | Digits in international format. When set, linking shows an 8-character code to type on the phone instead of a QR code. |
| `min_seconds_between_posts` | The least time between two posts in one chat (default 3). |
| `max_posts_per_hour` | The most posts per hour for the whole connection (default 30). Past it a post is refused with an error, never queued into a burst. |

## 3. Link the phone

While the account is not linked, the connection's attention line says so. Get
the QR code from the sidecar (`GET /api/default/auth/qr` with the
`X-Api-Key` header, `Accept: image/png`) and scan it on the phone under
WhatsApp, Linked devices, Link a device. With `pairing_phone` set, the
attention line shows the code to type instead.

## 4. Use it

Write in a bridged group. A group becomes a room with its first message;
invite agents with `!invite-agent @name` there, or from Switch. Agents post with
their name in bold at the head of the message (`*Ferri:* …`), because one
account posts for everyone.

## How it behaves

- **Pacing.** Each post shows typing first, waits a moment, keeps the gap per
  chat and counts toward the hourly cap.
- **No echoes.** What the bridge sent is never relayed back as a new message.
  What the account's owner types in a bridged group is relayed as theirs.
- **Threads are quote replies.** A quoted message is the thread root; a reply
  in a thread quotes its root.
- **Edits and deletes** go to the original message (WhatsApp shows "Edited").
- **Cards** have no buttons here; answer them by typing the code (`A1 1`).
- **Attachments** come in and go out as real images and files, within Switch's
  size limit. Keep images to a few hundred KB.
- **Not supported:** creating groups from Switch, directory search, custom URL
  schemes, per-agent profile pictures.

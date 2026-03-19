"""
Real-time Telegram listener.
Uses Telethon event handlers for instant message detection.
New messages → Supabase → triggers AI processing → website updates via SSE.

Run: python realtime.py
"""
import asyncio
import json
import hashlib
import os
import urllib.request
from datetime import datetime, timezone
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

API_ID = int(os.environ.get("TELEGRAM_API_ID", "35088062"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "7198226149f0bca3a11dad88e203af78")
SESSION_FILE = os.environ.get("SESSION_FILE", "backfill_session")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://tifrdeaitgrwylxjlqdq.supabase.co")
SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRpZnJkZWFpdGdyd3lseGpscWRxIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3Mzg1MDAzOSwiZXhwIjoyMDg5NDI2MDM5fQ.ovLkA9L5igHGXFG2XMoCzrDtduL54GYthq0Y-YSvtp4")
CRON_SECRET = os.environ.get("CRON_SECRET", "iran-tracker-cron-secret-change-me")
CRON_URL = os.environ.get("CRON_URL", "https://irantracker.netlify.app/api/cron/ingest")

# Channels to monitor — add any channel username here
# Farsi/Arabic channels work — AI translates automatically
CHANNELS = [
    "FotrosResistancee",
    "SimurghRes",
    "Middle_East_Spectator",
    # Add more:
    # "Aborejhaan",
    # "selovelove",
]

MEDIA_DIR = "media"
# Buffer: collect messages for a few seconds before triggering AI
# This allows cross-referencing when multiple channels report the same event
BUFFER_SECONDS = 10
message_buffer: list = []
buffer_task: asyncio.Task | None = None


def hash_msg(text: str, channel: str, msg_id: int) -> str:
    key = f"{channel}:{msg_id}:{text.strip().lower()}"
    return hashlib.sha256(key.encode()).hexdigest()


def supabase_post(path: str, data: list | dict) -> tuple[int, str]:
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:200]


def trigger_cron() -> dict:
    try:
        req = urllib.request.Request(
            CRON_URL,
            headers={"Authorization": f"Bearer {CRON_SECRET}"},
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        return {"error": str(e)}


async def upload_photo(client, msg, channel: str) -> str | None:
    os.makedirs(MEDIA_DIR, exist_ok=True)
    filename = f"{channel}_{msg.id}.jpg"
    filepath = os.path.join(MEDIA_DIR, filename)
    try:
        if not os.path.exists(filepath):
            await client.download_media(msg, filepath)
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            with open(filepath, "rb") as f:
                data = f.read()
            url = f"{SUPABASE_URL}/storage/v1/object/incident-media/{filename}"
            headers = {
                "apikey": SERVICE_KEY,
                "Authorization": f"Bearer {SERVICE_KEY}",
                "Content-Type": "image/jpeg",
                "x-upsert": "true",
            }
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req) as resp:
                if resp.status in (200, 201):
                    return f"{SUPABASE_URL}/storage/v1/object/public/incident-media/{filename}"
    except Exception:
        pass
    return None


async def process_buffer():
    """Wait for buffer to fill, then process all at once."""
    global message_buffer
    await asyncio.sleep(BUFFER_SECONDS)

    if not message_buffer:
        return

    msgs = message_buffer.copy()
    message_buffer = []

    ts = datetime.now().strftime("%H:%M:%S")
    channels = set(m["channel"] for m in msgs)
    print(f"[{ts}] Processing {len(msgs)} new messages from {', '.join(channels)}")

    # Store all in Supabase
    rows = []
    for m in msgs:
        rows.append({
            "channel": m["channel"],
            "message_hash": m["hash"],
            "message_text": m["text"][:10000],
            "processed": False,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "media_urls": m["media"],
            "message_date": m["date"],
        })

    # Insert in one batch
    status, body = supabase_post("telegram_messages", rows)
    if status not in (200, 201):
        print(f"  DB insert error: {status}")
        return

    # Trigger AI processing
    result = trigger_cron()
    incidents = result.get("new_incidents", 0)
    if incidents > 0:
        print(f"[{ts}] >>> {incidents} NEW INCIDENTS CREATED <<<")
    elif "error" in result:
        print(f"[{ts}] Cron error: {result['error'][:80]}")
    else:
        print(f"[{ts}] No actionable incidents from these messages")


async def main():
    global buffer_task

    # Use StringSession for cloud deployment, fall back to file session locally
    session_string = os.environ.get("TELEGRAM_SESSION", "")
    session = StringSession(session_string) if session_string else SESSION_FILE
    client = TelegramClient(session, API_ID, API_HASH)
    await client.start()
    print("=" * 50)
    print("  IRAN TRACKER — Real-time Telegram Listener")
    print("=" * 50)
    print(f"  Channels: {len(CHANNELS)}")
    for ch in CHANNELS:
        print(f"    - {ch}")
    print(f"  Buffer: {BUFFER_SECONDS}s (collects messages before AI processing)")
    print(f"  Cron: {CRON_URL}")
    print("=" * 50)
    print()

    # Resolve channel entities
    channel_entities = []
    for ch in CHANNELS:
        try:
            entity = await client.get_entity(ch)
            channel_entities.append(entity)
            print(f"  Listening to: {ch} (ID: {entity.id})")
        except Exception as e:
            print(f"  Failed to resolve {ch}: {e}")

    if not channel_entities:
        print("No channels resolved. Exiting.")
        return

    @client.on(events.NewMessage(chats=channel_entities))
    async def handler(event):
        global buffer_task

        msg = event.message
        channel_name = ""
        for ch in CHANNELS:
            try:
                entity = await client.get_entity(ch)
                if entity.id == event.chat_id:
                    channel_name = ch
                    break
            except:
                pass

        if not channel_name:
            channel_name = str(event.chat_id)

        text = msg.text or ""
        if len(text) < 3 and not msg.media:
            return

        # Handle media
        media_urls = []
        if msg.media and isinstance(msg.media, MessageMediaPhoto):
            photo_url = await upload_photo(client, msg, channel_name)
            if photo_url:
                media_urls.append({"type": "image", "url": photo_url})
        elif msg.media and isinstance(msg.media, MessageMediaDocument):
            doc = msg.media.document
            if doc and "video" in (doc.mime_type or ""):
                media_urls.append({"type": "video", "url": f"tg://file/{doc.id}"})

        h = hash_msg(text or "[Media]", channel_name, msg.id)

        message_buffer.append({
            "channel": channel_name,
            "hash": h,
            "text": text or "[Media]",
            "media": media_urls,
            "date": msg.date.isoformat() if msg.date else None,
        })

        ts = datetime.now().strftime("%H:%M:%S")
        preview = (text or "[Media]")[:60].replace("\n", " ")
        print(f"[{ts}] [{channel_name}] {preview}")

        # Reset buffer timer — wait for more messages before processing
        if buffer_task and not buffer_task.done():
            buffer_task.cancel()
        buffer_task = asyncio.create_task(process_buffer())

    print("\nListening for new messages... (Ctrl+C to stop)\n")
    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")

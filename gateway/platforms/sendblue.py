"""SendBlue (iMessage) platform adapter.

Connects to the SendBlue REST API for outbound iMessage and runs an aiohttp
webhook server to receive inbound messages forwarded by SendBlue.

Env vars:
  - SENDBLUE_API_KEY        (sb-api-key-id header)
  - SENDBLUE_API_SECRET     (sb-api-secret-key header)
  - SENDBLUE_FROM_NUMBER    (E.164 from-number, e.g. +14025551234)

Gateway-specific env vars:
  - SENDBLUE_WEBHOOK_PORT     (default 8646)
  - SENDBLUE_ALLOWED_USERS    (comma-separated E.164 phone numbers)
  - SENDBLUE_ALLOW_ALL_USERS  (true/false)
  - SENDBLUE_HOME_CHANNEL     (phone number for cron delivery)
"""

import asyncio
import collections
import json
import logging
import mimetypes
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_image_from_url,
    cache_audio_from_url,
    cache_document_from_bytes,
)

logger = logging.getLogger(__name__)

SB_API_BASE = "https://api.sendblue.com"
MAX_IMESSAGE_LENGTH = 8000  # iMessage is generous; keep practical limit
DEFAULT_WEBHOOK_PORT = 8646
TYPING_INTERVAL_SECS = 12
TYPING_MAX_SECS = 600  # safety cap (10 min)
MULTI_BUBBLE_DELAY = 1.2  # seconds between bubbles for natural feel

# E.164 phone number pattern for redaction
_PHONE_RE = re.compile(r"\+[1-9]\d{6,14}")

# Recent inbound message cache: keyed by chat_id -> {message_handle, payload, timestamp}
# Used by send_reaction() so the agent can react to the latest message without
# needing to track message_handles explicitly.
_RECENT_INBOUND_TTL = 3600  # 1 hour
_recent_inbound: Dict[str, dict] = {}


def remember_recent_inbound(chat_id: str, message_handle: str, payload: dict) -> None:
    """Cache the most recent inbound message for a chat (for tapback reactions)."""
    _recent_inbound[chat_id] = {
        "message_handle": message_handle,
        "payload": payload,
        "ts": time.monotonic(),
    }
    # Prune stale entries
    cutoff = time.monotonic() - _RECENT_INBOUND_TTL
    stale = [k for k, v in _recent_inbound.items() if v["ts"] < cutoff]
    for k in stale:
        _recent_inbound.pop(k, None)


def get_recent_inbound(chat_id: str) -> Optional[dict]:
    """Get the most recent inbound message for a chat, or None if expired."""
    entry = _recent_inbound.get(chat_id)
    if entry and (time.monotonic() - entry["ts"]) < _RECENT_INBOUND_TTL:
        return entry
    return None


# Media type detection from URLs/filenames
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic", ".heif", ".tiff"}
_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
_AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".ogg", ".wav", ".caf"}


def _guess_media_type(url: str) -> MessageType:
    """Guess MessageType from a URL or filename."""
    path = urlparse(url).path if url.startswith("http") else url
    ext = Path(path).suffix.lower()
    if ext in _IMAGE_EXTS:
        return MessageType.PHOTO
    if ext in _VIDEO_EXTS:
        return MessageType.VIDEO
    if ext in _AUDIO_EXTS:
        return MessageType.VOICE
    return MessageType.DOCUMENT


def _redact_phone(phone: str) -> str:
    """Redact a phone number for logging: +15551234567 -> +1555***4567."""
    if not phone:
        return "<none>"
    if len(phone) <= 8:
        return phone[:2] + "***" + phone[-2:] if len(phone) > 4 else "****"
    return phone[:5] + "***" + phone[-4:]


def check_sendblue_requirements() -> bool:
    """Check if SendBlue adapter dependencies are available."""
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        return False
    return bool(os.getenv("SENDBLUE_API_KEY") and os.getenv("SENDBLUE_API_SECRET"))


class SendBlueAdapter(BasePlatformAdapter):
    """
    SendBlue iMessage <-> Hermes gateway adapter.

    Each inbound phone number gets its own Hermes session (multi-tenant).
    Replies are sent from the configured SENDBLUE_FROM_NUMBER via the
    SendBlue REST API, which delivers them as iMessages.

    Features ported from the standalone sendblue-proxy:
    - Automatic read receipts on inbound messages
    - Typing indicator loop (async) while processing
    - Outbound echo filtering (is_outbound messages dropped)
    - Multi-bubble splitting (content separated by '||')
    - Tapback/reaction support via send_reaction()
    """

    MAX_MESSAGE_LENGTH = MAX_IMESSAGE_LENGTH

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.SENDBLUE)
        self._api_key: str = os.environ["SENDBLUE_API_KEY"]
        self._api_secret: str = os.environ["SENDBLUE_API_SECRET"]
        self._from_number: str = os.getenv("SENDBLUE_FROM_NUMBER", "")
        self._webhook_port: int = int(
            os.getenv("SENDBLUE_WEBHOOK_PORT", str(DEFAULT_WEBHOOK_PORT))
        )
        self._webhook_secret: str = os.getenv("SENDBLUE_WEBHOOK_SECRET", "")
        self._runner = None
        self._http_session: Optional["aiohttp.ClientSession"] = None
        # Active typing indicator tasks keyed by recipient number
        self._typing_tasks: Dict[str, asyncio.Task] = {}

    def _sb_headers(self) -> Dict[str, str]:
        """Build SendBlue API auth headers."""
        return {
            "sb-api-key-id": self._api_key,
            "sb-api-secret-key": self._api_secret,
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # SendBlue API helpers
    # ------------------------------------------------------------------

    async def _sb_post(self, path: str, body: dict) -> Optional[dict]:
        """POST to SendBlue API. Returns parsed JSON or None on failure."""
        if not self._http_session:
            return None
        url = f"{SB_API_BASE}{path}"
        try:
            async with self._http_session.post(
                url, json=body, headers=self._sb_headers()
            ) as resp:
                result = await resp.json()
                if resp.status >= 300:
                    logger.warning(
                        "[sendblue] %s -> HTTP %s: %s",
                        path, resp.status, str(result)[:200],
                    )
                    return None
                return result
        except Exception as e:
            logger.warning("[sendblue] %s failed: %s", path, e)
            return None

    async def _mark_read(self, message_handle: str, number: str) -> None:
        """Mark an inbound message as read."""
        if message_handle and number:
            await self._sb_post("/api/mark-read", {
                "message_handle": message_handle,
                "number": number,
                "from_number": self._from_number,
            })

    async def _send_typing_indicator(self, number: str) -> None:
        """Send a single typing indicator."""
        if number:
            await self._sb_post("/api/send-typing-indicator", {
                "number": number,
                "from_number": self._from_number,
            })

    async def _typing_loop(self, number: str) -> None:
        """Continuously send typing indicators until cancelled."""
        try:
            elapsed = 0.0
            while elapsed < TYPING_MAX_SECS:
                await self._send_typing_indicator(number)
                await asyncio.sleep(TYPING_INTERVAL_SECS)
                elapsed += TYPING_INTERVAL_SECS
            logger.warning(
                "[sendblue] typing loop hit max for %s", _redact_phone(number)
            )
        except asyncio.CancelledError:
            pass  # Normal: cancelled when response is sent

    def _start_typing(self, number: str) -> None:
        """Start a typing indicator loop for a recipient."""
        self._stop_typing(number)
        loop = asyncio.get_event_loop()
        task = loop.create_task(self._typing_loop(number))
        self._typing_tasks[number] = task

    def _stop_typing(self, number: str) -> None:
        """Stop the typing indicator loop for a recipient."""
        task = self._typing_tasks.pop(number, None)
        if task and not task.done():
            task.cancel()

    # ------------------------------------------------------------------
    # Required abstract methods
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        import aiohttp
        from aiohttp import web

        if not self._from_number:
            logger.error(
                "[sendblue] SENDBLUE_FROM_NUMBER not set — cannot send replies"
            )
            return False

        app = web.Application()
        app.router.add_post("/webhooks/sendblue", self._handle_webhook)
        # Accept the old webhook path so SendBlue dashboard doesn't need updating
        app.router.add_post(
            "/webhooks/sendblue-ac0660715352608f697448b6d669b842",
            self._handle_webhook,
        )
        app.router.add_post("/", self._handle_webhook)
        app.router.add_get("/health", lambda _: web.Response(text="ok"))

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self._webhook_port)
        await site.start()
        self._http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
        )
        self._running = True

        logger.info(
            "[sendblue] iMessage webhook server listening on port %d, from: %s",
            self._webhook_port,
            _redact_phone(self._from_number),
        )
        return True

    async def disconnect(self) -> None:
        for number in list(self._typing_tasks):
            self._stop_typing(number)
        if self._http_session:
            await self._http_session.close()
            self._http_session = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        self._running = False
        logger.info("[sendblue] Disconnected")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an iMessage via SendBlue.

        Supports multi-bubble splitting: if the formatted content contains '||',
        each segment is sent as a separate iMessage bubble with a natural delay.
        Otherwise, falls back to standard truncation for long messages.
        """
        formatted = self.format_message(content)

        # Multi-bubble split (agent can use || to send multiple bubbles)
        if "||" in formatted:
            bubbles = [b.strip() for b in formatted.split("||") if b.strip()][:5]
        else:
            bubbles = self.truncate_message(formatted)

        last_result = SendResult(success=True)

        for i, bubble in enumerate(bubbles):
            if i > 0:
                await asyncio.sleep(MULTI_BUBBLE_DELAY)

            result = await self._sb_post("/api/send-message", {
                "number": chat_id,
                "from_number": self._from_number,
                "content": bubble,
            })

            if result is None:
                self._stop_typing(chat_id)
                return SendResult(
                    success=False,
                    error="SendBlue API request failed",
                )

            msg_status = result.get("status", "")
            if msg_status in ("ERROR", "FAILED"):
                self._stop_typing(chat_id)
                error_msg = result.get("error_message", str(result))
                logger.error(
                    "[sendblue] send failed to %s: %s",
                    _redact_phone(chat_id), error_msg,
                )
                return SendResult(success=False, error=f"SendBlue: {error_msg}")

            last_result = SendResult(
                success=True,
                message_id=result.get("message_handle", ""),
            )

        # Stop typing after all bubbles sent
        self._stop_typing(chat_id)
        return last_result

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Send a typing indicator to the chat."""
        await self._send_typing_indicator(chat_id)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "dm"}

    # ------------------------------------------------------------------
    # SendBlue-specific: reactions/tapbacks
    # ------------------------------------------------------------------

    async def send_reaction(
        self,
        message_handle: str,
        reaction: str,
        chat_id: Optional[str] = None,
    ) -> Optional[dict]:
        """Send a tapback reaction to a specific message.

        If message_handle is empty and chat_id is provided, reacts to the
        most recent inbound message in that chat (from the recent cache).

        Valid reactions: love, like, dislike, laugh, emphasize, question
        """
        if not message_handle and chat_id:
            recent = get_recent_inbound(chat_id)
            if recent:
                message_handle = recent["message_handle"]
                logger.info(
                    "[sendblue] using cached message_handle for reaction: %s",
                    message_handle[:20],
                )
        if not message_handle:
            logger.warning("[sendblue] send_reaction: no message_handle available")
            return None
        return await self._sb_post("/api/send-reaction", {
            "from_number": self._from_number,
            "message_handle": message_handle,
            "reaction": reaction,
        })

    # ------------------------------------------------------------------
    # iMessage-specific formatting
    # ------------------------------------------------------------------

    def format_message(self, content: str) -> str:
        """Strip markdown — iMessage renders it as literal characters."""
        content = re.sub(r"\*\*(.+?)\*\*", r"\1", content, flags=re.DOTALL)
        content = re.sub(r"\*(.+?)\*", r"\1", content, flags=re.DOTALL)
        content = re.sub(r"__(.+?)__", r"\1", content, flags=re.DOTALL)
        content = re.sub(r"_(.+?)_", r"\1", content, flags=re.DOTALL)
        content = re.sub(r"```[a-z]*\n?", "", content)
        content = re.sub(r"`(.+?)`", r"\1", content)
        content = re.sub(r"^#{1,6}\s+", "", content, flags=re.MULTILINE)
        content = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", content)
        content = re.sub(r"\n{3,}", "\n\n", content)
        return content.strip()

    # ------------------------------------------------------------------
    # SendBlue webhook handler
    # ------------------------------------------------------------------

    async def _handle_webhook(self, request) -> "aiohttp.web.Response":
        from aiohttp import web
        import hmac

        # Verify webhook secret if configured (SendBlue sends it in sb-signing-secret header)
        if self._webhook_secret:
            provided = request.headers.get("sb-signing-secret", "")
            if not hmac.compare_digest(provided, self._webhook_secret):
                logger.warning("[sendblue] webhook rejected: invalid secret")
                return web.json_response({"error": "unauthorized"}, status=401)

        try:
            payload = await request.json()
        except Exception as e:
            logger.error("[sendblue] webhook parse error: %s", e)
            return web.json_response({"error": "invalid json"}, status=400)

        # --- Echo filter: drop outbound messages (our own replies) ---
        if payload.get("is_outbound"):
            recipient = (
                payload.get("number")
                or payload.get("to_number")
                or ""
            )
            if recipient:
                self._stop_typing(recipient)
            logger.debug(
                "[sendblue] outbound echo to %s — typing stopped",
                _redact_phone(recipient),
            )
            return web.json_response({"ok": True})

        # --- Inbound message ---
        from_number = payload.get("from_number", "").strip()
        content = payload.get("content", "").strip()
        message_handle = payload.get("message_handle", "")
        media_url = payload.get("media_url", "").strip()

        if not from_number or (not content and not media_url):
            return web.json_response({"ok": True})

        # Ignore messages from our own number
        if from_number == self._from_number:
            logger.debug(
                "[sendblue] ignoring message from own number %s",
                _redact_phone(from_number),
            )
            return web.json_response({"ok": True})

        logger.info(
            "[sendblue] inbound from %s: %s%s",
            _redact_phone(from_number),
            content[:80],
            f" [media: {media_url[:60]}]" if media_url else "",
        )

        # Cache for tapback reactions
        remember_recent_inbound(from_number, message_handle, payload)

        # Mark as read immediately (fire and forget)
        asyncio.create_task(self._mark_read(message_handle, from_number))

        # Start typing indicator loop
        self._start_typing(from_number)

        # Determine message type and handle inbound media
        msg_type = MessageType.TEXT
        cached_urls: List[str] = []
        media_types: List[str] = []

        if media_url:
            msg_type = _guess_media_type(media_url)
            try:
                if msg_type == MessageType.PHOTO:
                    ext = Path(urlparse(media_url).path).suffix.lower() or ".jpg"
                    cached_path = await cache_image_from_url(media_url, ext=ext)
                    cached_urls.append(cached_path)
                    mime = mimetypes.guess_type(f"file{ext}")[0] or "image/jpeg"
                    media_types.append(mime)
                    logger.info("[sendblue] cached inbound image: %s", cached_path)
                elif msg_type == MessageType.VOICE:
                    ext = Path(urlparse(media_url).path).suffix.lower() or ".m4a"
                    cached_path = await cache_audio_from_url(media_url, ext=ext)
                    cached_urls.append(cached_path)
                    mime = mimetypes.guess_type(f"file{ext}")[0] or "audio/mpeg"
                    media_types.append(mime)
                    logger.info("[sendblue] cached inbound audio: %s", cached_path)
                else:
                    # Video, document, or unknown: pass URL through for agent
                    cached_urls.append(media_url)
                    ext = Path(urlparse(media_url).path).suffix.lower()
                    mime = mimetypes.guess_type(f"file{ext}")[0] or "application/octet-stream"
                    media_types.append(mime)
                    logger.info("[sendblue] inbound media (passthrough): %s", media_url[:80])
            except Exception as e:
                logger.warning("[sendblue] failed to cache media: %s", e)
                cached_urls.append(media_url)
                media_types.append("unknown")

        source = self.build_source(
            chat_id=from_number,
            chat_name=from_number,
            chat_type="dm",
            user_id=from_number,
            user_name=from_number,
        )
        event = MessageEvent(
            text=content,
            message_type=msg_type,
            source=source,
            raw_message=payload,
            message_id=message_handle,
            media_urls=cached_urls,
            media_types=media_types,
        )

        # Non-blocking: ACK SendBlue fast, process in background
        task = asyncio.create_task(self.handle_message(event))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

        return web.json_response({"ok": True})

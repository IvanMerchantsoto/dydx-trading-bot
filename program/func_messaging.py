"""
func_messaging.py
- Uses POST (not GET) so special chars, newlines, and markdown don't break URL
- Respects Telegram 4096-char limit with safe truncation
- Simple rate-limit guard: max 1 msg/second (Telegram allows ~30/s but being conservative)
- parse_mode=MarkdownV2 disabled by default to avoid escape issues; use plain text
"""

import requests
import time
import threading
from decouple import config

_last_sent_ts: float = 0.0
_lock = threading.Lock()
_MIN_INTERVAL_S = 1.0   # minimum seconds between messages

_MAX_MSG_LEN = 4000     # Telegram hard limit is 4096; keep some margin


def _sanitize(message: str) -> str:
    """Truncate and ensure string type."""
    if not isinstance(message, str):
        try:
            message = str(message)
        except Exception:
            message = "(non-serializable message)"
    if len(message) > _MAX_MSG_LEN:
        suffix = "\n...[truncated]"
        message = message[: _MAX_MSG_LEN - len(suffix)] + suffix
    return message


def send_message(message):
    """
    Send a Telegram message via POST.
    Returns 'sent', 'failed', or 'throttled'.
    Does NOT raise exceptions.
    """
    global _last_sent_ts

    try:
        bot_token = config("TELEGRAM_TOKEN")
        chat_id = config("TELEGRAM_CHAT_ID")
    except Exception:
        return "failed"

    message = _sanitize(message)

    with _lock:
        now = time.monotonic()
        elapsed = now - _last_sent_ts
        if elapsed < _MIN_INTERVAL_S:
            time.sleep(_MIN_INTERVAL_S - elapsed)
        _last_sent_ts = time.monotonic()

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            # parse_mode intentionally omitted to avoid MarkdownV2 escape requirements
        }
        res = requests.post(url, json=payload, timeout=10)
        if res.status_code == 200:
            return "sent"
        else:
            return "failed"
    except Exception:
        return "failed"

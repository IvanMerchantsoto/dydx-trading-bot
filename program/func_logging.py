import os
import json
import gzip
import shutil
import datetime
from pathlib import Path

# =========================
# CONFIG
# =========================
# Ruta absoluta basada en la ubicación de este archivo.
# Así funciona sin importar desde qué directorio se lanza el bot.
_HERE = Path(__file__).resolve().parent
LOG_DIR = _HERE / "logs"
LOG_FILE = LOG_DIR / "bot_run.log.jsonl"

MAX_LOG_SIZE_MB = 20
MAX_BACKUPS = 5

# Solo estos eventos se imprimen por defecto en terminal
PRINT_EVENTS = {
    "entry_signal",
    "open_live",
    "entry_result",
    "open_error",
    "error",
    "orphan_saved",
    "abort_error",
    "critical_error",
}

LOG_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# UTILS
# =========================
def _utc_ts():
    return datetime.datetime.utcnow().isoformat() + "Z"


def _safe_json_dumps(data):
    return json.dumps(data, ensure_ascii=False, default=str)


def _should_rotate() -> bool:
    try:
        if not LOG_FILE.exists():
            return False
        size_mb = LOG_FILE.stat().st_size / (1024 * 1024)
        return size_mb >= MAX_LOG_SIZE_MB
    except Exception:
        return False


def _rotate_logs():
    try:
        if not LOG_FILE.exists() or LOG_FILE.stat().st_size == 0:
            return

        timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        rotated_plain = LOG_DIR / f"bot_run_{timestamp}.log.jsonl"
        rotated_gz = LOG_DIR / f"bot_run_{timestamp}.log.jsonl.gz"

        # rename
        LOG_FILE.rename(rotated_plain)

        # compress
        with open(rotated_plain, "rb") as f_in, gzip.open(rotated_gz, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

        rotated_plain.unlink(missing_ok=True)

        # cleanup old logs
        backups = sorted(LOG_DIR.glob("bot_run_*.log.jsonl.gz"))
        if len(backups) > MAX_BACKUPS:
            for old_file in backups[: len(backups) - MAX_BACKUPS]:
                try:
                    old_file.unlink(missing_ok=True)
                except Exception:
                    pass

    except Exception as e:
        print(f"[LOG] Rotation error: {e}")


# =========================
# CORE LOGGER
# =========================
def log_event(event: dict, print_terminal=None):
    try:
        if not isinstance(event, dict):
            event = {"type": "log", "msg": str(event)}

        # timestamp
        if "ts" not in event:
            event["ts"] = _utc_ts()

        # rotate if needed
        if _should_rotate():
            _rotate_logs()

        # write
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(_safe_json_dumps(event) + "\n")

        # terminal print control
        if print_terminal is None:
            print_terminal = event.get("type") in PRINT_EVENTS

        if print_terminal:
            print(_safe_json_dumps(event))

    except Exception as e:
        try:
            print(f"[LOG] write error: {e}")
            print(f"[LOG] original event: {event}")
        except Exception:
            pass


# =========================
# HELPERS
# =========================
def log_info(msg: str, print_terminal=False):
    log_event(
        {
            "type": "info",
            "msg": msg,
        },
        print_terminal=print_terminal,
    )


def log_error(msg: str, print_terminal=True):
    log_event(
        {
            "type": "error",
            "msg": msg,
        },
        print_terminal=print_terminal,
    )


def log_trade(event: dict, print_terminal=None):
    if not isinstance(event, dict):
        event = {"type": "trade", "msg": str(event)}
    log_event(event, print_terminal=print_terminal)


def log_signal(event: dict, print_terminal=None):
    if not isinstance(event, dict):
        event = {"type": "signal", "msg": str(event)}
    log_event(event, print_terminal=print_terminal)


# =========================
# UTIL PARA MAIN
# =========================
def get_log_path():
    return str(LOG_FILE)
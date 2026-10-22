"""Small filesystem and timestamp helpers shared by generator commands."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path


def json_default(value):
    """Serialize timezone-aware dates without silently stringifying other types."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def dump_json(path, value):
    """Atomically replace a JSON report in its own directory."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
    temporary.replace(path)


def sha256_file(path):
    """Hash a file with bounded memory."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def epoch_ms(value):
    """Convert an aware datetime to integer milliseconds."""
    return int(value.timestamp() * 1000)


def iso_ms(value):
    """Format epoch milliseconds in UTC."""
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()

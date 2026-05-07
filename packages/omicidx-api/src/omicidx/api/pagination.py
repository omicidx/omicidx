import base64
import binascii
import json
from dataclasses import dataclass

from fastapi import HTTPException


@dataclass
class CursorPage:
    """Decoded cursor with the keyset value for WHERE clause."""

    after: str | int


def encode_cursor(value: str | int) -> str:
    """Encode a keyset value into an opaque base64url cursor token."""
    payload = json.dumps({"v": value}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode()).rstrip(b"=").decode()


def decode_cursor(token: str) -> CursorPage:
    """Decode a cursor token back to a keyset value."""
    try:
        padded = token + "=" * (-len(token) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded))
        value = payload["v"]
    except (
        binascii.Error,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        UnicodeDecodeError,
    ) as exc:
        raise HTTPException(status_code=400, detail="Invalid cursor") from exc

    if not isinstance(value, str | int):
        raise HTTPException(status_code=400, detail="Invalid cursor")

    return CursorPage(after=value)

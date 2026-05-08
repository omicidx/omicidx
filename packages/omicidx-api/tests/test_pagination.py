import pytest
from fastapi import HTTPException
from omicidx.api.pagination import CursorPage, decode_cursor, encode_cursor


def test_encode_decode_string():
    cursor = encode_cursor("SAMN12345")
    result = decode_cursor(cursor)
    assert isinstance(result, CursorPage)
    assert result.after == "SAMN12345"


def test_encode_decode_int():
    cursor = encode_cursor(42)
    result = decode_cursor(cursor)
    assert result.after == 42


def test_cursor_is_opaque_base64url():
    cursor = encode_cursor("PRJNA100")
    # Should be base64url-safe (no +, /, or = padding)
    assert "+" not in cursor
    assert "/" not in cursor
    assert not cursor.endswith("=")


def test_roundtrip_preserves_type():
    for val in ["SRR000001", 12345, "GSE100000"]:
        assert decode_cursor(encode_cursor(val)).after == val


def test_invalid_cursor_raises_http_400():
    with pytest.raises(HTTPException) as exc_info:
        decode_cursor("not-a-valid-cursor")
    assert exc_info.value.status_code == 400


def test_cursor_missing_value_raises_http_400():
    with pytest.raises(HTTPException) as exc_info:
        decode_cursor("e30")  # {}
    assert exc_info.value.status_code == 400

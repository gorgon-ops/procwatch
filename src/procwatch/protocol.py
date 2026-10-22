"""Length-prefixed JSON framing: a 4-byte big-endian length, then UTF-8 JSON."""

from __future__ import annotations

import json
import struct
from collections.abc import Iterator
from typing import Any

#: Larger frames are refused, so a hostile peer cannot force unbounded allocation.
MAX_FRAME_BYTES = 1 << 20

_HEADER = struct.Struct("!I")
HEADER_SIZE = _HEADER.size

# --- message types --------------------------------------------------------

HELLO = "hello"          # first frame on every connection
HEARTBEAT = "heartbeat"  # periodic liveness
BYE = "bye"              # clean shutdown
DUMP_REQUEST = "dump"    # server -> client
STACKS = "stacks"        # reply to DUMP_REQUEST


class ProtocolError(Exception):
    pass


def encode(message: dict[str, Any]) -> bytes:
    payload = json.dumps(message, separators=(",", ":")).encode("utf-8")
    if len(payload) > MAX_FRAME_BYTES:
        raise ProtocolError(
            f"message of {len(payload)} bytes exceeds the "
            f"{MAX_FRAME_BYTES} byte frame limit"
        )
    return _HEADER.pack(len(payload)) + payload


class FrameDecoder:
    """Keeps any partial frame until the rest arrives."""

    __slots__ = ("_buffer",)

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> Iterator[dict[str, Any]]:
        self._buffer.extend(data)

        while True:
            if len(self._buffer) < HEADER_SIZE:
                return

            (length,) = _HEADER.unpack_from(self._buffer, 0)
            if length > MAX_FRAME_BYTES:
                raise ProtocolError(
                    f"peer announced a {length} byte frame, over the "
                    f"{MAX_FRAME_BYTES} byte limit"
                )

            end = HEADER_SIZE + length
            if len(self._buffer) < end:
                return

            payload = bytes(self._buffer[HEADER_SIZE:end])
            del self._buffer[:end]

            try:
                message = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ProtocolError(f"undecodable frame: {exc}") from exc

            if not isinstance(message, dict):
                raise ProtocolError(
                    f"expected a JSON object, got {type(message).__name__}"
                )

            yield message

    @property
    def pending_bytes(self) -> int:
        return len(self._buffer)

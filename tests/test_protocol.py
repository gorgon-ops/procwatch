from __future__ import annotations

import json
import struct

import pytest

from procwatch.protocol import (
    HEADER_SIZE,
    MAX_FRAME_BYTES,
    FrameDecoder,
    ProtocolError,
    encode,
)


def test_round_trip():
    message = {"type": "heartbeat", "name": "w", "seq": 1}
    assert list(FrameDecoder().feed(encode(message))) == [message]


def test_two_messages_in_one_read():
    """The original did json.loads on whatever recv returned; two glued frames raised."""
    blob = encode({"type": "a"}) + encode({"type": "b"})

    assert list(FrameDecoder().feed(blob)) == [{"type": "a"}, {"type": "b"}]


def test_message_split_across_reads():
    """A frame arriving in pieces is buffered until it is whole."""
    decoder = FrameDecoder()
    blob = encode({"type": "heartbeat", "payload": "x" * 500})

    for index in range(len(blob) - 1):
        assert list(decoder.feed(blob[index : index + 1])) == []

    assert list(decoder.feed(blob[-1:])) == [
        {"type": "heartbeat", "payload": "x" * 500}
    ]


def test_header_split_across_reads():
    decoder = FrameDecoder()
    blob = encode({"type": "a"})

    assert list(decoder.feed(blob[:2])) == []
    assert decoder.pending_bytes == 2
    assert list(decoder.feed(blob[2:])) == [{"type": "a"}]
    assert decoder.pending_bytes == 0


def test_byte_at_a_time_stream_of_many_messages():
    decoder = FrameDecoder()
    expected = [{"type": "heartbeat", "seq": n} for n in range(20)]
    blob = b"".join(encode(m) for m in expected)

    received = [m for byte in blob for m in decoder.feed(bytes([byte]))]

    assert received == expected


def test_unicode_survives_framing():
    """Length is in bytes, not characters: multi-byte text must not truncate."""
    message = {"type": "note", "text": "café — naïve 日本語"}
    assert list(FrameDecoder().feed(encode(message))) == [message]


def test_oversized_announcement_is_refused():
    """A peer must not be able to make us buffer without bound."""
    header = struct.pack("!I", MAX_FRAME_BYTES + 1)

    with pytest.raises(ProtocolError, match="over the"):
        list(FrameDecoder().feed(header + b"junk"))


def test_encoding_an_oversized_message_is_refused():
    with pytest.raises(ProtocolError, match="exceeds"):
        encode({"blob": "x" * (MAX_FRAME_BYTES + 10)})


def test_malformed_json_raises():
    payload = b"{not json"
    frame = struct.pack("!I", len(payload)) + payload

    with pytest.raises(ProtocolError, match="undecodable"):
        list(FrameDecoder().feed(frame))


def test_non_object_json_raises():
    payload = json.dumps([1, 2, 3]).encode()
    frame = struct.pack("!I", len(payload)) + payload

    with pytest.raises(ProtocolError, match="expected a JSON object"):
        list(FrameDecoder().feed(frame))


def test_empty_feed_is_harmless():
    decoder = FrameDecoder()
    assert list(decoder.feed(b"")) == []
    assert decoder.pending_bytes == 0


def test_header_size_matches_encoding():
    assert len(encode({})) == HEADER_SIZE + len(b"{}")

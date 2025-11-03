"""
packet.py — Minimal HUDP Wire Format (Timestamp, SeqNo, ChannelType, Checksum)
-------------------------------------------------------------------------------
Each frame includes:
| ts_send (4B) | seq_no (2B) | chan_type (1B) | checksum (2B) | payload ... |
"""

import struct
import time
import zlib

# Network (big-endian)
HEADER_FORMAT = "!IHBH"   # ts_send, seq_no, chan_type, checksum
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 9 bytes


# ---------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------
def now_ms() -> int:
    """Return current time in milliseconds since epoch."""
    return int(time.time() * 1000)


def _crc16(data: bytes) -> int:
    """Compute 16-bit CRC (folded CRC32)."""
    return zlib.crc32(data) & 0xFFFF


# ---------------------------------------------------------------------
# Encoding functions
# ---------------------------------------------------------------------
def encode_data(ts_send: int, seq_no: int, chan_type: int, payload: bytes) -> bytes:
    """
    Build a DATA frame.
    chan_type: 0 = unreliable, 1 = reliable
    """
    ts_send &= 0xFFFFFFFF # Wrap timestamp modulo 2^32 to keep it within 4 bytes
    header_wo_cksum = struct.pack(HEADER_FORMAT, ts_send, seq_no, chan_type, 0)
    checksum = _crc16(header_wo_cksum + payload)
    header = struct.pack(HEADER_FORMAT, ts_send, seq_no, chan_type, checksum)
    return header + payload


def encode_ack(seq_no: int, ts_send: int, chan_type: int = 0) -> bytes:
    """
    Build an ACK-only frame (no payload).
    chan_type = 0 by default.
    """
    header_wo_cksum = struct.pack(HEADER_FORMAT, ts_send, seq_no, chan_type, 0)
    checksum = _crc16(header_wo_cksum)
    header = struct.pack(HEADER_FORMAT, ts_send, seq_no, chan_type, checksum)
    return header


# ---------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------
def decode_frame(datagram: bytes):
    """
    Parse a datagram into (frame_type, header_dict, payload).

    Returns:
        ("DATA" | "ACK", header_dict, payload)
        where header_dict includes "valid": bool
    """
    if len(datagram) < HEADER_SIZE:
        raise ValueError("Truncated datagram (<9 bytes)")

    ts_send, seq_no, chan_type, checksum = struct.unpack(HEADER_FORMAT, datagram[:HEADER_SIZE])
    payload = datagram[HEADER_SIZE:]

    header_zero_cksum = struct.pack(HEADER_FORMAT, ts_send, seq_no, chan_type, 0)
    calc_cksum = _crc16(header_zero_cksum + payload)
    valid = (calc_cksum == checksum)

    frame_type = "ACK" if len(payload) == 0 else "DATA"

    header_dict = {
        "ts_send": ts_send,
        "seq_no": seq_no,
        "chan_type": chan_type,
        "checksum": checksum,
        "valid": valid,
    }

    return frame_type, header_dict, payload


# ---------------------------------------------------------------------
# Manual test
# ---------------------------------------------------------------------
if __name__ == "__main__":
    payload = b"move_forward"
    ts = now_ms()
    encoded = encode_data(ts, 42, 1, payload)
    ftype, hdr, pl = decode_frame(encoded)
    print("Frame:", ftype)
    print("Header:", hdr)
    print("Payload:", pl)

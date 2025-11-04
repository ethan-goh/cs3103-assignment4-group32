"""
packet.py — Minimal HUDP Wire Format (Timestamp, SeqNo, ChannelType, Checksum)
-------------------------------------------------------------------------------
Each frame includes:
| ts_send (4B) | seq_no (2B) | chan_type (1B) | checksum (2B) | payload ... |
"""

import struct
import time
import zlib
import json

# Network (big-endian)
HEADER_FORMAT = "!IHBH"   # ts_send, seq_no, chan_type, checksum
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 9 bytes

# Channel type constants
CHAN_UNRELIABLE = 0
CHAN_RELIABLE = 1
CHAN_STATS_SYNC = -1  # Special channel type for statistics synchronization


# ---------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------
def now_ms() -> int:
    """Return current time in milliseconds since epoch, modulo 2^32."""
    return int(time.time() * 1000) & 0xFFFFFFFF


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
    ts_send &= 0xFFFFFFFF  # Wrap timestamp modulo 2^32 to keep it within 4 bytes
    header_wo_cksum = struct.pack(HEADER_FORMAT, ts_send, seq_no, chan_type, 0)
    checksum = _crc16(header_wo_cksum)
    header = struct.pack(HEADER_FORMAT, ts_send, seq_no, chan_type, checksum)
    return header


def encode_stats_sync(reliable_sent: int, unreliable_sent: int, 
                     reliable_received: int, unreliable_received: int,
                     acks_sent: int = 0, acks_received: int = 0) -> bytes:
    """
    Build a STATS_SYNC frame for periodic statistics exchange.
    """
    ts_send = now_ms()
    
    stats_payload = {
        "type": "STATS_SYNC",
        "reliable_sent": reliable_sent,
        "unreliable_sent": unreliable_sent,
        "reliable_received": reliable_received,
        "unreliable_received": unreliable_received,
        "acks_sent": acks_sent,
        "acks_received": acks_received,
        "timestamp": ts_send
    }
    
    payload_bytes = json.dumps(stats_payload).encode('utf-8')
    
    # Use reliable channel to ensure delivery
    header_wo_cksum = struct.pack(HEADER_FORMAT, ts_send, 0, CHAN_RELIABLE, 0)
    checksum = _crc16(header_wo_cksum + payload_bytes)
    header = struct.pack(HEADER_FORMAT, ts_send, 0, CHAN_RELIABLE, checksum)
    
    return header + payload_bytes


# ---------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------
def decode_frame(datagram: bytes):
    """
    Parse a datagram into (frame_type, header_dict, payload).

    Returns:
        ("DATA" | "ACK" | "STATS_SYNC", header_dict, payload)
        where header_dict includes "valid": bool
    """
    if len(datagram) < HEADER_SIZE:
        raise ValueError("Truncated datagram (<9 bytes)")

    ts_send, seq_no, chan_type, checksum = struct.unpack(HEADER_FORMAT, datagram[:HEADER_SIZE])
    payload = datagram[HEADER_SIZE:]

    header_zero_cksum = struct.pack(HEADER_FORMAT, ts_send, seq_no, chan_type, 0)
    calc_cksum = _crc16(header_zero_cksum + payload)
    valid = (calc_cksum == checksum)

    # Determine frame type
    if len(payload) == 0:
        frame_type = "ACK"
    else:
        # Check if it's a STATS_SYNC message
        try:
            payload_json = json.loads(payload.decode('utf-8'))
            if payload_json.get("type") == "STATS_SYNC":
                frame_type = "STATS_SYNC"
            else:
                frame_type = "DATA"
        except (json.JSONDecodeError, UnicodeDecodeError):
            frame_type = "DATA"

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

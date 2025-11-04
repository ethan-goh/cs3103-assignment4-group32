"""
sr_sender.py — Selective Repeat Sender for HUDP
------------------------------------------------
Implements the sender-side logic of the Selective Repeat (SR) protocol,
extended with optional timeout-based skipping of lost packets.

Responsibilities:
- Maintain a fixed-size sliding window of outstanding reliable packets.
- Handle per-packet timers for retransmissions.
- Skip lost packets after skip_threshold_ms (to prevent stalling).
- Update state on receiving ACKs.
- Compute RTTs for performance metrics.

Note: This module only contains SR logic. It does not send packets itself.
      `io_async.py` is responsible for calling these methods periodically
      and actually sending/receiving datagrams.
"""

from typing import Dict, List, Tuple
from hudp.packet import encode_data, now_ms

MAX_SEQ = 65536


class SRSender:
    def __init__(self, window_size: int = 128, rto_ms: int = 50, skip_threshold_ms: int = 200):
        """
        Args:
            window_size: Number of unacked packets allowed at once.
            rto_ms: Retransmission timeout in milliseconds.
            skip_threshold_ms: Time threshold after which lost packets are skipped.
        """
        self.window_size = window_size
        self.rto_ms = rto_ms
        self.skip_threshold_ms = skip_threshold_ms

        # Base = first unacked packet in window
        self.base = 0
        # Next sequence number to assign
        self.next_seq = 0

        # Outstanding packets (seq → info dict)
        self.unacked: Dict[int, dict] = {}

    # ----------------------------------------------------------------------
    def queue_reliable(self, data: bytes, chan_type: int = 1) -> int:
        """
        Stage a new reliable packet for transmission.
        Returns:
            seq_no (int): assigned sequence number,
            or -1 if window is currently full.
        """
        if self._window_full():
            return -1  # caller (io_async) can retry later

        seq = self.next_seq
        self.next_seq = (self.next_seq + 1) % MAX_SEQ

        ts_send = now_ms()
        self.unacked[seq] = {
            "payload": data,
            "chan_type": chan_type,  # 1 = reliable, 0 = unreliable
            "ts_send": ts_send,
            "last_tx": 0,    # 0 means not yet sent
            "acked": False,
            "retransmissions": 0,
        }
        return seq

    # ----------------------------------------------------------------------
    def next_frames(self, now: int) -> List[Tuple[int, bytes]]:
        """
        Decide which reliable packets should be sent or retransmitted now.
        Called periodically by io_async.
        Returns:
            List of (seq_no, frame_bytes) tuples ready to send.
        """
        frames = []

        # Check for any timed-out (skipped) packets first
        self._maybe_skip_lost_packets(now)

        for seq, info in list(self.unacked.items()):
            if info["acked"]:
                continue

            # Send for first time OR retransmit if timeout expired
            if info["last_tx"] == 0 or (now - info["last_tx"]) >= self.rto_ms:
                # Track retransmissions (first send doesn't count)
                if info["last_tx"] != 0:
                    info["retransmissions"] += 1
                
                info["last_tx"] = now
                frame = encode_data(now, seq, info["chan_type"], info["payload"])
                frames.append((seq, frame))

        return frames

    # ----------------------------------------------------------------------
    def on_ack(self, ack_no: int, now: int) -> List[Tuple[int, int, int]]:
        """
        Handle an incoming ACK for a given sequence number.
        Marks the packet as acknowledged and slides the window if possible.
        Returns:
            List of (seq_no, rtt_ms, retransmissions) for all packets newly acknowledged.
        """
        if ack_no not in self.unacked:
            return []  # might be duplicate or old ACK

        pkt_info = self.unacked[ack_no]
        if pkt_info["acked"]:
            return []  # duplicate ACK

        pkt_info["acked"] = True
        rtt = now - pkt_info["ts_send"]
        retransmissions = pkt_info["retransmissions"]

        # Try to slide the window base
        self._slide_window()

        return [(ack_no, rtt, retransmissions)]

    # ----------------------------------------------------------------------
    def _slide_window(self):
        """Remove all contiguous acknowledged packets starting from base."""
        while self.base in self.unacked and self.unacked[self.base]["acked"]:
            del self.unacked[self.base]
            self.base = (self.base + 1) % MAX_SEQ

    # ----------------------------------------------------------------------
    def _maybe_skip_lost_packets(self, now: int):
        """
        Skip packets that have been unacknowledged beyond skip_threshold_ms.
        This prevents permanent stalls under persistent loss.
        """
        while self.base in self.unacked:
            pkt = self.unacked[self.base]
            if pkt["acked"]:
                del self.unacked[self.base]
                self.base = (self.base + 1) % MAX_SEQ
                continue

            elapsed = now - pkt["ts_send"]
            if elapsed >= self.skip_threshold_ms:
                print(f"[SENDER] Skipping lost seq={self.base} (>{self.skip_threshold_ms} ms)")
                del self.unacked[self.base]
                self.base = (self.base + 1) % MAX_SEQ
            else:
                break  # stop once the first non-expired packet is reached

    # ----------------------------------------------------------------------
    def _window_full(self) -> bool:
        """Return True if the sender window is currently full."""
        unacked_count = len([s for s, p in self.unacked.items() if not p["acked"]])
        return unacked_count >= self.window_size

    # ----------------------------------------------------------------------
    def get_window_state(self) -> Dict[str, int]:
        """Debug helper — return counts for monitoring or metrics."""
        total = len(self.unacked)
        acked = len([s for s, p in self.unacked.items() if p["acked"]])
        pending = total - acked
        return {
            "base": self.base,
            "next_seq": self.next_seq,
            "inflight": pending,
            "acked": acked,
        }


# ----------------------------------------------------------------------
# Manual test harness
# ----------------------------------------------------------------------
if __name__ == "__main__":
    import time

    sender = SRSender(window_size=4, rto_ms=200, skip_threshold_ms=300)
    data = b"Hello world"

    # Queue a few packets
    for i in range(3):
        seq = sender.queue_reliable(data)
        print(f"Queued seq {seq}")

    now = now_ms()
    frames = sender.next_frames(now)
    print(f"Frames to send: {[seq for seq, _ in frames]}")

    # Simulate ACK for seq 0
    time.sleep(0.25)
    acked = sender.on_ack(0, now_ms())
    print("ACKed:", acked)

    # Wait long enough for skip logic to trigger
    time.sleep(0.4)
    sender.next_frames(now_ms())  # triggers skip
    print("Window state after skip:", sender.get_window_state())

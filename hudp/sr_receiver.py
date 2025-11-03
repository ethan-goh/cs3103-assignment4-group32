from collections import deque

class SRReceiver:
    def __init__(self, window_size=128, skip_threshold_ms=1000):
        self.expected_seq = 0
        self.buffer = {}            # seq -> (ts_send, payload)
        self.ack_outbox = deque()   # seq numbers to ACK
        self.gap_start_ms = None    # when we first noticed a gap at expected_seq
        self.skip_threshold_ms = skip_threshold_ms
        self.window_size = window_size

    def _seq_diff(self, seq_a: int, seq_b: int) -> int:
        """
        Calculate sequence number difference handling 16-bit wraparound.
        Returns seq_a - seq_b in sequence space.
        Positive result means seq_a is ahead of seq_b.
        """
        diff = (seq_a - seq_b) & 0xFFFF
        # If difference > 32767, it's actually a negative difference (wrapped around)
        if diff > 32767:
            diff -= 65536
        return diff
    
    def _in_window(self, seq: int) -> bool:
        """
        Check if seq is within the receive window [expected_seq, expected_seq + window_size).
        Handles 16-bit sequence number wraparound correctly.
        """
        diff = self._seq_diff(seq, self.expected_seq)
        return 0 <= diff < self.window_size
    
    def _already_have(self, seq: int) -> bool:
        """
        Check if we have already received or delivered seq.
        Handles wraparound: seq is "old" if it's behind expected_seq.
        """
        diff = self._seq_diff(seq, self.expected_seq)
        return diff < 0 or seq in self.buffer

    def _maybe_start_gap_timer(self, now_ms: int):
        # If we're blocked by a hole at expected_seq, ensure a gap timer exists
        if self.gap_start_ms is None and self.buffer and self.expected_seq not in self.buffer:
            self.gap_start_ms = now_ms

    def _maybe_skip_gap(self, now_ms: int):
        # Skip missing expected_seq if we waited long enough
        while (
            self.gap_start_ms is not None and
            (now_ms - self.gap_start_ms) >= self.skip_threshold_ms and
            self.expected_seq not in self.buffer
        ):
            # skip one missing seq
            self.expected_seq = (self.expected_seq + 1) & 0xFFFF
            # if next expected still missing, keep waiting from "now"
            # (restart the wait for the next hole)
            self.gap_start_ms = now_ms if self.expected_seq not in self.buffer else None
    
    def _drain_contiguous(self):
        deliveries = []
        while True:
            entry = self.buffer.pop(self.expected_seq, None)
            if entry is None:
                break
            ts_send, payload = entry
            deliveries.append((self.expected_seq, payload))
            self.expected_seq = (self.expected_seq + 1) & 0xFFFF
        
        # After draining, if buffer is now empty or we have no gap at expected_seq,
        # reset the gap timer
        if not self.buffer or self.expected_seq not in self.buffer:
            # Check if we have packets ahead but gap at expected_seq
            has_gap = len(self.buffer) > 0 and self.expected_seq not in self.buffer
            if not has_gap:
                self.gap_start_ms = None
        
        return deliveries


    def on_data(self, seq: int, ts_send: int, payload: bytes, now_ms: int):
        """
        Insert a reliable DATA packet.
        Returns: list[(seq, payload)] that became deliverable NOW (in-order),
                 after applying skip-threshold if needed.
        """
        deliveries = []

        # ACK EVERY reliable DATA frame (even if way ahead of window)
        # This is more generous than standard SR but prevents retransmission loops
        self.ack_outbox.append(seq)

        # Check window bounds for buffering decisions (wraparound-aware)
        seq_diff = self._seq_diff(seq, self.expected_seq)
        
        if seq_diff < 0:
            # Old packet (already delivered) - ACKed above, but don't buffer
            # This handles the case where ACK was lost and sender retransmitted
            return deliveries
        
        if seq_diff >= self.window_size:
            # Packet too far ahead - ACKed above, but don't buffer (protect memory)
            return deliveries

        # Duplicate? (already delivered or buffered)
        if self._already_have(seq):
            # still ACKed above; nothing else to do
            # but we can still advance skip if a gap was pending
            self._maybe_skip_gap(now_ms)
            deliveries.extend(self._drain_contiguous())
            return deliveries

        # Buffer new out-of-order or the expected one
        self.buffer[seq] = (ts_send, payload)

        # If this wasn't the expected seq, make sure a gap timer exists
        if seq != self.expected_seq:
            if self.gap_start_ms is None:
                self.gap_start_ms = now_ms

        # If there is/was a gap at expected_seq, check whether to skip
        self._maybe_skip_gap(now_ms)

        # Finally, deliver all contiguous packets starting from expected_seq
        deliveries.extend(self._drain_contiguous())
        return deliveries


    def pop_ack(self):
        """
        Pop the next ACK sequence number to send.
        Returns: sequence number to ACK, or None if no ACKs queued.
        Called by IO layer to drain ACK outbox.
        """
        try:
            return self.ack_outbox.popleft()
        except IndexError:
            return None


def test_sr_receiver():
    """Light testing of SR Receiver functionality"""
    print("🧪 Testing SR Receiver Implementation\n")
    
    # Test 1: Basic in-order delivery
    print("=== Test 1: Basic In-Order ===")
    receiver = SRReceiver(window_size=10, skip_threshold_ms=1000)
    
    delivered = receiver.on_data(0, 100, b"data0", 1000)
    print(f"Seq 0: delivered={delivered}, expected_seq={receiver.expected_seq}")
    
    delivered = receiver.on_data(1, 101, b"data1", 1001)
    print(f"Seq 1: delivered={delivered}, expected_seq={receiver.expected_seq}")
    
    # Drain ACKs
    acks = [receiver.pop_ack() for _ in range(3) if receiver.ack_outbox]
    print(f"ACKs: {[ack for ack in acks if ack is not None]}\n")
    
    # Test 2: Out-of-order buffering
    print("=== Test 2: Out-of-Order Buffering ===")
    receiver = SRReceiver(window_size=10, skip_threshold_ms=1000)
    
    # Receive 2, then 1, then 0 - should deliver all when 0 arrives
    delivered = receiver.on_data(2, 102, b"data2", 1000)
    print(f"Seq 2 (expecting 0): delivered={delivered}, buffered={list(receiver.buffer.keys())}")
    
    delivered = receiver.on_data(1, 101, b"data1", 1001)
    print(f"Seq 1 (expecting 0): delivered={delivered}, buffered={list(receiver.buffer.keys())}")
    
    delivered = receiver.on_data(0, 100, b"data0", 1002)
    print(f"Seq 0: delivered={delivered}, buffered={list(receiver.buffer.keys())}")
    print(f"Expected seq now: {receiver.expected_seq}\n")
    
    # Test 3: Duplicate handling
    print("=== Test 3: Duplicates ===")
    receiver = SRReceiver(window_size=10, skip_threshold_ms=1000)
    
    delivered1 = receiver.on_data(0, 100, b"data0", 1000)
    delivered2 = receiver.on_data(0, 100, b"data0_dup", 1001)  # duplicate
    print(f"First seq 0: delivered={delivered1}")
    print(f"Duplicate seq 0: delivered={delivered2}")
    
    # Should have 2 ACKs for the same seq
    acks = [receiver.pop_ack(), receiver.pop_ack()]
    print(f"ACKs for duplicates: {acks}\n")
    
    # Test 4: Window Bounds - Within Window (ACK + Buffer)
    print("=== Test 4: Within Window - ACK + Buffer ===")
    receiver = SRReceiver(window_size=5, skip_threshold_ms=1000)
    
    # Packets within window [0, 5) should be ACKed and buffered
    delivered = receiver.on_data(2, 102, b"data2", 1000)
    print(f"Seq 2 (within window [0,5)): delivered={delivered}")
    print(f"Buffered: {list(receiver.buffer.keys())}")
    ack = receiver.pop_ack()
    print(f"ACK sent: {ack}")
    
    delivered = receiver.on_data(4, 104, b"data4", 1001)
    print(f"Seq 4 (within window [0,5)): delivered={delivered}")
    print(f"Buffered: {list(receiver.buffer.keys())}")
    ack = receiver.pop_ack()
    print(f"ACK sent: {ack}\n")
    
    # Test 5: Window Bounds - Behind Window (ACK but No Buffer)
    print("=== Test 5: Behind Window - ACK but No Buffer ===")
    receiver = SRReceiver(window_size=5, skip_threshold_ms=1000)
    
    # Advance the window by delivering some packets
    receiver.on_data(0, 100, b"data0", 1000)  # expected_seq becomes 1
    receiver.on_data(1, 101, b"data1", 1001)  # expected_seq becomes 2
    _ = receiver.pop_ack()  # Clear ACKs
    _ = receiver.pop_ack()
    
    print(f"Window now at expected_seq={receiver.expected_seq}, window=[{receiver.expected_seq},{receiver.expected_seq + receiver.window_size})")
    
    # Send old packet (behind window)
    delivered = receiver.on_data(0, 100, b"data0_retx", 1002)  # seq=0 is behind expected_seq=2
    print(f"Seq 0 (behind window): delivered={delivered}")
    print(f"Buffered: {list(receiver.buffer.keys())} (should be empty - not buffered)")
    ack = receiver.pop_ack()
    print(f"ACK sent: {ack} (should still ACK old packets)\n")
    
    # Test 6: Window Bounds - Ahead of Window (ACK but No Buffer)
    print("=== Test 6: Ahead of Window - ACK but No Buffer ===")
    receiver = SRReceiver(window_size=3, skip_threshold_ms=1000)  # Small window
    
    print(f"Window: [{receiver.expected_seq},{receiver.expected_seq + receiver.window_size})")
    
    # Send packet way ahead of window [0, 3)
    delivered = receiver.on_data(10, 110, b"data10", 1000)  # seq=10 is ahead of window
    print(f"Seq 10 (ahead of window [0,3)): delivered={delivered}")
    print(f"Buffered: {list(receiver.buffer.keys())} (should be empty - not buffered)")
    ack = receiver.pop_ack()
    print(f"ACK sent: {ack} (should still ACK ahead packets)\n")
    
    # Test 7: Wraparound Window Behavior  
    print("=== Test 7: Wraparound Window Behavior ===")
    receiver = SRReceiver(window_size=10, skip_threshold_ms=1000)
    
    # Set up near wraparound boundary
    receiver.expected_seq = 65530  # Window: [65530, 65535, 0, 1, 2, 3, 4]
    
    print(f"Expected seq: {receiver.expected_seq}, Window size: {receiver.window_size}")
    print("Window spans: [65530, 65531, 65532, 65533, 65534, 65535, 0, 1, 2, 3]")
    
    # Test within wraparound window
    delivered = receiver.on_data(65532, 1000, b"data65532", 1000)  # Within window
    print(f"Seq 65532 (within wraparound window): delivered={delivered}")
    print(f"Buffered: {list(receiver.buffer.keys())}")
    ack = receiver.pop_ack()
    print(f"ACK: {ack}")
    
    delivered = receiver.on_data(1, 1001, b"data1", 1001)  # Within window (wrapped)
    print(f"Seq 1 (within wraparound window): delivered={delivered}")
    print(f"Buffered: {list(receiver.buffer.keys())}")
    ack = receiver.pop_ack()
    print(f"ACK: {ack}")
    
    # Test behind wraparound window  
    delivered = receiver.on_data(65525, 1002, b"data65525", 1002)  # Behind window
    print(f"Seq 65525 (behind wraparound window): delivered={delivered}")
    print(f"Buffered: {list(receiver.buffer.keys())} (should not include 65525)")
    ack = receiver.pop_ack()
    print(f"ACK: {ack} (should still ACK)")
    
    # Test ahead of wraparound window
    delivered = receiver.on_data(15, 1003, b"data15", 1003)  # Ahead of window
    print(f"Seq 15 (ahead of wraparound window): delivered={delivered}")
    print(f"Buffered: {list(receiver.buffer.keys())} (should not include 15)")
    ack = receiver.pop_ack()
    print(f"ACK: {ack} (should still ACK)\n")
    
    # Test 8: Skip threshold
    print("=== Test 8: Skip Threshold ===")
    receiver = SRReceiver(window_size=10, skip_threshold_ms=200)  # Short timeout
    
    # Receive seq 2, creating a gap at seq 0,1
    delivered = receiver.on_data(2, 102, b"data2", 1000)
    print(f"Seq 2 at t=1000: delivered={delivered}, gap_start={receiver.gap_start_ms}")
    
    # Wait and receive seq 3 - should trigger skip of seq 0
    delivered = receiver.on_data(3, 103, b"data3", 1300)  # 300ms later
    print(f"Seq 3 at t=1300 (300ms later): delivered={delivered}")
    print(f"Expected seq after skip: {receiver.expected_seq}")
    print(f"Remaining buffered: {list(receiver.buffer.keys())}\n")
    
    print("✅ All tests completed!")


if __name__ == "__main__":
    test_sr_receiver()
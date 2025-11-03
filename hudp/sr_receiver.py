from collections import deque

class SRReceiver:
    def __init__(self, window_size=128, skip_threshold_ms=1000):
        self.expected_seq = 0
        self.buffer = {}            # seq -> (ts_send, payload)
        self.ack_outbox = deque()   # seq numbers to ACK
        self.gap_start_ms = None    # when we first noticed a gap at expected_seq
        self.skip_threshold_ms = skip_threshold_ms
        self.window_size = window_size

    def _in_window(self, seq: int) -> bool:
        """Check if seq is within the receive window."""
        return self.expected_seq <= seq < self.expected_seq + self.window_size
    
    def _already_have(self, seq: int) -> bool:
        """Check if we have already received or delivered seq."""
        return seq < self.expected_seq or seq in self.buffer

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
        return deliveries


    def on_data(self, seq: int, ts_send: int, payload: bytes, now_ms: int):
        """
        Insert a reliable DATA packet.
        Returns: list[(seq, payload)] that became deliverable NOW (in-order),
                 after applying skip-threshold if needed.
        """
        deliveries = []

        # ACK every DATA frame (duplicates included)
        self.ack_outbox.append(seq)

        # Drop if far outside our receive window (protect memory)
        if not self._in_window(seq):
            return deliveries

        # Duplicate? (already delivered or buffered)
        if self._already_have(seq):
            # still ACKed above; nothing else to do
            # but we can still advance skip if a gap was pending
            if self.gap_start_ms == 0:  # sentinel: need to start now
                self.gap_start_ms = now_ms
            self._maybe_skip_gap(now_ms)
            deliveries.extend(self._drain_contiguous())
            return deliveries

        # Buffer new out-of-order or the expected one
        self.buffer[seq] = (ts_send, payload)

        # If we were waiting to start the gap timer, start it now
        if self.gap_start_ms == 0:
            self.gap_start_ms = now_ms

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
    
    # Test 4: Window bounds
    print("=== Test 4: Window Bounds ===")
    receiver = SRReceiver(window_size=3, skip_threshold_ms=1000)  # Small window
    
    # Try to send packet outside window [0, 3)
    delivered = receiver.on_data(5, 105, b"data5", 1000)  # Outside window
    print(f"Seq 5 (outside window [0,3)): delivered={delivered}")
    print(f"Buffered: {list(receiver.buffer.keys())}")
    
    # This should not generate an ACK since it's outside window
    ack = receiver.pop_ack()
    print(f"ACK for out-of-window: {ack}\n")
    
    # Test 5: Skip threshold
    print("=== Test 5: Skip Threshold ===")
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
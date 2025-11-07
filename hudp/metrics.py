# metrics.py
import math

CHAN_RELIABLE = 1       # chan_type = 1 for reliable
CHAN_UNRELIABLE = 0     # chan_type = 0 for unreliable


class ChannelStats:
    def __init__(self):
        # Local counters (what I know about myself)
        self.sent = 0
        self.received = 0
        self.bytes_recv = 0
        self.bytes_sent = 0  # NEW: Track sent bytes for sender throughput
        self.acks_received = 0  # Track ACKs received (for reliable channel)

        # Peer counters (what peer told me via STATS_SYNC)
        self.peer_sent = 0
        self.peer_received = 0
        self.peer_acks_sent = 0      # How many ACKs peer has sent
        self.peer_acks_received = 0  # How many ACKs peer has received

        # Timing for received data (server-side)
        self.first_recv_ts = None
        self.last_recv_ts = None
        
        # Timing for sent data (client-side)
        self.first_sent_ts = None
        self.last_sent_ts = None

        self.last_transit = None
        self.jitter = 0.0
        self.latency_sum = 0.0
        
        # Track STATS_SYNC samples for client-side latency estimation
        self._stats_sync_samples = 0

    def on_sent(self, payload_len: int = 0, send_ts_ms: int = 0):
        """Track sent packets with payload size and timestamp"""
        self.sent += 1
        self.bytes_sent += payload_len
        
        # Track timing for sender throughput calculation
        if self.first_sent_ts is None:
            self.first_sent_ts = send_ts_ms
        self.last_sent_ts = send_ts_ms

    def on_ack_received(self):
        """Track when an ACK is received (for reliable channel only)"""
        self.acks_received += 1

    def on_stats_sync_rtt(self, rtt_ms: int, recv_ts_ms: int):
        """
        Track RTT from STATS_SYNC messages (for client-side metrics).
        This allows clients to estimate latency/jitter even without receiving app data.
        
        Args:
            rtt_ms: Round-trip time in milliseconds
            recv_ts_ms: Local receive timestamp in milliseconds
        """
        # DEBUG: Print to verify this is being called
        print(f"[DEBUG metrics.py] on_stats_sync_rtt called: rtt_ms={rtt_ms}, samples={self._stats_sync_samples}, latency_sum={self.latency_sum}")
        
        # Track first and last receive times for throughput calculation
        if self.first_recv_ts is None:
            self.first_recv_ts = recv_ts_ms
        self.last_recv_ts = recv_ts_ms
        
        # Use RTT as transit time for latency calculation
        # RTT is bidirectional, so divide by 2 for one-way latency estimate
        transit = rtt_ms / 2.0
        self.latency_sum += transit
        
        # Update jitter using RFC 3550 formula
        if self.last_transit is not None:
            d = transit - self.last_transit
            self.jitter += (abs(d) - self.jitter) / 16.0
        self.last_transit = transit
        
        # Increment STATS_SYNC sample counter
        self._stats_sync_samples += 1

    def update_peer_stats(self, peer_sent: int, peer_received: int, 
                         peer_acks_sent: int = 0, peer_acks_received: int = 0):
        """Update peer statistics from STATS_SYNC message"""
        self.peer_sent = peer_sent
        self.peer_received = peer_received
        self.peer_acks_sent = peer_acks_sent
        self.peer_acks_received = peer_acks_received

    def on_recv(self, send_ts_ms: int, recv_ts_ms: int, payload_len: int):
        self.received += 1
        self.bytes_recv += payload_len

        if self.first_recv_ts is None:
            self.first_recv_ts = recv_ts_ms
        self.last_recv_ts = recv_ts_ms

        # Handle timestamp wraparound for latency calculation
        # Both timestamps are modulo 2^32, so we need to handle wraparound
        send_ts_32 = send_ts_ms & 0xFFFFFFFF
        recv_ts_32 = recv_ts_ms & 0xFFFFFFFF
        
        # Calculate transit time with wraparound handling
        if recv_ts_32 >= send_ts_32:
            transit = recv_ts_32 - send_ts_32
        else:
            # Wraparound occurred
            transit = (0x100000000 + recv_ts_32 - send_ts_32) & 0xFFFFFFFF
        
        # Sanity check: if transit time is too large, it's likely a wraparound issue
        # Cap at 1 hour (3600000 ms) which should be reasonable for any network
        if transit > 3600000:
            transit = recv_ts_ms - send_ts_ms  # Fall back to simple subtraction
            if transit < 0:
                transit = 0  # Invalid, treat as 0
        
        self.latency_sum += transit

        if self.last_transit is not None:
            d = transit - self.last_transit
            self.jitter += (abs(d) - self.jitter) / 16.0
        self.last_transit = transit

    def summary(self, channel_type: int = None):
        """
        Calculate symmetric metrics for both senders and receivers.
        
        Philosophy:
        - SENDER (client): Measures outbound data flow - throughput = bytes_sent / time
        - RECEIVER (server): Measures inbound data flow - throughput = bytes_recv / time
        - Both measure the SAME data flow from different perspectives
        - PDR compares sent vs successfully delivered (using peer stats)
        - Latency/jitter measured from actual packet timing or STATS_SYNC RTT
        """
        # Calculate average latency
        # Use STATS_SYNC samples if available (for clients), otherwise use received packets (for servers)
        total_samples = self.received + self._stats_sync_samples
        
        if total_samples == 0:
            avg_lat = 0.0
        else:
            avg_lat = self.latency_sum / total_samples

        # Calculate throughput based on role
        # SENDER (has sent > 0, received = 0): Use sent bytes over sent time window
        # RECEIVER (has received > 0): Use received bytes over received time window
        if self.sent > 0 and self.received == 0:
            # This is a SENDER - measure outbound throughput
            if self.first_sent_ts is None or self.last_sent_ts == self.first_sent_ts:
                duration_s = 0.0
            else:
                duration_s = (self.last_sent_ts - self.first_sent_ts) / 1000.0
            throughput = (self.bytes_sent / duration_s) if duration_s > 0 else 0.0
        else:
            # This is a RECEIVER - measure inbound throughput
            if self.first_recv_ts is None or self.last_recv_ts == self.first_recv_ts:
                duration_s = 0.0
            else:
                duration_s = (self.last_recv_ts - self.first_recv_ts) / 1000.0
            throughput = (self.bytes_recv / duration_s) if duration_s > 0 else 0.0
        
        # Calculate PDR based on channel type and role
        # PDR should answer: "What % of packets sent were successfully delivered?"
        if channel_type == CHAN_RELIABLE:
            # Reliable channel: PDR based on ACKs
            if self.sent > 0:
                # SENDER: What % of my packets were ACKed?
                # Use locally tracked acks_received for accurate real-time PDR
                pdr = (self.acks_received / self.sent) * 100.0
                peer_received_display = self.acks_received
            elif self.received > 0 and self.peer_sent > 0:
                # RECEIVER: What % of peer's packets did I successfully receive?
                pdr = (self.received / self.peer_sent) * 100.0
                peer_received_display = self.received
            else:
                pdr = 0.0
                peer_received_display = 0
        else:
            # Unreliable channel: PDR based on packet counts
            if self.sent > 0:
                # SENDER: What % of my packets did peer receive?
                pdr = (self.peer_received / self.sent) * 100.0 if self.peer_received > 0 else 0.0
                peer_received_display = self.peer_received
            elif self.received > 0 and self.peer_sent > 0:
                # RECEIVER: What % of peer's packets did I receive?
                pdr = (self.received / self.peer_sent) * 100.0
                peer_received_display = self.received
            else:
                pdr = 0.0
                peer_received_display = 0

        return {
            "avg_latency_ms": avg_lat,
            "jitter_ms": self.jitter,
            "throughput_Bps": throughput,
            "pdr_percent": pdr,
            "self_sent": self.sent,
            "self_received": self.received,
            "peer_sent": self.peer_sent,
            "peer_received": peer_received_display,
            "acks_received": self.acks_received,
        }

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
        self.acks_received = 0  # Track ACKs received (for reliable channel)

        # Peer counters (what peer told me via STATS_SYNC)
        self.peer_sent = 0
        self.peer_received = 0
        self.peer_acks_sent = 0      # How many ACKs peer has sent
        self.peer_acks_received = 0  # How many ACKs peer has received

        self.first_recv_ts = None
        self.last_recv_ts = None

        self.last_transit = None
        self.jitter = 0.0
        self.latency_sum = 0.0

    def on_sent(self):
        self.sent += 1

    def on_ack_received(self):
        """Track when an ACK is received (for reliable channel only)"""
        self.acks_received += 1

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
        if self.received == 0:
            avg_lat = 0.0
        else:
            avg_lat = self.latency_sum / self.received

        if self.first_recv_ts is None or self.last_recv_ts == self.first_recv_ts:
            duration_s = 0.0
        else:
            duration_s = (self.last_recv_ts - self.first_recv_ts) / 1000.0

        throughput = (self.bytes_recv / duration_s) if duration_s > 0 else 0.0
        
        # Calculate PDR based on channel type
        if channel_type == CHAN_RELIABLE:
            # Reliable channel: PDR = peer_acks_received / my_sent * 100
            # (How many of my DATA packets were ACKed by peer)
            if self.sent > 0:
                pdr = (self.peer_acks_received / self.sent) * 100.0
            else:
                pdr = 0.0
            peer_received_display = self.peer_acks_received
        else:
            # Unreliable channel: PDR = my_received / peer_sent * 100  
            # (How many packets peer sent that I actually received)
            if self.peer_sent > 0:
                pdr = (self.received / self.peer_sent) * 100.0
            else:
                pdr = 0.0
            # For unreliable channel, show how many unreliable packets peer received
            peer_received_display = self.peer_received

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

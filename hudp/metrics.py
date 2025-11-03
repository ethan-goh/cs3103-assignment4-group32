# metrics.py
import math

CHAN_RELIABLE = 1      # chan_type=1 for reliable
CHAN_UNRELIABLE = 0    # chan_type=0 for unreliable

class ChannelStats:
    def __init__(self):
        self.sent = 0
        self.received = 0
        self.bytes_recv = 0
        self.first_recv_ts = None
        self.last_recv_ts = None


        self.last_transit = None   
        self.jitter = 0.0          
        self.latency_sum = 0.0     

    def on_sent(self):
        self.sent += 1

    def on_recv(self, send_ts_ms: int, recv_ts_ms: int, payload_len: int):
        self.received += 1
        self.bytes_recv += payload_len

        if self.first_recv_ts is None:
            self.first_recv_ts = recv_ts_ms
        self.last_recv_ts = recv_ts_ms

        transit = recv_ts_ms - send_ts_ms  


        self.latency_sum += transit

        if self.last_transit is not None:
            d = transit - self.last_transit
            self.jitter += (abs(d) - self.jitter) / 16.0
        self.last_transit = transit

    def summary(self):
        if self.received == 0:
            avg_lat = 0.0
        else:
            avg_lat = self.latency_sum / self.received

        if self.first_recv_ts is None or self.last_recv_ts == self.first_recv_ts:
            duration_s = 0.0
        else:
            duration_s = (self.last_recv_ts - self.first_recv_ts) / 1000.0

        throughput = (self.bytes_recv / duration_s) if duration_s > 0 else 0.0
        pdr = (self.received / self.sent * 100.0) if self.sent > 0 else 0.0

        return {
            "avg_latency_ms": avg_lat,
            "jitter_ms": self.jitter,
            "throughput_Bps": throughput,
            "pdr_percent": pdr,
        }

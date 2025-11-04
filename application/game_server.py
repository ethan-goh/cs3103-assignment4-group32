import argparse
import json
import time

from hudp import metrics
from hudp.api import GameNet


def run_server(listen_host: str, listen_port: int) -> None:
    """
    Receive packets from GameNet and print logs with seqno, channel type,
    timestamps, retransmissions, RTT, etc.
    """

    gn = GameNet(local_addr=(listen_host, listen_port))

    print(f"[SERVER] Listening on {listen_host}:{listen_port}")
    print("Press Ctrl+C to stop.\n")
    client_stats = None
    try:
        while True:
            result = gn.recv(timeout=None)  
            if result is None:
                continue

            payload_bytes, meta = result
            now_ms = int(time.time() * 1000)
            

            try:
                payload = json.loads(payload_bytes.decode("utf-8"))
            except Exception:
                payload = {"raw": payload_bytes.hex()}

            seq_no = meta.get("seq_no")
            chan_type = meta.get("chan_type")
            ts_send = meta.get("ts_send_ms")
            rtt_ms = meta.get("rtt_ms")
            num_retx = meta.get("retransmissions")
            from_addr = meta.get("from_addr")

            print(
                "[SERVER RECV] "
                f"time_local={now_ms} "
                f"from={from_addr} "
                f"seq={seq_no} chan={chan_type} "
                f"ts_send={ts_send} rtt_ms={rtt_ms} "
                f"retransmissions={num_retx}\n"
                f"    payload={payload}"
            )

    except KeyboardInterrupt:
        print("\n[SERVER] Stopping...")
        metrics = gn.get_metrics()
        # === PRINT METRICS HERE ===
        print("\n=== H-UDP METRICS (per channel) ===")
        for chan_type, stats in metrics.items():
            name = "RELIABLE" if chan_type == 1 else "UNRELIABLE"
            print(f"\nChannel {chan_type} ({name})")
            print(f"  Avg latency:   {stats['avg_latency_ms']:.2f} ms")
            print(f"  Jitter (RFC3550-style): {stats['jitter_ms']:.2f} ms")
            print(f"  Throughput:    {stats['throughput_Bps']:.2f} B/s")
            print(f"  PDR:           {stats['pdr_percent']:.2f} %")
            print(f"  Packets sent:  {stats['self_sent']}")
            print(f"  Packets recv:  {stats['self_received']}")
    finally:
        gn.close()


def main():
    parser = argparse.ArgumentParser(description="H-UDP Game Server (receiver)")
    parser.add_argument("--listen-host", default="0.0.0.0")
    parser.add_argument("--listen-port", type=int, default=5000)
    args = parser.parse_args()

    run_server(args.listen_host, args.listen_port)


if __name__ == "__main__":
    main()

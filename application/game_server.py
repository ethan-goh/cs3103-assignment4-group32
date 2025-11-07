import argparse
import json
import signal
import sys
import time

from hudp import metrics
from hudp.api import GameNet

# Global flag for graceful shutdown
shutdown_flag = False

def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_flag
    print("\n[SERVER] Shutdown signal received, cleaning up...")
    shutdown_flag = True


def run_server(listen_host: str, listen_port: int, debug: bool = False, 
               metrics_interval: float = 5.0, rto_ms: int = 50, 
               skip_threshold_ms: int = 200, adaptive: bool = False) -> None:
    """
    Receive packets from GameNet and optionally print logs.
    
    Args:
        listen_host: Host to listen on
        listen_port: Port to listen on
        debug: If True, print verbose packet logs
        metrics_interval: How often to print metrics (in seconds)
        rto_ms: Retransmission timeout in milliseconds
        skip_threshold_ms: Skip threshold in milliseconds
        adaptive: Enable adaptive parameter tuning
    """
    global shutdown_flag
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # kill command

    gn = GameNet(local_addr=(listen_host, listen_port),
                 rto_ms=rto_ms, skip_threshold_ms=skip_threshold_ms,
                 adaptive=adaptive)

    print(f"[SERVER] Listening on {listen_host}:{listen_port}")
    if adaptive:
        print("[SERVER] Adaptive mode enabled - RTO and skip threshold will adjust based on network conditions")
    if debug:
        print("[SERVER] Debug mode enabled - verbose logging on")
    print("Press Ctrl+C to stop.\n")

    last_metrics_time = time.time()

    try:
        while not shutdown_flag:
            result = gn.recv(timeout=0.1)  # Short timeout to check metrics periodically
            if result is None:
                # Check if we should print metrics
                current_time = time.time()
                if current_time - last_metrics_time >= metrics_interval:
                    last_metrics_time = current_time
                    print_metrics(gn, "[SERVER]")
                continue

            payload_bytes, meta = result
            now_ms = int(time.time() * 1000)

            if debug:
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

            # Check if we should print metrics
            current_time = time.time()
            if current_time - last_metrics_time >= metrics_interval:
                last_metrics_time = current_time
                print_metrics(gn, "[SERVER]")

    except KeyboardInterrupt:
        pass  # Signal handler already set shutdown_flag
    except Exception as e:
        print(f"\n[SERVER] Error: {e}")
    finally:
        print("\n[SERVER] Stopping...")
        # Print final metrics
        print_metrics(gn, "[SERVER] FINAL")
        gn.close()


def print_metrics(gn: GameNet, prefix: str = ""):
    """Print current metrics in a compact format."""
    metrics_data = gn.get_metrics()
    timestamp = time.strftime("%H:%M:%S")
    
    print(f"\n{prefix} METRICS @ {timestamp}")
    for chan_type, stats in metrics_data.items():
        name = "REL" if chan_type == 1 else "UNREL"
        print(
            f"  {name}: "
            f"PDR={stats['pdr_percent']:.1f}% "
            f"Lat={stats['avg_latency_ms']:.1f}ms "
            f"Jitter={stats['jitter_ms']:.1f}ms "
            f"Thru={stats['throughput_Bps']:.0f}B/s "
            f"Sent={stats['self_sent']} "
            f"Recv={stats['self_received']}"
        )


def main():
    parser = argparse.ArgumentParser(description="H-UDP Game Server (receiver)")
    parser.add_argument("--listen-host", default="0.0.0.0")
    parser.add_argument("--listen-port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true", 
                        help="Enable verbose debug logging")
    parser.add_argument("--metrics-interval", type=float, default=5.0,
                        help="Interval for printing metrics (seconds)")
    parser.add_argument("--rto", type=int, default=50,
                        help="Retransmission timeout in milliseconds (default: 50)")
    parser.add_argument("--skip-threshold", type=int, default=200,
                        help="Skip threshold for lost packets in milliseconds (default: 200)")
    parser.add_argument("--adaptive", action="store_true",
                        help="Enable adaptive RTO and skip threshold tuning")
    args = parser.parse_args()

    run_server(args.listen_host, args.listen_port, 
               debug=args.debug, metrics_interval=args.metrics_interval,
               rto_ms=args.rto, skip_threshold_ms=args.skip_threshold,
               adaptive=args.adaptive)


if __name__ == "__main__":
    main()

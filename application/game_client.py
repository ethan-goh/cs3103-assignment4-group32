import argparse
import json
import random
import signal
import sys
import time

from hudp.api import GameNet

# Global flag for graceful shutdown
shutdown_flag = False

def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_flag
    print("\n[CLIENT] Shutdown signal received, cleaning up...")
    shutdown_flag = True


def run_client(server_host: str,
               server_port: int,
               send_rate: float,
               reliable_prob: float,
               debug: bool = False,
               metrics_interval: float = 5.0,
               rto_ms: int = 50,
               skip_threshold_ms: int = 200,
               adaptive: bool = False) -> None:
    """
    Periodically send mock game packets to the server, randomly marking each as
    reliable or unreliable, via the GameNet API.
    
    Args:
        server_host: Server hostname/IP
        server_port: Server port
        send_rate: Packets per second
        reliable_prob: Probability that a packet is marked reliable
        debug: If True, print verbose packet logs
        metrics_interval: How often to print metrics (in seconds)
        rto_ms: Retransmission timeout in milliseconds
        skip_threshold_ms: Skip threshold for lost packets in milliseconds
        adaptive: Enable adaptive parameter tuning
    """
    global shutdown_flag
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # kill command

    gn = GameNet(local_addr=("0.0.0.0", 0),
                 remote_addr=(server_host, server_port),
                 rto_ms=rto_ms, skip_threshold_ms=skip_threshold_ms,
                 adaptive=adaptive)

    msg_id = 0
    interval = 1.0 / send_rate

    print(f"[CLIENT] Sending to {server_host}:{server_port} at {send_rate} pkt/s")
    print(f"[CLIENT] Reliable probability = {reliable_prob:.2f}")
    if adaptive:
        print("[CLIENT] Adaptive mode enabled - RTO and skip threshold will adjust based on network conditions")
    if debug:
        print("[CLIENT] Debug mode enabled - verbose logging on")
    print("Press Ctrl+C to stop.\n")

    last_metrics_time = time.time()

    try:
        while not shutdown_flag:
            msg_id += 1
            now_ms = int(time.time() * 1000)
            is_reliable = random.random() < reliable_prob
            chan_type = 1 if is_reliable else 0  # 1=reliable, 0=unreliable

            payload_obj = {
                "msg_id": msg_id,
                "ts_client_send_ms": now_ms,
                "reliable": is_reliable,
                "player_id": 1,
                "x": random.randint(0, 100),
                "y": random.randint(0, 100),
            }
            payload_bytes = json.dumps(payload_obj).encode("utf-8")

            seq_no = gn.send(payload_bytes,
                             reliable=is_reliable,
                             channel_id=chan_type)

            # Note: send() now automatically retries if buffer is full
            # Only returns -1 if buffer is still full after retries
            if is_reliable and seq_no == -1:
                if debug:
                    print(
                        f"[CLIENT SEND FAILED] msg_id={msg_id} "
                        f"chan={chan_type} - BUFFER FULL (even after retries), packet dropped"
                    )
            else:
                if debug:
                    print(
                        f"[CLIENT SEND] msg_id={msg_id} "
                        f"seq={seq_no} chan={chan_type} "
                        f"reliable={is_reliable} ts={now_ms}"
                    )

            # Check if we should print metrics
            current_time = time.time()
            if current_time - last_metrics_time >= metrics_interval:
                last_metrics_time = current_time
                print_metrics(gn, "[CLIENT]")

            time.sleep(interval)

    except KeyboardInterrupt:
        pass  # Signal handler already set shutdown_flag
    except Exception as e:
        print(f"\n[CLIENT] Error: {e}")
    finally:
        print("\n[CLIENT] Stopping...")
        # Print final metrics
        print_metrics(gn, "[CLIENT] FINAL")
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
    parser = argparse.ArgumentParser(description="H-UDP Game Client (sender)")
    parser.add_argument("--server-host", default="127.0.0.1")
    parser.add_argument("--server-port", type=int, default=5000)
    parser.add_argument("--rate", type=float, default=20.0,
                        help="packets per second")
    parser.add_argument("--reliable-prob", type=float, default=0.5,
                        help="probability a packet is marked reliable")
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

    run_client(
        server_host=args.server_host,
        server_port=args.server_port,
        send_rate=args.rate,
        reliable_prob=args.reliable_prob,
        debug=args.debug,
        metrics_interval=args.metrics_interval,
        rto_ms=args.rto,
        skip_threshold_ms=args.skip_threshold,
        adaptive=args.adaptive,
    )


if __name__ == "__main__":
    main()

import argparse
import json
import random
import time

from hudp.api import GameNet


def run_client(server_host: str,
               server_port: int,
               send_rate: float,
               reliable_prob: float) -> None:
    """
    Periodically send mock game packets to the server, randomly marking each as
    reliable or unreliable, via the GameNet API.
    """

    gn = GameNet(local_addr=("0.0.0.0", 0),
                 remote_addr=(server_host, server_port))

    msg_id = 0
    interval = 1.0 / send_rate

    print(f"[CLIENT] Sending to {server_host}:{server_port} at {send_rate} pkt/s")
    print(f"[CLIENT] Reliable probability = {reliable_prob:.2f}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
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

            print(
                f"[CLIENT SEND] msg_id={msg_id} "
                f"seq={seq_no} chan={chan_type} "
                f"reliable={is_reliable} ts={now_ms}"
            )

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[CLIENT] Stopping...")
    finally:
        gn.close()


def main():
    parser = argparse.ArgumentParser(description="H-UDP Game Client (sender)")
    parser.add_argument("--server-host", default="127.0.0.1")
    parser.add_argument("--server-port", type=int, default=5000)
    parser.add_argument("--rate", type=float, default=20.0,
                        help="packets per second")
    parser.add_argument("--reliable-prob", type=float, default=0.5,
                        help="probability a packet is marked reliable")
    args = parser.parse_args()

    run_client(
        server_host=args.server_host,
        server_port=args.server_port,
        send_rate=args.rate,
        reliable_prob=args.reliable_prob,
    )


if __name__ == "__main__":
    main()

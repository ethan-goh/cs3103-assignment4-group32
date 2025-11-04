import argparse
import json
import time

from hudp.api import GameNet


def run_server(listen_host: str, listen_port: int) -> None:
    """
    Receive packets from GameNet and print logs with seqno, channel type,
    timestamps, retransmissions, RTT, etc.
    """

    gn = GameNet(local_addr=(listen_host, listen_port))

    print(f"[SERVER] Listening on {listen_host}:{listen_port}")
    print("Press Ctrl+C to stop.\n")

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
        # === PRINT METRICS HERE ===

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

# HUDP Protocol Implementation Contracts

## Overview

This document defines the contracts for implementing a minimal reliable UDP protocol with Selective Repeat (SR) for game networking.

---

## 1. `packet.py` — Wire Format (Encode/Decode)

### Purpose

Define the minimal header and provide functions to build/parse UDP payloads for DATA and ACK frames.
A 16-bit checksum is included for integrity verification across header + payload.

### Required Functions (Exposed API)

#### `encode_data(ts_send: int, seq_no: int, chan_type: int, payload: bytes) -> bytes`

Build a DATA frame (reliable if chan_type = 1).
Computes and embeds checksum.

#### `encode_ack(seq_no: int, ts_send: int, chan_type: int = 0) -> bytes`

Build an ACK-only frame (no payload).
Checksum computed the same way as for DATA frames.

#### `decode_frame(datagram: bytes) -> tuple[str, dict, bytes]`

Parse bytes → ("DATA" | "ACK", header_dict, payload).
Verifies checksum. Returns header_dict with "valid": True/False.

#### `now_ms() -> int`

Return current timestamp in milliseconds.

---

## 2. `sr_sender.py` — Selective Repeat Sender

### Purpose

Maintain SR window of unacked DATA, start per-packet timers, and retransmit on timeout. Accept ACKs to slide the window.

### Required Functions (Exposed API)

#### `queue_reliable(data: bytes, channel_id: int) -> int`

Reserve a new sequence number and stage a DATA packet for sending.  
**Returns:** The sequence number.

#### `next_frames(now_ms: int) -> list[tuple[int, bytes]]`

Decide which reliable DATA frames to send right now (new + timed-out retransmissions).  
**Returns:** List of `(seq, payload)` tuples.

#### `on_ack(ack_no: int, now_ms: int) -> list[tuple[int, int]]`

Mark `ack_no` as delivered; slide base window.  
**Returns:** List of `(seq, rtt_ms)` for any acked packet (caller may ignore RTT).

### Minimal Logic Assumptions

- **Fixed window size:** e.g., 128
- **Simple RTO:** Start at 200 ms; optional backoff; keep it simple
- **One timer per packet:** If `now - last_send >= rto`, retransmit

---

## 3. `sr_receiver.py` — Selective Repeat Receiver

### Purpose

Buffer out-of-order DATA, deliver in order, and skip the head if it waits longer than _t_ ms. Queue an ACK for every reliable DATA received (including duplicates).

### Required Functions (Exposed API)

#### `on_data(seq: int, ts_send: int, payload: bytes, now_ms: int) -> list[tuple[int, bytes]]`

Insert DATA packet into buffer.  
**Returns:** List of `(seq, payload)` that became deliverable to the app now (in order, with skip-threshold applied).

#### `queue_ack(seq: int)` _(internal)_

Push sequence number into an internal ACK outbox (IO will drain).

#### `pop_ack() -> int | None`

Pop the next `ack_no` to send as an ACK-only frame.  
**Returns:** Acknowledgment number or `None` if empty.  
_(IO calls this repeatedly until None)_

---

## 4. `io_async.py` — UDP I/O Adapter (Minimal)

### Purpose

The only module that talks to the UDP socket.

### Event Loops

#### Send Loop

- Asks the sender for frames (`next_frames`) and transmits them
- Drains the receiver's ACK outbox and sends ACK-only frames

#### Receive Loop

- Reads datagrams, decodes them
- Routes DATA to the receiver and ACK to the sender

### Implementation Note

For true minimalism, you can implement this with blocking sockets on two threads (send & recv). If you prefer `asyncio`, keep tiny tasks.

### Required Functions (Exposed API)

#### `start()` / `close()`

Start/stop I/O loops.

#### `send_reliable(data: bytes, channel_id: int) -> int`

Enqueue DATA via `SRSender.queue_reliable`.  
**Returns:** Sequence number.

#### `send_unreliable(data: bytes)`

Build and send an unreliable DATA frame immediately.

#### `recv(timeout: float | None) -> tuple[bytes, dict] | None`

Blocking pop of delivered app messages (from receiver's outputs).  
**Returns:** `(payload, metadata)` tuple or `None` on timeout.

---

## 5. `api.py` — Minimal GameNet Façade

### Purpose

Simple class the app uses. Internally owns `UDPIO`.

### Required Functions (Exposed API)

#### `send(data: bytes, reliable: bool = False, channel_id: int = 0) -> int | None`

Send data over the network.  
**Returns:**

- For reliable: sequence number
- For unreliable: `None`

#### `recv(timeout: float | None = None) -> tuple[bytes, dict] | None`

Block until a message is delivered or timeout.  
**Returns:** `(payload, metadata)` tuple or `None`.

#### `close()`

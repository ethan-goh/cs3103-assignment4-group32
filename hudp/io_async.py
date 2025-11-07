"""
io_async.py - UDP I/O Adapter

This is the ONLY module that touches the UDP socket.
It runs two concurrent loops:
  1. SEND LOOP: Transmits new/retransmitted DATA frames and drains ACKs
  2. RECV LOOP: Receives datagrams and routes them to sender/receiver

Key Design:
- Uses threading (not asyncio) for simplicity
- Maintains a queue of delivered messages for the application
- Coordinates between sr_sender, sr_receiver, and packet modules
"""

import socket
import threading
import queue
import time
import json
from typing import Optional, Tuple, Dict

from . import packet
from .sr_sender import SRSender
from .sr_receiver import SRReceiver
from .adaptive import AdaptiveParameterController


class UDPIO:
    """
    UDP I/O adapter that manages socket operations and coordinates
    the Selective Repeat sender and receiver.
    """
    
    def __init__(self, local_addr: Tuple[str, int], remote_addr: Optional[Tuple[str, int]] = None, 
                 send_interval_ms: int = 10, stats_sync_interval: float = 2.0,
                 rto_ms: int = 50, skip_threshold_ms: int = 200, adaptive: bool = False):
        """
        Initialize the UDP I/O adapter.
        
        Args:
            local_addr: (host, port) tuple for binding the local socket
            remote_addr: (host, port) tuple for the remote endpoint
                        Can be None for server mode (will be set on first recv)
            send_interval_ms: How often the send loop runs (milliseconds)
            stats_sync_interval: How often to exchange statistics (seconds)
            rto_ms: Retransmission timeout in milliseconds (default: 50)
            skip_threshold_ms: Skip threshold for lost packets in milliseconds (default: 200)
            adaptive: Enable adaptive RTO and skip threshold (default: False)
        """
        self.local_addr = local_addr
        self.remote_addr = remote_addr
        self.send_interval_ms = send_interval_ms
        self.stats_sync_interval = stats_sync_interval
        
        # Create UDP socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(local_addr)
        
        # Adaptive parameter control
        self.adaptive = adaptive
        self.adaptive_controller = None
        if adaptive:
            self.adaptive_controller = AdaptiveParameterController(
                initial_rto=rto_ms,
                initial_skip=skip_threshold_ms
            )
        
        # SR protocol components with configurable parameters
        self.sender = SRSender(rto_ms=rto_ms, skip_threshold_ms=skip_threshold_ms)
        self.receiver = SRReceiver(skip_threshold_ms=skip_threshold_ms)
        
        # Store ACK information for metadata population
        # Maps seq_no -> (rtt_ms, retransmissions)
        self.ack_info = {}
        
        # Queue for delivering messages to the application
        # Each item is (payload: bytes, metadata: dict)
        self.delivered_queue = queue.Queue()
        
        # Callback for metrics tracking (set by api.py)
        self.metrics_callback = None
        
        # Statistics tracking for STATS_SYNC
        self.stats_lock = threading.Lock()
        self.reliable_sent = 0
        self.unreliable_sent = 0
        self.reliable_received = 0
        self.unreliable_received = 0
        self.acks_sent = 0      # Track ACKs sent (server perspective)
        self.acks_received = 0  # Track ACKs received (client perspective)
        self.peer_stats = {
            'reliable_sent': 0,
            'unreliable_sent': 0,
            'reliable_received': 0,
            'unreliable_received': 0,
            'acks_sent': 0,
            'acks_received': 0
        }
        self.last_stats_sync = 0
        
        # Threading control
        self.running = False
        self.send_thread: Optional[threading.Thread] = None
        self.recv_thread: Optional[threading.Thread] = None

    def set_metrics_callback(self, callback):
        """Set callback function for metrics tracking."""
        self.metrics_callback = callback
        
    def get_local_stats(self):
        """Get current local statistics."""
        with self.stats_lock:
            return {
                'reliable_sent': self.reliable_sent,
                'unreliable_sent': self.unreliable_sent,
                'reliable_received': self.reliable_received,
                'unreliable_received': self.unreliable_received,
                'acks_sent': self.acks_sent,
                'acks_received': self.acks_received
            }
    
    def get_peer_stats(self):
        """Get current peer statistics."""
        with self.stats_lock:
            return self.peer_stats.copy()
    
    def send_final_stats(self):
        """Send final statistics before shutdown."""
        if self.remote_addr is None:
            return
        
        with self.stats_lock:
            stats_frame = packet.encode_stats_sync(
                reliable_sent=self.reliable_sent,
                unreliable_sent=self.unreliable_sent,
                reliable_received=self.reliable_received,
                unreliable_received=self.unreliable_received,
                acks_sent=self.acks_sent,
                acks_received=self.acks_received
            )
        
        # Send final stats (fire-and-forget)
        try:
            self.socket.sendto(stats_frame, self.remote_addr)
        except:
            pass  # Best effort on shutdown
        
    def start(self):
        """
        Start the I/O loops (send and receive threads).
        Must be called before using send/recv functions.
        """
        if self.running:
            return  # Already started
            
        self.running = True
        
        # Start receive loop thread
        self.recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self.recv_thread.start()
        
        # Start send loop thread
        self.send_thread = threading.Thread(target=self._send_loop, daemon=True)
        self.send_thread.start()
        
    def close(self):
        """
        Stop the I/O loops and close the socket.
        """
        if not self.running:
            return
        
        # Send final stats before closing
        self.send_final_stats()
            
        self.running = False
        
        # Wait for threads to finish (with timeout)
        if self.recv_thread:
            self.recv_thread.join(timeout=1.0)
        if self.send_thread:
            self.send_thread.join(timeout=1.0)
            
        # Close the socket
        self.socket.close()
        
    def send_reliable(self, data: bytes, channel_id: int = 0) -> int:
        """
        Send data reliably using Selective Repeat.
        
        Args:
            data: The payload to send
            channel_id: Channel identifier (for the packet header)
            
        Returns:
            Sequence number of the queued packet, or -1 if buffer is full
        """
        # Queue the data in the SR sender's window
        seq = self.sender.queue_reliable(data, channel_id)
        
        if seq != -1:  # Successfully queued
            with self.stats_lock:
                self.reliable_sent += 1
            
            # Track reliable send in metrics
            if self.metrics_callback:
                now_ms = int(time.time() * 1000)
                self.metrics_callback('sent', {
                    'channel': 1,  # Channel 1 = reliable
                    'payload_len': len(data),
                    'ts_ms': now_ms
                })
        
        return seq
        
    def send_unreliable(self, data: bytes):
        """
        Send data unreliably (fire-and-forget, no ACKs, no retransmissions).
        
        Args:
            data: The payload to send
        """
        if self.remote_addr is None:
            # Can't send if we don't know the remote address yet
            return
            
        # Build an unreliable DATA frame
        # chan_type=0 for unreliable, seq_no=0 (ignored for unreliable)
        now = packet.now_ms()
        frame = packet.encode_data(
            ts_send=now,
            seq_no=0,           # Sequence doesn't matter for unreliable
            chan_type=0,        # Unreliable channel (chan_type=0)
            payload=data
        )
        
        # Send immediately (fire-and-forget)
        self.socket.sendto(frame, self.remote_addr)
        
        # Track unreliable send
        with self.stats_lock:
            self.unreliable_sent += 1
        
        # Track unreliable send in metrics
        if self.metrics_callback:
            self.metrics_callback('sent', {
                'channel': 0,  # Channel 0 = unreliable
                'payload_len': len(data),
                'ts_ms': now
            })


    def recv(self, timeout: Optional[float] = None) -> Optional[Tuple[bytes, Dict]]:
        """
        Receive a delivered message (blocking).
        
        This pops from the queue of messages that have been:
        - Received, reordered, and delivered by sr_receiver (for reliable)
        - Received directly (for unreliable)
        
        Args:
            timeout: Maximum time to wait in seconds (None = wait forever)
            
        Returns:
            (payload, metadata) tuple, or None if timeout expires
            metadata includes: {'reliable': bool, 'seq': int, 'ts_send': int, ...}
        """
        try:
            return self.delivered_queue.get(timeout=timeout)
        except queue.Empty:
            return None
            
    # =========================================================================
    # SEND LOOP - Runs continuously to transmit packets and ACKs
    # =========================================================================
    
    def _send_loop(self):
        """
        Send loop: Continuously transmits DATA frames and ACKs.
        
        This loop:
        1. Asks sr_sender for frames to send (new + timed-out retransmissions)
        2. Transmits those DATA frames
        3. Drains ACKs from sr_receiver and sends ACK-only frames
        4. Sends periodic STATS_SYNC messages
        5. Sleeps briefly before repeating
        """
        while self.running:
            now = packet.now_ms()
            
            # Skip sending if we don't have a remote address yet
            if self.remote_addr is None:
                time.sleep(self.send_interval_ms / 1000.0)
                continue
            
            # Try to apply any pending adaptive RTO updates if window usage is low
            if self.adaptive and self.adaptive_controller:
                window_usage = len(self.sender.unacked) / self.sender.window_size if self.sender.window_size > 0 else 0.0
                self.adaptive_controller.try_apply_pending(window_usage)
            
            # ---- STEP 1: Send DATA frames (new + retransmissions) ----
            # Ask the SR sender what needs to be sent right now
            frames_to_send = self.sender.next_frames(now)
            
            for seq, frame in frames_to_send:
                # Frame is already encoded by sr_sender with current timestamp
                # Just send it directly
                self.socket.sendto(frame, self.remote_addr)
            
            # ---- STEP 2: Drain and send ACKs ----
            # The receiver queues an ACK for every reliable DATA it receives
            # We need to send those ACKs back to the remote sender
            while True:
                ack_no = self.receiver.pop_ack()
                if ack_no is None:
                    break  # No more ACKs to send
                    
                # Build an ACK-only frame
                ack_frame = packet.encode_ack(
                    seq_no=ack_no,
                    ts_send=now,
                    chan_type=1  # ACK for reliable channel
                )
                
                # Transmit the ACK
                self.socket.sendto(ack_frame, self.remote_addr)
                
                # Track ACK as sent (server perspective)
                with self.stats_lock:
                    self.acks_sent += 1
                
                # Note: ACKs are protocol overhead, not counted in application-level metrics

            # ---- STEP 3: Send periodic STATS_SYNC ----
            current_time = time.time()
            if current_time - self.last_stats_sync >= self.stats_sync_interval:
                self.last_stats_sync = current_time
                
                with self.stats_lock:
                    stats_frame = packet.encode_stats_sync(
                        reliable_sent=self.reliable_sent,
                        unreliable_sent=self.unreliable_sent,
                        reliable_received=self.reliable_received,
                        unreliable_received=self.unreliable_received,
                        acks_sent=self.acks_sent,
                        acks_received=self.acks_received
                    )
                
                # Send STATS_SYNC as reliable (will be handled by SR)
                self.socket.sendto(stats_frame, self.remote_addr)

            # ---- STEP 4: Sleep briefly ----
            # Don't hog the CPU; sleep for the configured interval
            time.sleep(self.send_interval_ms / 1000.0)
            
    # =========================================================================
    # RECV LOOP - Runs continuously to receive and route packets
    # =========================================================================
    
    def _recv_loop(self):
        """
        Receive loop: Continuously receives datagrams and routes them.
        
        This loop:
        1. Receives a datagram from the UDP socket (blocking)
        2. Decodes it into frame_type, header, payload
        3. Routes DATA frames to the appropriate handler:
           - Reliable DATA → sr_receiver.on_data()
           - Unreliable DATA → directly to app queue
        4. Routes ACK frames to sr_sender.on_ack()
        5. Routes STATS_SYNC frames to stats handler
        """
        # Set a timeout on the socket so we can periodically check self.running
        self.socket.settimeout(0.1)  # 100ms timeout
        
        while self.running:
            try:
                # ---- STEP 1: Receive a datagram ----
                datagram, addr = self.socket.recvfrom(65536)  # Max UDP size
                
                # If we don't have a remote address yet (server mode), set it now
                if self.remote_addr is None:
                    self.remote_addr = addr
                
                # ---- STEP 2: Decode the frame ----
                frame_type, header, payload = packet.decode_frame(datagram)
                now = packet.now_ms()
                
                # ---- STEP 2.5: Validate checksum ----
                # Drop frames with invalid checksum (corrupted data)
                if not header.get('valid', False):
                    # Silently drop corrupted frames
                    continue
                
                # ---- STEP 3: Route based on frame type ----
                
                if frame_type == "STATS_SYNC":
                    # Handle statistics synchronization (invisible to application)
                    try:
                        stats_data = json.loads(payload.decode('utf-8'))
                        
                        # Calculate RTT from STATS_SYNC (for client-side latency estimation)
                        peer_ts = stats_data.get('timestamp', 0)
                        if peer_ts > 0:
                            # Handle timestamp wraparound (both timestamps are modulo 2^32)
                            peer_ts_32 = peer_ts & 0xFFFFFFFF
                            now_32 = now & 0xFFFFFFFF
                            
                            if now_32 >= peer_ts_32:
                                rtt_ms = now_32 - peer_ts_32
                            else:
                                # Wraparound occurred
                                rtt_ms = (0x100000000 + now_32 - peer_ts_32) & 0xFFFFFFFF
                            
                            # DEBUG: Uncomment to see RTT values
                            print(f"[DEBUG STATS_SYNC] peer_ts={peer_ts}, now={now}, rtt_ms={rtt_ms}, callback={self.metrics_callback is not None}")
                            
                            # Sanity check: RTT should be reasonable (< 1 hour)
                            # NOTE: On localhost, RTT can be 0-1ms, which is valid
                            if rtt_ms < 3600000:
                                # Notify metrics callback about RTT (use for both channels)
                                if self.metrics_callback:
                                    self.metrics_callback('stats_sync_rtt', {'rtt_ms': rtt_ms, 'recv_ts': now})
                                
                                # Update adaptive controller if enabled
                                if self.adaptive and self.adaptive_controller:
                                    # Calculate loss rate from peer stats (FIX 5: use unique packets)
                                    # Use sequence numbers to get unique packets sent (not including retransmissions)
                                    # The sender's next_seq tracks how many unique packets have been queued
                                    unique_sent_reliable = self.sender.next_seq  # Total unique reliable packets sent
                                    
                                    # For loss calculation, use the reliable channel stats
                                    # (unreliable doesn't retransmit, so it doesn't suffer from the feedback loop)
                                    peer_received_reliable = stats_data.get('reliable_received', 0)
                                    
                                    # Calculate loss rate based on unique packets
                                    # Handle edge cases: division by zero, initial state
                                    if unique_sent_reliable > 0:
                                        loss_rate = max(0.0, min(1.0, 1.0 - (peer_received_reliable / unique_sent_reliable)))
                                    else:
                                        loss_rate = 0.0  # No packets sent yet, assume no loss
                                    
                                    # Calculate current SR window usage
                                    window_usage = len(self.sender.unacked) / self.sender.window_size if self.sender.window_size > 0 else 0.0
                                    
                                    # Sanity check: if loss rate is invalid, skip this update
                                    if loss_rate < 0 or loss_rate > 1:
                                        print(f"[ADAPTIVE WARNING] Invalid loss rate {loss_rate}, skipping update")
                                        continue
                                    
                                    # Update RTO based on RFC 6298 (skip threshold stays fixed)
                                    # Pass window_usage to allow safe updates only when window is not busy
                                    new_rto, fixed_skip = self.adaptive_controller.on_stats_sync(rtt_ms, loss_rate, window_usage)
                                    
                                    # Sanity check the new RTO
                                    if new_rto <= 0 or new_rto > 1000:
                                        print(f"[ADAPTIVE WARNING] Invalid RTO={new_rto}ms, skipping update")
                                        continue
                                    
                                    # Apply new RTO to sender only (skip threshold remains fixed)
                                    # This only affects RELIABLE channel retransmissions
                                    self.sender.update_rto(new_rto)
                                    # Do NOT update skip threshold - it stays at initial value
                                    # self.receiver.update_skip_threshold(fixed_skip)  # Skip this!
                        
                        with self.stats_lock:
                            self.peer_stats['reliable_sent'] = stats_data.get('reliable_sent', 0)
                            self.peer_stats['unreliable_sent'] = stats_data.get('unreliable_sent', 0)
                            self.peer_stats['reliable_received'] = stats_data.get('reliable_received', 0)
                            self.peer_stats['unreliable_received'] = stats_data.get('unreliable_received', 0)
                            self.peer_stats['acks_sent'] = stats_data.get('acks_sent', 0)
                            self.peer_stats['acks_received'] = stats_data.get('acks_received', 0)
                    except (json.JSONDecodeError, KeyError):
                        # Malformed STATS_SYNC, ignore
                        pass
                    # Don't deliver STATS_SYNC to application
                    continue
                
                elif frame_type == "DATA":
                    # Check if reliable or unreliable
                    chan_type = header.get('chan_type', 0)
                    
                    if chan_type == 1:  # Reliable DATA (chan_type=1)
                        # Track received
                        with self.stats_lock:
                            self.reliable_received += 1
                        
                        # Route to SR receiver
                        seq = header['seq_no']
                        ts_send = header['ts_send']
                        
                        # on_data returns a list of (seq, payload) that are now deliverable
                        deliverable = self.receiver.on_data(seq, ts_send, payload, now)
                        
                        # Push all newly-deliverable messages to the app queue
                        for del_seq, del_payload in deliverable:
                            # Get ACK info if available (RTT and retransmissions)
                            rtt_ms, retransmissions = self.ack_info.pop(del_seq, (None, None))
                            
                            metadata = {
                                'reliable': True,
                                'seq_no': del_seq,
                                'chan_type': chan_type,
                                'ts_send_ms': ts_send,
                                'recv_time': now,
                                'from_addr': addr,
                                'valid': True,  # Always True since we drop invalid frames
                                'rtt_ms': rtt_ms,
                                'retransmissions': retransmissions,
                            }
                            self.delivered_queue.put((del_payload, metadata))
                            
                    else:  # Unreliable DATA (chan_type=0)
                        # Track received
                        with self.stats_lock:
                            self.unreliable_received += 1
                        
                        # Deliver directly to app queue (no SR logic)
                        metadata = {
                            'reliable': False,
                            'seq_no': header.get('seq_no', 0),
                            'chan_type': chan_type,
                            'ts_send_ms': header.get('ts_send', 0),
                            'recv_time': now,
                            'from_addr': addr,
                            'valid': True,  # Always True since we drop invalid frames
                        }
                        self.delivered_queue.put((payload, metadata))
                        
                elif frame_type == "ACK":
                    # Route to SR sender first to see if this ACK is valid
                    ack_no = header['seq_no']
                    ack_results = self.sender.on_ack(ack_no, now)

                    # Only count ACK if it successfully acknowledged a new packet
                    if ack_results:  # Non-empty list means packet was newly acknowledged
                        # Track ACK as received (client perspective)
                        with self.stats_lock:
                            self.acks_received += 1
                        
                        # Track ACK in metrics for reliable channel
                        if self.metrics_callback:
                            self.metrics_callback('ack_received', 1)  # Channel 1 = reliable

                        # Store ACK info for when the packet is delivered
                        for seq, rtt, retx in ack_results:
                            self.ack_info[seq] = (rtt, retx)
                    
            except socket.timeout:
                # Timeout is expected; just loop again and check if still running
                continue
            except Exception as e:
                # Log error but keep running (you might want proper logging here)
                if self.running:
                    print(f"Error in recv loop: {e}")
                    
        # Loop exited (self.running became False)


# ==============================================================================
# EXPLANATION OF KEY DESIGN DECISIONS
# ==============================================================================

"""
1. WHY TWO THREADS?
   - Recv thread: Blocks on socket.recvfrom(), waiting for incoming packets
   - Send thread: Actively checks timers and sends packets/ACKs
   - These must run concurrently; you can't do both in one thread without
     complex non-blocking I/O or asyncio

2. WHY THE SEND LOOP RUNS CONTINUOUSLY?
   - SR timers are implicit: sender checks (now - last_send >= RTO)
   - We need to call next_frames() regularly to detect timeouts
   - ACKs from receiver need to be drained and sent continuously
   - Without this, retransmissions would never happen!

3. WHY THE DELIVERED QUEUE?
   - Decouples I/O timing from application timing
   - Receiver might deliver multiple packets in one on_data() call
     (e.g., packet 5 arrives and unblocks 6, 7, 8 from the buffer)
   - App can recv() at its own pace without blocking the I/O threads

4. THREAD SAFETY:
   - delivered_queue: queue.Queue is thread-safe
   - sender and receiver: Assumed to be called from one thread each
     (sender from send loop, receiver from recv loop)
   - If you need cross-thread access to sender/receiver, add locks

5. SOCKET TIMEOUT:
   - We set a 100ms timeout on recvfrom() so the recv loop can check
     self.running periodically
   - Without this, the thread would block forever on recvfrom() and
     never notice when we call close()

6. ERROR HANDLING:
   - Basic try-except around socket operations
   - In production, you'd want proper logging
   - Malformed packets are ignored (decode_frame would raise an exception)
   - CHECKSUM VALIDATION: Invalid frames (corrupted data) are silently dropped
     in the recv loop before reaching sr_receiver or the application queue

7. BUFFER FULL HANDLING:
   - send_reliable() returns -1 if sender.queue_reliable() returns -1
   - The API layer (api.py) can decide how to handle this
   - Could: block and retry, drop packet, raise exception, etc.
"""

"""
api.py - Minimal GameNet Façade

This is the clean API that applications use to send/receive messages.
It wraps the UDPIO layer and provides a simple, user-friendly interface.
"""

from typing import Optional, Tuple, Dict
from .io_async import UDPIO
from .metrics import ChannelStats, CHAN_RELIABLE, CHAN_UNRELIABLE


class GameNet:
    """
    GameNet API - Simple façade for reliable/unreliable UDP messaging.
    
    This class provides a clean interface for applications to use the
    H-UDP protocol without needing to understand the internal SR mechanics.
    """
    
    def __init__(self, local_addr: Tuple[str, int], remote_addr: Optional[Tuple[str, int]] = None,
                 send_interval_ms: int = 10):
        """
        Initialize the GameNet API.
        
        Args:
            local_addr: (host, port) tuple for the local endpoint
                       - For servers: ("0.0.0.0", port) to listen on all interfaces
                       - For clients: ("0.0.0.0", 0) for any available port, or specific port
            remote_addr: (host, port) tuple for the remote endpoint
                        - The address where packets will be sent
                        - For servers: Can be None (will be set on first packet received)
                        - For clients: Must specify the server address
            send_interval_ms: How often the send loop runs in milliseconds (default: 10)
        
        Example:
            # Server (remote_addr will be set on first packet)
            server = GameNet(local_addr=("0.0.0.0", 6000))
            
            # Client
            client = GameNet(local_addr=("0.0.0.0", 5000), remote_addr=("server_ip", 6000))
        """
        self.local_addr = local_addr
        self.remote_addr = remote_addr
        
        # Create the underlying I/O adapter
        self.io = UDPIO(
            local_addr=local_addr,
            remote_addr=remote_addr,
            send_interval_ms=send_interval_ms
        )
        self._stats = {
            CHAN_UNRELIABLE: ChannelStats(),
            CHAN_RELIABLE: ChannelStats(),
        }
        
        # Set up metrics callback for tracking ACK sends/receives
        self.io.set_metrics_callback(self._on_metrics_event)
        
        # Automatically start the I/O loops
        self.io.start()

    def _on_metrics_event(self, event_type: str, channel: int):
        """Handle metrics events from the I/O layer."""
        if channel not in self._stats:
            self._stats[channel] = ChannelStats()
            
        if event_type == 'sent':
            self._stats[channel].on_sent()
        elif event_type == 'ack_received':
            # Track ACKs received (for reliable channel only)
            if channel == CHAN_RELIABLE:
                self._stats[channel].on_ack_received()

    def send(self, data: bytes, reliable: bool = False, channel_id: int = 0, 
             max_retries: int = 10, retry_delay_ms: int = 10) -> Optional[int]:
        """
        Send data over the network.
        
        Args:
            data: The payload to send (bytes)
            reliable: If True, uses SR protocol (ACKs, retransmissions, ordering)
                     If False, fire-and-forget (no guarantees)
            channel_id: Channel identifier for reliable messages (default: 0)
                       Ignored for unreliable messages
            max_retries: Maximum number of retry attempts if buffer is full (default: 10)
                        Only applies to reliable sends
            retry_delay_ms: Delay in milliseconds between retry attempts (default: 10ms)
        
        Returns:
            - For reliable=True: Returns sequence number (int) if queued successfully,
                                or -1 if send buffer is still full after max_retries
            - For reliable=False: Returns None (fire-and-forget has no seq number)
        
        Example:
            # Reliable send with default retry (will retry up to 10 times)
            seq = gamenet.send(b"PLAYER_JOINED", reliable=True)
            if seq == -1:
                print("Send buffer full after retries, packet dropped")
            
            # Reliable send with custom retry behavior
            seq = gamenet.send(b"CRITICAL_DATA", reliable=True, max_retries=50, retry_delay_ms=5)
            
            # Unreliable send (frequent position updates, no retries needed)
            gamenet.send(b"pos:100,200", reliable=False)
        """
        # Use proper channel type for metrics (0=unreliable, 1=reliable)
        chan = 1 if reliable else 0
        if chan not in self._stats:
            self._stats[chan] = ChannelStats()

        if reliable:
            # Use Selective Repeat protocol with retry logic
            self._stats[chan].on_sent()
            seq = self.io.send_reliable(data, channel_id)
            
            # Retry if buffer is full
            retry_count = 0
            while seq == -1 and retry_count < max_retries:
                # Small delay before retry to allow buffer to drain
                import time
                time.sleep(retry_delay_ms / 1000.0)
                
                seq = self.io.send_reliable(data, channel_id)
                retry_count += 1
            
            return seq  # Returns seq number or -1 if still full after retries
        else:
            # Fire-and-forget (no ACKs, no retransmissions)
            self._stats[chan].on_sent()
            self.io.send_unreliable(data)
            return None  # Unreliable sends don't have sequence numbers
            
    def recv(self, timeout: Optional[float] = None) -> Optional[Tuple[bytes, Dict]]:
        """
        Receive a message from the network (blocking).
        
        This returns messages that have been:
        - For reliable: received, reordered, and delivered in sequence by SR receiver
        - For unreliable: received directly (no ordering guarantees)
        
        Args:
            timeout: Maximum time to wait in seconds
                    - None (default): Wait forever until a message arrives
                    - 0: Non-blocking, return immediately if no message
                    - >0: Wait up to this many seconds
        
        Returns:
            (payload, metadata) tuple if a message is available, or None on timeout
            
            metadata dict contains:
                'reliable': bool - Whether this was a reliable message
                'seq': int - Sequence number (meaningful for reliable messages)
                'ts_send': int - Sender's timestamp in milliseconds
                'recv_time': int - Local receive timestamp in milliseconds
        
        Example:
            # Blocking receive with timeout
            result = gamenet.recv(timeout=1.0)
            if result:
                payload, meta = result
                if meta['reliable']:
                    print(f"Reliable message seq={meta['seq']}: {payload}")
                else:
                    print(f"Unreliable message: {payload}")
            else:
                print("Timeout - no message received")
                
            # Non-blocking poll
            result = gamenet.recv(timeout=0)
            if result:
                payload, meta = result
                # Process message
        """
        result = self.io.recv(timeout)
        if result is None:
            return None

        payload, meta = result

        chan = meta.get("chan_type", CHAN_UNRELIABLE)
        if chan not in self._stats:
            self._stats[chan] = ChannelStats()

        ts_send = meta.get("ts_send_ms", 0)
        ts_recv = meta.get("recv_time", 0)
        self._stats[chan].on_recv(send_ts_ms=ts_send,
                                  recv_ts_ms=ts_recv,
                                  payload_len=len(payload))

        return payload, meta
    
    def get_metrics(self) -> Dict[int, Dict]:
        """
        Return per-channel performance metrics.

        Returns:
            {chan_type: {
                "avg_latency_ms": ...,
                "jitter_ms": ...,
                "throughput_Bps": ...,
                "pdr_percent": ...,
                "self_sent": ...,
                "self_received": ...
            }}
        """
        return {chan: stats.summary() for chan, stats in self._stats.items()}
        
    def close(self):
        """
        Shut down the connection and clean up resources.
        
        This will:
        - Stop the send and receive loops
        - Close the UDP socket
        - Clean up any pending data
        
        After calling close(), this GameNet instance cannot be used anymore.
        
        Example:
            try:
                gamenet = GameNet(...)
                # Use gamenet...
            finally:
                gamenet.close()  # Always clean up
        """
        self.io.close()
        
    def __enter__(self):
        """
        Context manager support: allows using 'with' statement.
        
        Example:
            with GameNet(local_addr=(...), remote_addr=(...)) as gn:
                gn.send(b"hello", reliable=True)
                payload, meta = gn.recv(timeout=1.0)
            # Automatically calls close() when exiting the 'with' block
        """
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.close()
        return False  # Don't suppress exceptions


# ==============================================================================
# DESIGN NOTES
# ==============================================================================

"""
1. WHY THIS SIMPLE FAÇADE?
   - Application code shouldn't need to know about UDPIO, threads, or SR details
   - Clean separation: api.py is what users import, everything else is internal
   - Easy to mock/test: just implement send()/recv() interface

2. AUTOMATIC START:
   - The I/O loops start automatically in __init__
   - User doesn't need to remember to call start()
   - Can immediately use send()/recv() after construction

3. BUFFER FULL HANDLING:
   - send() automatically retries if reliable buffer is full (default: 10 retries)
   - Configurable via max_retries and retry_delay_ms parameters
   - Small delay (default 10ms) between retries allows send buffer to drain
   - Returns -1 only if buffer is still full after all retry attempts
   - Application can:
     * Use default retry behavior (recommended for most cases)
     * Customize retry parameters for critical data
     * Check return value -1 for persistent buffer full condition
     * For unreliable sends, no retry needed (fire-and-forget)
   - Retry logic is blocking but brief (max ~100ms with defaults)

4. CONTEXT MANAGER SUPPORT:
   - Supports 'with' statement for automatic cleanup
   - Ensures close() is called even if exceptions occur
   - Best practice for resource management

5. METADATA IN recv():
   - Applications get both payload and metadata
   - Can distinguish reliable vs unreliable messages
   - Can measure latency using timestamps
   - Can detect gaps in sequence numbers (if needed)

6. NO BUFFERING IN API LAYER:
   - All buffering/queuing happens in io_async.py
   - This layer is truly just a thin façade
   - Keeps responsibilities clear

7. THREAD SAFETY:
   - Multiple threads can call send() concurrently (UDPIO handles it)
   - recv() is safe to call from multiple threads (queue.Queue is thread-safe)
   - In practice, usually one thread sends and one receives

8. INITIALIZATION PATTERNS:

   Server (listen on specific port):
       server = GameNet(
           local_addr=("0.0.0.0", 6000),      # Bind to all interfaces, port 6000
           remote_addr=("client_ip", 5000)     # Send to client at port 5000
       )
   
   Client (any available port):
       client = GameNet(
           local_addr=("0.0.0.0", 0),          # OS assigns available port
           remote_addr=("server_ip", 6000)     # Connect to server at port 6000
       )
       
   Client (specific port):
       client = GameNet(
           local_addr=("0.0.0.0", 5000),       # Use specific port 5000
           remote_addr=("server_ip", 6000)     # Connect to server at port 6000
       )

9. COMMON USAGE PATTERNS:

   Pattern 1: Game state updates (reliable with default retry)
       seq = gamenet.send(json.dumps({"event": "player_joined"}).encode(), reliable=True)
       if seq == -1:
           # Buffer full even after 10 retries (rare but possible)
           log_error("Failed to send critical game state")
   
   Pattern 2: Critical data with aggressive retry
       seq = gamenet.send(critical_data, reliable=True, max_retries=50, retry_delay_ms=5)
   
   Pattern 3: Position updates (unreliable, high frequency, no retry needed)
       while game_running:
           pos_data = f"pos:{x},{y},{z}".encode()
           gamenet.send(pos_data, reliable=False)
           time.sleep(0.016)  # ~60 fps
   
   Pattern 4: Message processing loop
       while True:
           result = gamenet.recv(timeout=1.0)
           if result:
               payload, meta = result
               handle_message(payload, meta)
           else:
               # Timeout - check if should continue
               if should_quit:
                   break
"""

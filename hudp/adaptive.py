"""
Adaptive Parameter Controller for H-UDP

This module implements dynamic RTO adjustment based on observed network
conditions from STATS_SYNC measurements. Uses RFC 6298 smoothed RTT algorithm.
Skip threshold remains static for stability.
"""

import time
from typing import Tuple, Optional


class AdaptiveParameterController:
    """
    Dynamically adjusts RTO based on network conditions using RFC 6298.
    
    Skip threshold remains FIXED at initial value to prevent destabilizing
    the SR window. Only RTO adapts based on observed RTT variance.
    """
    
    def __init__(self, initial_rto: int = 10, initial_skip: int = 200,
                 min_rto: int = 15, max_rto: int = 85):
        """
        Initialize adaptive controller - Pure RFC 6298.
        
        Args:
            initial_rto: Starting RTO value in milliseconds (default: 100ms)
            initial_skip: FIXED skip threshold in milliseconds (default: 200ms)
            min_rto: Minimum RTO clamp value (default: 100ms per RFC 6298)
            max_rto: Maximum RTO clamp value (default: 200ms)
        """
        self.initial_rto = initial_rto
        self.initial_skip = initial_skip  # FIXED - never changes
        self.min_rto = min_rto
        self.max_rto = max_rto
        
        # RFC 6298: Initialize to None, set on first measurement
        self.srtt: Optional[float] = None
        self.rttvar: Optional[float] = None
        
        # Current parameters
        self.current_rto = initial_rto
        self.current_skip = initial_skip  # FIXED - never changes
        
        # Pending update mechanism - apply updates between transmissions
        self.pending_rto = None  # Store calculated RTO, apply when safe
        
        # Statistics for monitoring
        self.update_count = 0
        self.last_update_time = time.time()
        
    def on_stats_sync(self, rtt_ms: float, loss_rate: float, window_usage: float = 0.0) -> Tuple[int, int]:
        """
        Update RTO based on new STATS_SYNC measurements using RFC 6298.
        Skip threshold remains FIXED at initial value.
        
        Args:
            rtt_ms: Measured round-trip time in milliseconds
            loss_rate: Observed packet loss rate (0.0 to 1.0) - for monitoring only
            window_usage: Current SR window usage (0.0 to 1.0) - for safe updates
            
        Returns:
            Tuple of (new_rto_ms, fixed_skip_threshold_ms)
        """
        # Sanity check inputs
        if rtt_ms <= 0 or rtt_ms > 10000:  # Reject invalid RTT measurements
            return self.current_rto, self.current_skip
        
        loss_rate = max(0.0, min(1.0, loss_rate))  # Clamp loss to [0, 1]
        
        # RFC 6298 Algorithm (pure, no modifications)
        if self.srtt is None:
            # (2.2) First measurement
            self.srtt = rtt_ms
            self.rttvar = rtt_ms / 2.0
        else:
            # (2.3) Subsequent measurements
            alpha = 0.125  # SRTT weight
            beta = 0.25    # RTTVAR weight
            
            rttvar_sample = abs(self.srtt - rtt_ms)
            self.rttvar = (1 - beta) * self.rttvar + beta * rttvar_sample
            self.srtt = (1 - alpha) * self.srtt + alpha * rtt_ms
        
        # (2.4) RTO calculation
        new_rto = self.srtt + 4.0 * self.rttvar
        
        # (2.5) Clamp to minimum/maximum
        new_rto = max(self.min_rto, min(self.max_rto, new_rto))
        
        # SAFE UPDATE STRATEGY: Only apply RTO updates when window is not too busy
        # This prevents disrupting in-flight packets
        if window_usage < 0.7:  # Window less than 70% full - safe to update
            self.current_rto = int(new_rto)
            self.pending_rto = None
            update_applied = True
        else:
            # Defer update until window clears
            self.pending_rto = int(new_rto)
            update_applied = False
        
        # Update statistics
        self.update_count += 1
        self.last_update_time = time.time()
        
        # Log adaptation for monitoring
        status = "APPLIED" if update_applied else f"DEFERRED (window {window_usage*100:.0f}% full)"
        print(f"[ADAPTIVE] RTT={rtt_ms:.1f}ms, Loss={loss_rate*100:.1f}%, "
              f"SRTT={self.srtt:.1f}ms, RTTVAR={self.rttvar:.1f}ms → "
              f"RTO={int(new_rto)}ms [{status}] (Skip={self.current_skip}ms fixed)")
        
        return self.current_rto, self.current_skip
    
    def try_apply_pending(self, window_usage: float) -> bool:
        """
        Try to apply a pending RTO update if window usage is low enough.
        Called periodically from io_async loop.
        
        Returns:
            True if pending update was applied, False otherwise
        """
        if self.pending_rto is not None and window_usage < 0.5:
            old_rto = self.current_rto
            self.current_rto = self.pending_rto
            self.pending_rto = None
            print(f"[ADAPTIVE] Applied deferred RTO update: {old_rto}ms → {self.current_rto}ms")
            return True
        return False
    
    def get_current_params(self) -> Tuple[int, int]:
        """
        Get current adaptive parameters without updating.
        
        Returns:
            Tuple of (current_rto_ms, current_skip_threshold_ms)
        """
        return self.current_rto, self.current_skip
    
    def get_statistics(self) -> dict:
        """
        Get controller statistics for monitoring and debugging.
        
        Returns:
            Dictionary with current state and statistics
        """
        return {
            'srtt_ms': round(self.srtt, 2),
            'rttvar_ms': round(self.rttvar, 2),
            'current_rto_ms': self.current_rto,
            'current_skip_ms': self.current_skip,  # Always fixed at initial value
            'skip_rto_ratio': round(self.current_skip / self.current_rto, 2),
            'update_count': self.update_count,
            'time_since_last_update': round(time.time() - self.last_update_time, 1)
        }
    
    def reset(self):
        """Reset controller to initial state."""
        self.srtt = float(self.initial_rto)  # Reset to initial, not None
        self.rttvar = float(self.initial_rto) / 4.0
        self.current_rto = self.initial_rto
        self.current_skip = self.initial_skip  # Always fixed at initial value
        self.pending_rto = None  # Clear any pending updates
        self.update_count = 0
        self.last_update_time = time.time()

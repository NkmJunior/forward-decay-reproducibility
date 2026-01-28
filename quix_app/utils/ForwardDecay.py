import math
from collections import defaultdict

class ForwardDecay:
    def __init__(self, lambda_=0.01, t0=None):
        """
        Forward Decay implementation.
        
        Parameters:
        -----------
        lambda_ : float
            Decay rate λ (controls how fast items lose importance)
        
        t0 : float or None
            Origin time. If None, t0 will be set at first update call.
        """
        self.lambda_ = lambda_
        self.t0 = t0
        
        # Storage: item -> sum of exp(-λ(timestamp - t0))
        self.decayed_counts = defaultdict(float)

    def _ensure_t0(self, timestamp):
        """Set the origin time if not already done."""
        if self.t0 is None:
            self.t0 = timestamp

    def update(self, item_id, timestamp):
        """
        Add a new element to the stream.
        
        Parameters:
        -----------
        item_id : int or str
            Identifier for the item (e.g., packet type)
        
        timestamp : float
            Timestamp in seconds (UNIX timestamp)
        """
        self._ensure_t0(timestamp)
        
        # Compute decayed contribution: d = exp(-λ * (t - t0))
        d = math.exp(self.lambda_ * (timestamp - self.t0))
        
        # Add to storage
        self.decayed_counts[item_id] += d

    def query(self, item_id, current_time):
        """
        Get the current decayed frequency of an item.
        
        Parameters:
        -----------
        item_id : int or str
        current_time : float
        
        Returns:
        --------
        float : Forward-decayed frequency estimate
        """
        if item_id not in self.decayed_counts:
            return 0.0

        if self.t0 is None:
            return 0.0

        multiplier = math.exp(-self.lambda_ * (current_time - self.t0))
        return self.decayed_counts[item_id] * multiplier

    def total_frequency(self, current_time):
        """
        Total decayed count over ALL items.
        """
        if self.t0 is None:
            return 0.0

        multiplier = math.exp(self.lambda_ * (current_time - self.t0))
        return sum(v * multiplier for v in self.decayed_counts.values())

    def top_k(self, k, current_time):
        """
        Return the top-k items according to decayed frequency.
        
        Returns a list of tuples: [(item_id, decayed_freq), ...]
        """
        if self.t0 is None:
            return []

        multiplier = math.exp(self.lambda_ * (current_time - self.t0))

        scored = [
            (item, value * multiplier)
            for item, value in self.decayed_counts.items()
        ]

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:k]

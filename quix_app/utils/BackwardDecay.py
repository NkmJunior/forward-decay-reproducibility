import math
from collections import defaultdict

class BackwardDecay:
    def __init__(self, lambda_=0.01):
        """
        Backward decay implementation.
        
        Parameters:
        -----------
        lambda_ : float
            Decay rate λ.
        """
        self.lambda_ = lambda_
        
        # Storage: item -> list of timestamps
        self.timestamps = defaultdict(list)

    def update(self, item_id, timestamp):
        """
        Add a new element timestamp for this item.
        
        Parameters:
        -----------
        item_id : int or str
        timestamp : float
        """
        self.timestamps[item_id].append(timestamp)

    def query(self, item_id, current_time):
        """
        Compute backward-decayed frequency for the given item at time current_time.
        
        Using:
        w = exp(-λ * (current_time - timestamp))
        
        Parameters:
        -----------
        item_id : int or str
        current_time : float
        
        Returns:
        --------
        float
        """
        if item_id not in self.timestamps:
            return 0.0
        
        total = 0.0
        for ts in self.timestamps[item_id]:
            total += math.exp(-self.lambda_ * (current_time - ts))
        
        return total

    def total_frequency(self, current_time):
        """
        Total backward-decayed count for ALL items.
        """
        total = 0.0
        
        for _, timestamps in self.timestamps.items():
            for ts in timestamps:
                total += math.exp(-self.lambda_ * (current_time - ts))
        
        return total

    def top_k(self, k, current_time):
        """
        Return the top-k items according to backward-decayed frequency.
        """
        scores = []
        
        for item, timestamps in self.timestamps.items():
            total = 0.0
            for ts in timestamps:
                total += math.exp(-self.lambda_ * (current_time - ts))
            scores.append((item, total))
        
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:k]

from collections import defaultdict
import bisect

class SlidingWindow:
    def __init__(self, window_size=10.0):
        """
        Sliding Window implementation.

        Parameters:
        -----------
        window_size : float
            Duration of the window (seconds).
        """
        self.window_size = window_size
        # item_id -> sorted list of timestamps
        self.timestamps = defaultdict(list)

    def _cleanup(self, item_id, current_time):
        """
        Remove timestamps older than (current_time - window_size).
        """
        window_start = current_time - self.window_size
        ts_list = self.timestamps[item_id]

        # Find the first timestamp >= window_start using binary search
        idx = bisect.bisect_left(ts_list, window_start)

        # Drop older timestamps
        self.timestamps[item_id] = ts_list[idx:]

    def update(self, item_id, timestamp):
        """
        Add a new element to the window.

        Parameters:
        -----------
        item_id : int or str
        timestamp : float
        """
        # Insert timestamp into sorted list
        bisect.insort(self.timestamps[item_id], timestamp)

        # Cleanup old timestamps
        self._cleanup(item_id, timestamp)

    def query(self, item_id, current_time):
        """
        Return the number of occurrences inside the window.
        """
        if item_id not in self.timestamps:
            return 0

        self._cleanup(item_id, current_time)
        return len(self.timestamps[item_id])

    def total_frequency(self, current_time):
        """
        Sum of counts over all items in the window.
        """
        total = 0
        for item_id in self.timestamps.keys():
            self._cleanup(item_id, current_time)
            total += len(self.timestamps[item_id])
        return total

    def top_k(self, k, current_time):
        """
        Return top-k items in the current window.
        """
        scored = []

        for item, ts_list in self.timestamps.items():
            self._cleanup(item, current_time)
            scored.append((item, len(self.timestamps[item])))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:k]

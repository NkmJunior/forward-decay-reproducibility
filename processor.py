"""Forward Decay Algorithm Processor

Implements the Forward Decay algorithm from:
Cormode, G., Shkapenyuk, V., Srivastava, D., & Xu, B. (2009).
"Forward decay: A practical time decay model for streaming systems."

The algorithm maintains time-decayed aggregates over streaming data,
where older items contribute less to the aggregate than newer items.
"""

import time
import json
import os
import math
from collections import defaultdict
from quixstreams import Application

# ============================================================================
# Configuration
# ============================================================================
TOPIC_NAME = "network-traffic"
OUTPUT_TOPIC = "decay-results"
REPORT_INTERVAL_SEC = 5.0

# Forward Decay parameters
LANDMARK_TIME = time.time()  # L: landmark time (start of the decay window)
DECAY_RATE = 0.001           # α: decay rate (higher = faster decay)


# ============================================================================
# Forward Decay Algorithm Implementation
# ============================================================================
class ForwardDecayAggregator:
    """
    Implements Forward Decay with exponential decay function.

    The decay function: g(t) = e^(α * (t - L))
    Where:
        - t is the current timestamp
        - L is the landmark time
        - α is the decay rate

    For each item with value v arriving at time t:
        - Forward-decayed weight: w(t) = v * g(t) = v * e^(α * (t - L))

    To query the decayed sum at time t_q:
        - Decayed sum = (1/g(t_q)) * Σ w(t_i)
    """

    def __init__(self, landmark_time: float, decay_rate: float):
        self.landmark_time = landmark_time
        self.decay_rate = decay_rate

        # Aggregation state
        self.forward_sum = 0.0      # Σ v_i * g(t_i)
        self.forward_count = 0.0    # Σ g(t_i) (for weighted count)
        self.item_count = 0         # Total items processed

    def decay_function(self, timestamp: float) -> float:
        """Compute g(t) = e^(α * (t - L))"""
        return math.exp(self.decay_rate * (timestamp - self.landmark_time))

    def update(self, value: float, timestamp: float):
        """Add a new item with given value and timestamp."""
        g_t = self.decay_function(timestamp)

        # Forward-decayed contribution
        self.forward_sum += value * g_t
        self.forward_count += g_t
        self.item_count += 1

    def query_sum(self, query_time: float) -> float:
        """Query the time-decayed sum at the given time."""
        g_q = self.decay_function(query_time)
        if g_q == 0:
            return 0.0
        return self.forward_sum / g_q

    def query_average(self, query_time: float) -> float:
        """Query the time-decayed average at the given time."""
        g_q = self.decay_function(query_time)
        if g_q == 0 or self.forward_count == 0:
            return 0.0
        decayed_sum = self.forward_sum / g_q
        decayed_count = self.forward_count / g_q
        return decayed_sum / decayed_count if decayed_count > 0 else 0.0

    def get_stats(self, query_time: float) -> dict:
        """Get current statistics."""
        return {
            "decayed_sum": self.query_sum(query_time),
            "decayed_avg": self.query_average(query_time),
            "total_items": self.item_count,
            "forward_sum": self.forward_sum,
            "decay_rate": self.decay_rate,
            "query_time": query_time
        }


class PolynomialDecayAggregator:
    """
    Implements Forward Decay with polynomial decay function.

    The decay function: g(t) = (t - L + 1)^β
    Where:
        - t is the current timestamp
        - L is the landmark time
        - β is the polynomial degree (higher = faster decay)
    """

    def __init__(self, landmark_time: float, poly_degree: float = 2.0):
        self.landmark_time = landmark_time
        self.poly_degree = poly_degree

        self.forward_sum = 0.0
        self.forward_count = 0.0
        self.item_count = 0

    def decay_function(self, timestamp: float) -> float:
        """Compute g(t) = (t - L + 1)^β"""
        return math.pow(timestamp - self.landmark_time + 1, self.poly_degree)

    def update(self, value: float, timestamp: float):
        """Add a new item with given value and timestamp."""
        g_t = self.decay_function(timestamp)
        self.forward_sum += value * g_t
        self.forward_count += g_t
        self.item_count += 1

    def query_sum(self, query_time: float) -> float:
        """Query the time-decayed sum at the given time."""
        g_q = self.decay_function(query_time)
        if g_q == 0:
            return 0.0
        return self.forward_sum / g_q

    def query_average(self, query_time: float) -> float:
        """Query the time-decayed average at the given time."""
        g_q = self.decay_function(query_time)
        if g_q == 0 or self.forward_count == 0:
            return 0.0
        decayed_sum = self.forward_sum / g_q
        decayed_count = self.forward_count / g_q
        return decayed_sum / decayed_count if decayed_count > 0 else 0.0

    def get_stats(self, query_time: float) -> dict:
        """Get current statistics."""
        return {
            "decayed_sum": self.query_sum(query_time),
            "decayed_avg": self.query_average(query_time),
            "total_items": self.item_count,
            "forward_sum": self.forward_sum,
            "poly_degree": self.poly_degree,
            "query_time": query_time
        }


class SlidingWindowDecayAggregator:
    """
    Combines Forward Decay with sliding window semantics.

    Maintains multiple "buckets" to allow approximate window queries.
    This is useful for queries like "decayed sum over the last N seconds".
    """

    def __init__(self, landmark_time: float, decay_rate: float,
                 window_size: float = 60.0, num_buckets: int = 6):
        self.landmark_time = landmark_time
        self.decay_rate = decay_rate
        self.window_size = window_size
        self.bucket_size = window_size / num_buckets
        self.num_buckets = num_buckets

        # Each bucket stores (forward_sum, forward_count, bucket_start_time)
        self.buckets = []
        self.current_bucket_sum = 0.0
        self.current_bucket_count = 0.0
        self.current_bucket_start = landmark_time
        self.item_count = 0

    def decay_function(self, timestamp: float) -> float:
        """Compute g(t) = e^(α * (t - L))"""
        return math.exp(self.decay_rate * (timestamp - self.landmark_time))

    def _rotate_buckets(self, current_time: float):
        """Rotate buckets if needed."""
        while current_time - self.current_bucket_start >= self.bucket_size:
            # Save current bucket
            self.buckets.append({
                "sum": self.current_bucket_sum,
                "count": self.current_bucket_count,
                "start": self.current_bucket_start
            })

            # Keep only recent buckets
            if len(self.buckets) > self.num_buckets:
                self.buckets.pop(0)

            # Reset current bucket
            self.current_bucket_start += self.bucket_size
            self.current_bucket_sum = 0.0
            self.current_bucket_count = 0.0

    def update(self, value: float, timestamp: float):
        """Add a new item with given value and timestamp."""
        self._rotate_buckets(timestamp)

        g_t = self.decay_function(timestamp)
        self.current_bucket_sum += value * g_t
        self.current_bucket_count += g_t
        self.item_count += 1

    def query_sum(self, query_time: float) -> float:
        """Query the time-decayed sum over the sliding window."""
        self._rotate_buckets(query_time)
        g_q = self.decay_function(query_time)

        if g_q == 0:
            return 0.0

        total_sum = self.current_bucket_sum
        for bucket in self.buckets:
            total_sum += bucket["sum"]

        return total_sum / g_q

    def get_stats(self, query_time: float) -> dict:
        """Get current statistics."""
        return {
            "windowed_decayed_sum": self.query_sum(query_time),
            "total_items": self.item_count,
            "num_active_buckets": len(self.buckets) + 1,
            "window_size": self.window_size,
            "query_time": query_time
        }


# ============================================================================
# Main Processor
# ============================================================================
def main():
    broker_address = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:19092")
    decay_type = os.getenv("DECAY_TYPE", "exponential")  # exponential, polynomial, sliding

    app = Application(
        broker_address=broker_address,
        consumer_group="decay-processor-group",
        auto_offset_reset="latest",
        loglevel="WARNING"
    )

    input_topic = app.topic(TOPIC_NAME, value_deserializer="json")

    # Initialize aggregators based on configuration
    landmark = time.time()

    # Per-key aggregators (e.g., per destination IP)
    aggregators = defaultdict(lambda: create_aggregator(decay_type, landmark))

    # Global aggregator for overall statistics
    global_aggregator = create_aggregator(decay_type, landmark)

    print(f"[*] Processor initialized. Broker: {broker_address}")
    print(f"[*] Decay type: {decay_type}")
    print(f"[*] Listening on topic: {TOPIC_NAME}")

    message_count = 0
    last_report_time = time.time()

    with app.get_consumer() as consumer:
        consumer.subscribe([input_topic.name])

        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"[!] Consumer error: {msg.error()}")
                continue

            # Parse the message
            try:
                packet = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, AttributeError) as e:
                print(f"[!] Failed to parse message: {e}")
                continue

            timestamp = packet.get("timestamp", time.time())
            value = packet.get("packet_size", 0)
            key = packet.get("dest_ip", "unknown")

            # Update aggregators
            aggregators[key].update(value, timestamp)
            global_aggregator.update(value, timestamp)

            message_count += 1

            # Periodic reporting
            current_time = time.time()
            if current_time - last_report_time >= REPORT_INTERVAL_SEC:
                stats = global_aggregator.get_stats(current_time)

                print(f"\n{'='*60}")
                print(f"[*] Forward Decay Statistics (t={current_time:.2f})")
                print(f"    Messages processed: {message_count}")
                print(f"    Decay type: {decay_type}")
                print(f"    Decayed sum: {stats['decayed_sum']:.2f}")
                if 'decayed_avg' in stats:
                    print(f"    Decayed average: {stats['decayed_avg']:.2f}")
                print(f"{'='*60}")

                # Per-key stats (top 5)
                if aggregators:
                    print("\n[*] Per-destination statistics:")
                    for k, agg in sorted(
                        aggregators.items(),
                        key=lambda x: x[1].query_sum(current_time),
                        reverse=True
                    )[:5]:
                        k_stats = agg.get_stats(current_time)
                        print(f"    {k}: sum={k_stats['decayed_sum']:.2f}, "
                              f"items={k_stats['total_items']}")

                last_report_time = current_time


def create_aggregator(decay_type: str, landmark: float):
    """Factory function to create the appropriate aggregator."""
    if decay_type == "polynomial":
        poly_degree = float(os.getenv("POLY_DEGREE", "2.0"))
        return PolynomialDecayAggregator(landmark, poly_degree)
    elif decay_type == "sliding":
        decay_rate = float(os.getenv("DECAY_RATE", "0.001"))
        window_size = float(os.getenv("WINDOW_SIZE", "60.0"))
        return SlidingWindowDecayAggregator(landmark, decay_rate, window_size)
    else:  # exponential (default)
        decay_rate = float(os.getenv("DECAY_RATE", "0.001"))
        return ForwardDecayAggregator(landmark, decay_rate)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[!] Processor stopping...")

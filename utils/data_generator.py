"""Shared data generation utilities for traffic simulation."""
import numpy as np
import time


def generate_zipf_item(num_items, alpha):
    """Generate a single item ID following Zipf distribution.

    Args:
        num_items: Maximum number of items (1 to num_items)
        alpha: Zipf distribution parameter (higher = more skewed)

    Returns:
        int: Item ID between 1 and num_items
    """
    sample = np.random.zipf(alpha, 1)[0]
    if sample > num_items:
        sample = np.random.randint(1, num_items + 1)
    return int(sample)


def generate_zipf_items(num_items, alpha, size):
    """Generate multiple item IDs following Zipf distribution.

    Args:
        num_items: Maximum number of items (1 to num_items)
        alpha: Zipf distribution parameter (higher = more skewed)
        size: Number of items to generate

    Returns:
        np.ndarray: Array of item IDs
    """
    zipf_samples = np.random.zipf(alpha, size)
    # Clip values to valid range
    zipf_samples = np.minimum(zipf_samples, num_items)
    return zipf_samples.astype(int)


def generate_packet(num_items, alpha, timestamp=None):
    """Generate a single network packet.

    Args:
        num_items: Maximum number of items
        alpha: Zipf distribution parameter
        timestamp: Optional timestamp (defaults to current time)

    Returns:
        dict: Packet with timestamp, item_id, packet_size
    """
    if timestamp is None:
        timestamp = time.time()

    return {
        "timestamp": timestamp,
        "item_id": generate_zipf_item(num_items, alpha),
        "packet_size": np.random.randint(40, 1500)
    }

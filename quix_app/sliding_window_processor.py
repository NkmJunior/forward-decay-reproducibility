"""
Sliding Window Processor using the base processor framework.
"""
from .base_processor import BaseProcessor
from .utils.SlidingWindow import SlidingWindow


class SlidingWindowProcessor(BaseProcessor):
    """
    Processor for Sliding Window algorithm.
    """

    def __init__(self):
        super().__init__(
            algorithm_class=SlidingWindow,
            algo_params={"window_size": 10.0},
            consumer_group="sliding-window-group"
        )

    def log_progress(self, item, freq):
        print(f"[SW] Processed {self.message_count} packets. "
              f"Current item: {item}, Freq: {freq:.4f}")


def run_sliding_window_processor():
    """
    Entry point for running the sliding window processor.
    """
    processor = SlidingWindowProcessor()
    processor.run()


if __name__ == "__main__":
    run_sliding_window_processor()
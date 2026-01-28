"""
Forward Decay Processor using the base processor framework.
"""
from .base_processor import BaseProcessor
from .utils.ForwardDecay import ForwardDecay


class ForwardDecayProcessor(BaseProcessor):
    """
    Processor for Forward Decay algorithm.
    """

    def __init__(self):
        super().__init__(
            algorithm_class=ForwardDecay,
            algo_params={"lambda_": 0.01},
            consumer_group="forward-decay-group"
        )

    def log_progress(self, item, freq):
        print(f"[FD] Processed {self.message_count} packets. "
              f"Current item: {item}, Freq: {freq:.4f}")


def run_forward_processor():
    """
    Entry point for running the forward decay processor.
    """
    processor = ForwardDecayProcessor()
    processor.run()


if __name__ == "__main__":
    run_forward_processor()

if __name__ == "__main__":
    run_forward_processor()
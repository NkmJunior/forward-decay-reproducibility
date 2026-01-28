"""
Backward Decay Processor using the base processor framework.
"""
from .base_processor import BaseProcessor
from .utils.BackwardDecay import BackwardDecay


class BackwardDecayProcessor(BaseProcessor):
    """
    Processor for Backward Decay algorithm.
    """

    def __init__(self):
        super().__init__(
            algorithm_class=BackwardDecay,
            algo_params={"lambda_": 0.01},
            consumer_group="backward-decay-group"
        )

    def log_progress(self, item, freq):
        print(f"[BD] Processed {self.message_count} packets. "
              f"Current item: {item}, Freq: {freq:.4f}")


def run_backward_processor():
    """
    Entry point for running the backward decay processor.
    """
    processor = BackwardDecayProcessor()
    processor.run()


if __name__ == "__main__":
    run_backward_processor()
"""
Base processor class for unified Kafka stream processing.
"""
import os
from quixstreams import Application


class BaseProcessor:
    """
    Base class for all streaming processors.
    Handles common Kafka setup and message processing logic.
    """

    def __init__(self, algorithm_class, algo_params, consumer_group, topic_name="traffic"):
        """
        Initialize the processor.

        Args:
            algorithm_class: The algorithm class (e.g., ForwardDecay)
            algo_params: Dict of parameters for the algorithm
            consumer_group: Kafka consumer group name
            topic_name: Kafka topic name
        """
        self.algorithm = algorithm_class(**algo_params)
        self.consumer_group = consumer_group
        self.topic_name = topic_name
        self.message_count = 0

    def process_message(self, row):
        """
        Process a single message. Override log_progress for custom logging.
        """
        ts = row["timestamp"]
        item = row["item_id"]

        # Update algorithm
        self.algorithm.update(item, ts)

        # Log progress
        self.message_count += 1
        if self.message_count % 5000 == 0:
            freq = self.algorithm.query(item, ts)
            self.log_progress(item, freq)

    def log_progress(self, item, freq):
        """
        Log processing progress. Override in subclasses for custom messages.
        """
        print(f"[{self.__class__.__name__}] Processed {self.message_count} packets. "
              f"Current item: {item}, Freq: {freq:.4f}")

    def run(self):
        """
        Run the streaming processor.
        """
        broker_address = os.getenv("KAFKA_BROKER_ADDRESS", "127.0.0.1:9092")

        app = Application(
            broker_address=broker_address,
            consumer_group=self.consumer_group,
            auto_offset_reset="earliest"
        )

        topic = app.topic(self.topic_name, value_deserializer="json")
        sdf = app.dataframe(topic)
        sdf = sdf.update(self.process_message)

        print(f"[{self.__class__.__name__}] Running...")
        app.run(sdf)
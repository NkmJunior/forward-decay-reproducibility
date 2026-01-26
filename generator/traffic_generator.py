import csv
import time
import sys
import os
import numpy as np
from datetime import datetime, timedelta

# Add parent directory to path for imports
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
sys.path.insert(0, ROOT_DIR)

from utils.data_generator import generate_zipf_items

OUTPUT_PATH = os.path.join(BASE_DIR, "..", "data", "generated_stream.csv")


def traffic_generator(
    output_file=OUTPUT_PATH,
    num_items=1000,
    heavy_hitters=10,
    rate=1000,
    duration=10,
    zipf_alpha=1.2,
):
    """
    Generate a synthetic traffic stream and save as CSV.
    """

    total_packets = rate * duration
    start_time = datetime.now()

    print(f"Generating {total_packets} packets...")
    print(f"Saving to: {output_file}")

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "item_id", "packet_size"])

        # Generate item IDs with Zipf distribution
        items = generate_zipf_items(num_items, zipf_alpha, total_packets)

        for i in range(total_packets):
            timestamp = start_time + timedelta(seconds=i / rate)
            item_id = int(items[i])
            packet_size = int(np.random.randint(40, 1500))  # bytes

            writer.writerow([timestamp.timestamp(), item_id, packet_size])

    print("Generation complete.")


if __name__ == "__main__":
    traffic_generator(
        output_file=OUTPUT_PATH,
        num_items=1000,
        heavy_hitters=10,
        rate=10000,        # 10k packets/s (modif possible)
        duration=30,       # Increased to 30 seconds for meaningful sliding window evaluation
        zipf_alpha=1.2,
    )

# # directory destination: cd "/mnt/c/Users/Pica Sebi/Documents/GitHub/forward-decay-reproducibility"

# import time
# import json
# import random
# import logging
# from collections import deque
# from threading import Thread, Lock
# import queue

# # Configuration constants
# TOPIC_NAME = "network-traffic"
# REPORT_INTERVAL_SEC = 1.0

# class LocalMessageQueue:
#     """In-memory message queue that simulates Kafka behavior"""
#     def __init__(self):
#         self.topics = {}
#         self.locks = {}
        
#     def get_topic_queue(self, topic_name):
#         if topic_name not in self.topics:
#             self.topics[topic_name] = deque(maxlen=100000)  # Keep last 100k messages
#             self.locks[topic_name] = Lock()
#         return self.topics[topic_name], self.locks[topic_name]
    
#     def produce(self, topic, key, value):
#         topic_queue, lock = self.get_topic_queue(topic)
#         with lock:
#             topic_queue.append({
#                 'key': key,
#                 'value': value,
#                 'timestamp': time.time()
#             })
    
#     def consume(self, topic, count=1):
#         topic_queue, lock = self.get_topic_queue(topic)
#         messages = []
#         with lock:
#             for _ in range(min(count, len(topic_queue))):
#                 if topic_queue:
#                     messages.append(topic_queue.popleft())
#         return messages
    
#     def get_queue_size(self, topic):
#         if topic in self.topics:
#             return len(self.topics[topic])
#         return 0


# class LocalProducer:
#     """Simulates Kafka producer with local queue"""
#     def __init__(self, message_queue, config=None):
#         self.queue = message_queue
#         self.config = config or {}
#         self.batch_size = self.config.get('batch.size', 16384)
#         self.linger_ms = self.config.get('linger.ms', 5)
#         self.compression = self.config.get('compression.type', 'lz4')
        
#     def produce(self, topic, key, value):
#         """Add message to queue"""
#         self.queue.produce(topic, key, value)
        
#     def flush(self):
#         """No-op for local producer"""
#         pass
    
#     def __enter__(self):
#         return self
    
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.flush()


# class LocalApplication:
#     """Simulates QuixStreams Application for local development"""
#     def __init__(self, broker_address=None, producer_extra_config=None, loglevel="WARNING"):
#         self.broker_address = broker_address or "local"
#         self.producer_config = producer_extra_config or {}
#         self.message_queue = LocalMessageQueue()
#         logging.basicConfig(level=getattr(logging, loglevel))
        
#     def topic(self, name):
#         """Return a topic object"""
#         return LocalTopic(name)
    
#     def get_producer(self):
#         """Return a producer instance"""
#         return LocalProducer(self.message_queue, self.producer_config)
    
#     def get_consumer(self, topic_name, group_id="default"):
#         """Return a consumer for the topic"""
#         return LocalConsumer(self.message_queue, topic_name, group_id)


# class LocalTopic:
#     """Represents a Kafka topic"""
#     def __init__(self, name):
#         self.name = name


# class LocalConsumer:
#     """Simulates Kafka consumer"""
#     def __init__(self, message_queue, topic, group_id):
#         self.queue = message_queue
#         self.topic = topic
#         self.group_id = group_id
    
#     def consume(self, count=1):
#         return self.queue.consume(self.topic, count)


# # ============================================================================
# # TRAFFIC GENERATOR (Updated to use local queue)
# # ============================================================================

# PRODUCER_CONFIG = {
#     "linger.ms": 5,
#     "batch.size": 16384,
#     "compression.type": "lz4",
#     "enable.idempotence": False
# }

# def main():
#     # Use local application instead of connecting to Kafka
#     app = LocalApplication(
#         broker_address="local",
#         producer_extra_config=PRODUCER_CONFIG,
#         loglevel="WARNING" 
#     )
    
#     topic = app.topic(TOPIC_NAME)
    
#     print(f"[*] Local Producer initialized. Topic: {TOPIC_NAME}")
#     print(f"[*] Messages stored in-memory (max 100k per topic)")

#     packet_count = 0
#     last_report_time = time.time()

#     with app.get_producer() as producer:
#         try:
#             while True:
#                 # Synthetic packet generation
#                 packet = {
#                     "timestamp": time.time(),
#                     "item_id": random.randint(1000, 9999),
#                     "dest_ip": f"192.168.1.{random.randint(1, 254)}", 
#                     "packet_size": random.randint(64, 1500)
#                 }
                
#                 # Produce to local queue
#                 producer.produce(
#                     topic=topic.name, 
#                     key=packet["dest_ip"], 
#                     value=json.dumps(packet)
#                 )
                
#                 packet_count += 1
                
#                 # Non-blocking telemetry reporting
#                 if packet_count % 5000 == 0:
#                     current_time = time.time()
#                     elapsed = current_time - last_report_time
                    
#                     if elapsed >= REPORT_INTERVAL_SEC:
#                         rate = packet_count / elapsed
#                         queue_size = app.message_queue.get_queue_size(topic.name)
#                         print(f"[*] Throughput: {rate:.0f} msgs/sec | Total produced: {packet_count} | Queue size: {queue_size}")
                        
#                         packet_count = 0
#                         last_report_time = current_time
                        
#         except KeyboardInterrupt:
#             print("\n[!] Producer stopping...")
#             print(f"[*] Final queue size: {app.message_queue.get_queue_size(topic.name)} messages")


# if __name__ == "__main__":
#     main()
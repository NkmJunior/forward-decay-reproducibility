import time
import sys
import os
import csv
from quixstreams import Application

# Add parent directory to path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
sys.path.insert(0, ROOT_DIR)

from utils.data_generator import generate_packet

RATE = 5000
NUM_ITEMS = 1000
ZIPF_ALPHA = 1.2

def run_realtime_producer(save_to_csv=None, duration_seconds=None):
    """Run the realtime producer.

    Args:
        save_to_csv: Optional path to CSV file. If provided, packets will be
                     saved to this file in addition to being sent to Kafka.
        duration_seconds: Optional duration in seconds. If provided, producer
                         will stop after this time.
    """
    # Get broker address from environment or default to localhost
    broker_address = os.getenv("KAFKA_BROKER_ADDRESS", "127.0.0.1:9092")

    # 1. Connect to the broker
    app = Application(broker_address=broker_address)

    # 2. Define the topic with explicit JSON serialization
    topic = app.topic("traffic", value_serializer="json")

    # 3. Create the producer
    producer = app.get_producer()

    # 4. Setup CSV writer if requested
    csv_file = None
    csv_writer = None
    if save_to_csv:
        csv_file = open(save_to_csv, "w", newline="")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["timestamp", "item_id", "packet_size"])
        print(f"[Producer] Saving packets to {save_to_csv}")

    print(f"[Producer] Streaming at {RATE} packets/s")
    if duration_seconds:
        print(f"[Producer] Will run for {duration_seconds} seconds")
    delay = 1.0 / RATE
    start_time = time.time()

    try:
        while True:
            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                print(f"\n[Producer] Reached duration limit ({duration_seconds}s), stopping...")
                break
            # Generate packet using shared utility
            packet = generate_packet(NUM_ITEMS, ZIPF_ALPHA)
            ts = packet["timestamp"]
            item_id = packet["item_id"]
            packet_size = packet["packet_size"]

            message = {
                "timestamp": ts,
                "item_id": item_id,
                "packet_size": packet_size
            }

            # 5. Serialize and produce to Kafka
            kafka_msg = topic.serialize(key=str(item_id), value=message)
            producer.produce(
                topic.name,
                value=kafka_msg.value,
                key=kafka_msg.key
            )

            # 6. Save to CSV if requested
            if csv_writer:
                csv_writer.writerow([ts, item_id, packet_size])

            # Poll to handle delivery callbacks
            producer.poll(0)

            time.sleep(delay)

    finally:
        if csv_file:
            csv_file.close()
            print(f"\n[Producer] CSV file closed: {save_to_csv}")

if __name__ == "__main__":
    # Check for CSV output argument
    csv_output = os.getenv("SAVE_TO_CSV")
    duration = os.getenv("PRODUCER_DURATION")
    duration_seconds = int(duration) if duration else None
    run_realtime_producer(save_to_csv=csv_output, duration_seconds=duration_seconds)
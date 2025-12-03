import time
import numpy as np
from quixstreams import Application

RATE = 5000
NUM_ITEMS = 1000
ZIPF_ALPHA = 1.2

def generate_zipf_item(num_items, alpha):
    sample = np.random.zipf(alpha, 1)[0]
    if sample > num_items:
        sample = np.random.randint(1, num_items)
    return int(sample)

def run_realtime_producer():
    # 1. Connect to the broker
    app = Application(broker_address="127.0.0.1:9092")
    
    # 2. Define the topic with explicit JSON serialization
    topic = app.topic("traffic", value_serializer="json")

    # 3. Create the producer
    producer = app.get_producer()

    print(f"[Producer] Streaming at {RATE} packets/s")
    delay = 1.0 / RATE

    while True:
        ts = time.time()
        item_id = generate_zipf_item(NUM_ITEMS, ZIPF_ALPHA)
        packet_size = np.random.randint(40, 1500)

        message = {
            "timestamp": ts,
            "item_id": item_id,
            "packet_size": packet_size
        }

        # 4. Serialize the dictionary to bytes using the topic's configuration
        # This returns an object with .value (bytes) and .key (bytes)
        kafka_msg = topic.serialize(key=str(item_id), value=message)

        # 5. Produce the BYTES, not the dictionary
        producer.produce(
            topic.name, 
            value=kafka_msg.value, 
            key=kafka_msg.key
        )
        
        # Optional: Poll to handle delivery callbacks and keep the internal queue healthy
        producer.poll(0)
        
        time.sleep(delay)

if __name__ == "__main__":
    run_realtime_producer()
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
    app = Application(broker_address="localhost:9092")
    topic = app.topic("traffic")

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

        producer.produce(topic.name, value=message)
        time.sleep(delay)

if __name__ == "__main__":
    run_realtime_producer()

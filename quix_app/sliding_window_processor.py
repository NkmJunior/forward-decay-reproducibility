from quixstreams import Application
from utils.SlidingWindow import SlidingWindow

# Initialize Algorithm
sw = SlidingWindow(window_size=10)
message_count = 0

def process_message(row):
    global message_count
    
    ts = row["timestamp"]
    item = row["item_id"]

    sw.update(item, ts)
    
    message_count += 1
    if message_count % 5000 == 0:
        freq = sw.query(item, ts)
        print(f"[SW] Processed {message_count} packets. Current item: {item}, Freq: {freq:.4f}")

def run_sliding_window_processor():
    # Fix: Use 127.0.0.1 and unique consumer group
    app = Application(
        broker_address="127.0.0.1:9092",
        consumer_group="sliding-window-group",
        auto_offset_reset="earliest"
    )

    topic = app.topic("traffic", value_deserializer="json")

    sdf = app.dataframe(topic)
    sdf = sdf.update(process_message)

    print("[Sliding Window] Running... (Printing every 5000 packets)")
    app.run(sdf)

if __name__ == "__main__":
    run_sliding_window_processor()
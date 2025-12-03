from quixstreams import Application
from utils.BackwardDecay import BackwardDecay

# Initialize Algorithm
bd = BackwardDecay(lambda_=0.01)
message_count = 0

def process_message(row):
    global message_count
    
    # 1. Extract data
    ts = row["timestamp"]
    item = row["item_id"]

    # 2. Update Algorithm
    bd.update(item, ts)
    
    # 3. Log progress every 5000 items
    message_count += 1
    if message_count % 5000 == 0:
        freq = bd.query(item, ts)
        print(f"[BD] Processed {message_count} packets. Current item: {item}, Freq: {freq:.4f}")

def run_backward_processor():
    # Fix: Use 127.0.0.1 and unique consumer group
    app = Application(
        broker_address="127.0.0.1:9092",
        consumer_group="backward-decay-group",
        auto_offset_reset="earliest"
    )

    # Fix: Use JSON deserializer
    topic = app.topic("traffic", value_deserializer="json")

    # Fix: Use DataFrame API instead of subscribe()
    sdf = app.dataframe(topic)
    sdf = sdf.update(process_message)

    print("[Backward Processor] Running... (Printing every 5000 packets)")
    app.run(sdf)

if __name__ == "__main__":
    run_backward_processor()
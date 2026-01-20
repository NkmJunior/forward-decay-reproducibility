from quixstreams import Application
import os
# Ensure this import matches your file structure.
# If running "python quix_app/forward_decay_processor.py", this is correct:
from utils.ForwardDecay import ForwardDecay 

# Initialize the algorithm
fd = ForwardDecay(lambda_=0.01)

def process_message(row):
    """
    Callback function that receives the message VALUE (dictionary) directly.
    """
    # In the DataFrame API, 'row' is the deserialized dictionary 
    # (e.g., {"timestamp": ..., "item_id": ...})
    ts = row["timestamp"]
    item = row["item_id"]

    # Update and Query the Forward Decay algorithm
    fd.update(item, ts)
    freq = fd.query(item, ts)

    print(f"[FD] item={item}, freq={freq:.4f}")

def run_forward_processor():
    # Get broker address from environment or default to localhost
    broker_address = os.getenv("KAFKA_BROKER_ADDRESS", "127.0.0.1:9092")

    # 1. Use broker_address from environment variable
    # 2. auto_offset_reset="earliest" ensures you read data sent before the processor started
    app = Application(
        broker_address=broker_address,
        consumer_group="forward-decay-group",
        auto_offset_reset="earliest"
    )

    # 3. Define topic with JSON deserializer so 'row' is a dict, not bytes
    topic = app.topic("traffic", value_deserializer="json")

    # 4. Create a StreamingDataFrame
    sdf = app.dataframe(topic)

    # 5. Connect your processing function
    # .update() runs the function for every message but keeps the stream going
    sdf = sdf.update(process_message)

    print("[Forward Processor] Running...")
    
    # 6. Start the application loop (blocks here and processes messages)
    app.run(sdf)

if __name__ == "__main__":
    run_forward_processor()
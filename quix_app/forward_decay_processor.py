from quixstreams import Application
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
    # 1. Use 127.0.0.1 to match your producer and avoid Windows localhost issues
    # 2. auto_offset_reset="earliest" ensures you read data sent before the processor started
    app = Application(
        broker_address="127.0.0.1:9092", 
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
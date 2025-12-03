from quixstreams import Application
from utils.ForwardDecay import ForwardDecay

fd = ForwardDecay(lambda_=0.01)

def process_message(msg):
    ts = msg.value["timestamp"]
    item = msg.value["item_id"]

    fd.update(item, ts)
    freq = fd.query(item, ts)

    print(f"[FD] item={item}, freq={freq}")

def run_forward_processor():
    app = Application(broker_address="localhost:9092")
    topic = app.topic("traffic")

    consumer = app.get_consumer()
    consumer.subscribe([topic.name], callback=process_message)

    print("[Forward Processor] Running...")
    app.run()

if __name__ == "__main__":
    run_forward_processor()

from quixstreams import Application
from quix_app.utils.BackwardDecay import BackwardDecay

bd = BackwardDecay(lambda_=0.01)

def process_message(msg):
    ts = msg["timestamp"]
    item = msg["item_id"]

    bd.update(item, ts)
    freq = bd.query(item, ts)
    
    print(f"[BD] item={item} freq={freq}")

def run_backward_processor():
    app = Application(broker_address="localhost:9092")
    topic = app.topic("traffic")

    consumer = app.get_consumer()
    consumer.subscribe([topic.name], callback=process_message)

    print("Backward Decay processor running...")
    app.run()

if __name__ == "__main__":
    run_backward_processor()

from quixstreams import Application
from quix_app.utils.SlidingWindow import SlidingWindow

sw = SlidingWindow(window_size=10)

def process_message(msg):
    ts = msg["timestamp"]
    item = msg["item_id"]

    sw.update(item, ts)
    freq = sw.query(item, ts)

    print(f"[SW] item={item} freq={freq}")

def run_sliding_window_processor():
    app = Application(broker_address="localhost:9092")
    topic = app.topic("traffic")

    consumer = app.get_consumer()
    consumer.subscribe([topic.name], callback=process_message)

    print("Sliding Window processor running...")
    app.run()

if __name__ == "__main__":
    run_sliding_window_processor()
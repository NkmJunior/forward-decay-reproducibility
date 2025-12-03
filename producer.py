# directory destination: cd "/mnt/c/Users/Pica Sebi/Documents/GitHub/forward-decay-reproducibility"



import time
import json
import random
import os
import logging
from quixstreams import Application

# Configuration constants
TOPIC_NAME = "network-traffic"
REPORT_INTERVAL_SEC = 1.0

PRODUCER_CONFIG = {
    "linger.ms": 5,           # Buffer for 5ms to enable batching
    "batch.size": 16384,      # 16KB max batch size
    "compression.type": "lz4",# Low-overhead compression
    "acks": "1"               # Leader ack only (faster than 'all')
}

def main():
    broker_address = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:19092")
    
    app = Application(
        broker_address=broker_address,
        producer_extra_config=PRODUCER_CONFIG,
        loglevel="WARNING" 
    )
    
    topic = app.topic(TOPIC_NAME)
    
    print(f"[*] Producer initialized. Target: {broker_address} | Topic: {TOPIC_NAME}")

    packet_count = 0
    last_report_time = time.time()

    with app.get_producer() as producer:
        while True:
            # Synthetic packet generation
            packet = {
                "timestamp": time.time(),
                "item_id": random.randint(1000, 9999),
                "dest_ip": "192.168.1.10", 
                "packet_size": random.randint(64, 1500)
            }
            
            # Async dispatch - fire and forget
            producer.produce(
                topic=topic.name, 
                key=packet["dest_ip"], 
                value=json.dumps(packet)
            )
            
            packet_count += 1
            
            # Non-blocking telemetry reporting
            if packet_count % 5000 == 0:
                current_time = time.time()
                elapsed = current_time - last_report_time
                
                if elapsed >= REPORT_INTERVAL_SEC:
                    rate = packet_count / elapsed
                    print(f"[*] Throughput: {rate:.0f} msgs/sec | Total: {packet_count} buffered")
                    
                    packet_count = 0
                    last_report_time = current_time

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[!] Producer stopping...")
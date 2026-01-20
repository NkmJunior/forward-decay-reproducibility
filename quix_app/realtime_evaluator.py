"""Real-time evaluation consumer that evaluates algorithm performance from Kafka."""
import os
import sys
import time
import json
import signal
import atexit
from collections import defaultdict
from quixstreams import Application

# Add parent directory to path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
sys.path.insert(0, ROOT_DIR)

from quix_app.utils.ForwardDecay import ForwardDecay
from quix_app.utils.BackwardDecay import BackwardDecay
from quix_app.utils.SlidingWindow import SlidingWindow

# Configuration
LAMBDA = 0.01
WINDOW_SIZE = 10.0
EVAL_EVERY = 5000  # Evaluate every N packets
TRACK_ITEMS = [1, 2, 3, 4, 5]
TOP_K = 5

# Global state
packet_count = 0
ground_truth = defaultdict(int)
results = {
    "timestamps": [],
    "fd_avg_error": [],
    "bd_avg_error": [],
    "sw_avg_error": [],
    "topk_accuracy_fd": [],
    "topk_accuracy_bd": [],
    "topk_accuracy_sw": [],
    "memory_fd": [],
    "memory_bd": [],
    "memory_sw": [],
}

# Initialize algorithms
fd = ForwardDecay(lambda_=LAMBDA)
bd = BackwardDecay(lambda_=LAMBDA)
sw = SlidingWindow(window_size=WINDOW_SIZE)


def relative_error(est, truth):
    """Calculate relative error between estimate and truth."""
    if truth == 0:
        return 0 if est == 0 else 1
    return abs(est - truth) / truth


def process_packet(row):
    """Process a single packet and update algorithms."""
    global packet_count

    ts = row["timestamp"]
    item = row["item_id"]

    # Update ground truth
    ground_truth[item] += 1

    # Update all algorithms
    fd.update(item, ts)
    bd.update(item, ts)
    sw.update(item, ts)

    packet_count += 1

    # Evaluate periodically
    if packet_count % EVAL_EVERY == 0:
        evaluate_performance(ts)


def evaluate_performance(ts):
    """Evaluate algorithm performance at current timestamp."""
    results["timestamps"].append(ts)

    # Calculate errors for tracked items
    fd_errs = []
    bd_errs = []
    sw_errs = []

    for item in TRACK_ITEMS:
        truth = ground_truth[item]

        fd_est = fd.query(item, ts)
        bd_est = bd.query(item, ts)
        sw_est = sw.query(item, ts)

        fd_errs.append(relative_error(fd_est, truth))
        bd_errs.append(relative_error(bd_est, truth))
        sw_errs.append(relative_error(sw_est, truth))

    # Average errors
    fd_avg_err = sum(fd_errs) / len(fd_errs)
    bd_avg_err = sum(bd_errs) / len(bd_errs)
    sw_avg_err = sum(sw_errs) / len(sw_errs)

    results["fd_avg_error"].append(fd_avg_err)
    results["bd_avg_error"].append(bd_avg_err)
    results["sw_avg_error"].append(sw_avg_err)

    # Top-K accuracy
    true_topk = sorted(
        ground_truth.items(), key=lambda x: x[1], reverse=True
    )[:TOP_K]
    true_items = set([x[0] for x in true_topk])

    fd_top = set([x[0] for x in fd.top_k(TOP_K, ts)])
    bd_top = set([x[0] for x in bd.top_k(TOP_K, ts)])
    sw_top = set([x[0] for x in sw.top_k(TOP_K, ts)])

    results["topk_accuracy_fd"].append(len(fd_top & true_items) / TOP_K)
    results["topk_accuracy_bd"].append(len(bd_top & true_items) / TOP_K)
    results["topk_accuracy_sw"].append(len(sw_top & true_items) / TOP_K)

    # Memory usage
    results["memory_fd"].append(len(fd.decayed_counts))
    results["memory_bd"].append(sum(len(v) for v in bd.timestamps.values()))
    results["memory_sw"].append(sum(len(v) for v in sw.timestamps.values()))

    # Print progress
    print(
        f"[{packet_count}] FD: {fd_avg_err:.4f}, "
        f"BD: {bd_avg_err:.4f}, "
        f"SW: {sw_avg_err:.4f} | "
        f"TopK: FD={results['topk_accuracy_fd'][-1]:.2f}, "
        f"BD={results['topk_accuracy_bd'][-1]:.2f}, "
        f"SW={results['topk_accuracy_sw'][-1]:.2f}"
    )


def save_results(output_file):
    """Save evaluation results to file."""
    print("\n[Realtime Evaluator] Stopping...")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(results, f, indent=4)
    print(f"[Realtime Evaluator] Results saved to {output_file}")
    print(f"[Realtime Evaluator] Total packets processed: {packet_count}")


def run_realtime_evaluator():
    """Run the real-time evaluator."""
    broker_address = os.getenv("KAFKA_BROKER_ADDRESS", "127.0.0.1:9092")
    output_file = os.getenv("EVAL_OUTPUT", "evaluation/realtime_results.json")

    # Register cleanup function to always save results on exit
    atexit.register(save_results, output_file)

    # Setup signal handler for graceful shutdown
    def signal_handler(signum, frame):
        print("\n[Realtime Evaluator] Received signal, shutting down...")
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    print("[Realtime Evaluator] Waiting for Kafka to be ready...")
    time.sleep(10)  # Wait for Kafka to start

    app = Application(
        broker_address=broker_address,
        consumer_group="realtime-evaluator-group",
        auto_offset_reset="earliest"  # Read from beginning
    )

    topic = app.topic("traffic", value_deserializer="json")
    sdf = app.dataframe(topic)
    sdf = sdf.update(process_packet)

    print("[Realtime Evaluator] Starting...")
    print(f"[Realtime Evaluator] Evaluating every {EVAL_EVERY} packets")
    print(f"[Realtime Evaluator] Results will be saved to {output_file}")

    app.run(sdf)


if __name__ == "__main__":
    run_realtime_evaluator()

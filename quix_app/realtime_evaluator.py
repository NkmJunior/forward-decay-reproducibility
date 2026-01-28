"""Real-time evaluation consumer that evaluates algorithm performance from Kafka."""
import os
import sys
import time
import json
import signal
import atexit
import math
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
WINDOW_SIZE = 30.0
EVAL_EVERY = 5000  # Evaluate every N packets
TRACK_ITEMS = [1, 2, 3, 4, 5]
TOP_K = 5

# Global state
L = None  # Landmark
decayed_ground_truth = defaultdict(float)

# Define packet_count globally
packet_count = 0

# Define raw_ground_truth globally
raw_ground_truth = defaultdict(int)

# Store timing data for each packet
timing_data = {
    "fd_times": [],
    "bd_times": [],
    "sw_times": []
}
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
    "fd_time": [],
    "bd_time": [],
    "sw_time": [],
    "eps": [] 
}

# Initialize algorithms
fd = ForwardDecay(lambda_=LAMBDA)
bd = BackwardDecay(lambda_=LAMBDA)
sw = SlidingWindow(window_size=WINDOW_SIZE)
last_eval_wall_time = time.time()


def relative_error(est, truth):
    """Calculate relative error between estimate and truth."""
    if truth == 0:
        return 0 if est == 0 else 1
    return abs(est - truth) / truth


def process_packet(row):
    """Process a single packet and update algorithms."""
    global L, packet_count

    ts = row["timestamp"]
    item = row["item_id"]
    
    raw_ground_truth[item] += 1


    # Initialize landmark
    if L is None:
        L = ts

    # Update decayed ground truth with numerator
    weight_numerator = math.exp(LAMBDA * (ts - L))
    decayed_ground_truth[item] += weight_numerator

    # Update all algorithms with timing measurements
    t0 = time.perf_counter()
    fd.update(item, ts)
    fd_time = time.perf_counter() - t0
    timing_data["fd_times"].append(fd_time)

    t0 = time.perf_counter()
    bd.update(item, ts)
    bd_time = time.perf_counter() - t0
    timing_data["bd_times"].append(bd_time)

    t0 = time.perf_counter()
    sw.update(item, ts)
    sw_time = time.perf_counter() - t0
    timing_data["sw_times"].append(sw_time)

    packet_count += 1

    # Evaluate periodically
    if packet_count % EVAL_EVERY == 0:
        evaluate_performance(ts)


# def evaluate_performance(ts):
#     """Evaluate the performance of algorithms."""
#     results["timestamps"].append(ts)

#     # Calculate denominator for current time
#     current_denominator = math.exp(LAMBDA * (ts - L))

#     fd_errs = []
#     bd_errs = []
#     sw_errs = []

#     for item in TRACK_ITEMS:
#         # Calculate exact truth value
#         exact_truth = decayed_ground_truth[item] / current_denominator

#         fd_est = fd.query(item, ts)
#         bd_est = bd.query(item, ts)
#         sw_est = sw.query(item, ts)

#         fd_errs.append(relative_error(fd_est, exact_truth))
#         bd_errs.append(relative_error(bd_est, exact_truth))
#         sw_errs.append(relative_error(sw_est, exact_truth))

#     # Average errors
#     fd_avg_err = sum(fd_errs) / len(fd_errs)
#     bd_avg_err = sum(bd_errs) / len(bd_errs)
#     sw_avg_err = sum(sw_errs) / len(sw_errs)

#     results["fd_avg_error"].append(fd_avg_err)
#     results["bd_avg_error"].append(bd_avg_err)
#     results["sw_avg_error"].append(sw_avg_err)

#     # Top-K accuracy
#     true_topk = sorted(
#         raw_ground_truth.items(), key=lambda x: x[1], reverse=True
#     )[:TOP_K]
#     true_items = set([x[0] for x in true_topk])

#     fd_top = set([x[0] for x in fd.top_k(TOP_K, ts)])
#     bd_top = set([x[0] for x in bd.top_k(TOP_K, ts)])
#     sw_top = set([x[0] for x in sw.top_k(TOP_K, ts)])

#     results["topk_accuracy_fd"].append(len(fd_top & true_items) / TOP_K)
#     results["topk_accuracy_bd"].append(len(bd_top & true_items) / TOP_K)
#     results["topk_accuracy_sw"].append(len(sw_top & true_items) / TOP_K)

#     # Memory usage
#     results["memory_fd"].append(len(fd.decayed_counts))
#     results["memory_bd"].append(sum(len(v) for v in bd.timestamps.values()))
#     results["memory_sw"].append(sum(len(v) for v in sw.timestamps.values()))

#     # Average timing for the last EVAL_EVERY packets
#     import numpy as np
#     results["fd_time"].append(np.mean(timing_data["fd_times"][-EVAL_EVERY:]))
#     results["bd_time"].append(np.mean(timing_data["bd_times"][-EVAL_EVERY:]))
#     results["sw_time"].append(np.mean(timing_data["sw_times"][-EVAL_EVERY:]))

#     # Print progress
#     print(
#         f"[{packet_count}] FD: {fd_avg_err:.4f}, "
#         f"BD: {bd_avg_err:.4f}, "
#         f"SW: {sw_avg_err:.4f} | "
#         f"TopK: FD={results['topk_accuracy_fd'][-1]:.2f}, "
#         f"BD={results['topk_accuracy_bd'][-1]:.2f}, "
#         f"SW={results['topk_accuracy_sw'][-1]:.2f}"
#     )
def evaluate_performance(ts):
    """评估性能并计算 EPS。"""
    global last_eval_wall_time
    now = time.time()
    results["timestamps"].append(ts)

    # 计算吞吐量 (EPS)
    duration = now - last_eval_wall_time
    eps = EVAL_EVERY / duration if duration > 0 else 0
    results["eps"].append(eps)
    last_eval_wall_time = now

    # 1. 计算误差 (逻辑保持之前的精确真值对比)
    current_denominator = math.exp(LAMBDA * (ts - L))
    fd_errs, bd_errs, sw_errs = [], [], []

    for item in TRACK_ITEMS:
        exact_truth = decayed_ground_truth[item] / current_denominator
        fd_errs.append(relative_error(fd.query(item, ts), exact_truth))
        bd_errs.append(relative_error(bd.query(item, ts), exact_truth))
        sw_errs.append(relative_error(sw.query(item, ts), exact_truth))

    results["fd_avg_error"].append(sum(fd_errs) / len(fd_errs))
    results["bd_avg_error"].append(sum(bd_errs) / len(bd_errs))
    results["sw_avg_error"].append(sum(sw_errs) / len(sw_errs))

    # 2. Top-K 准确率 (基于 raw_ground_truth)
    true_topk = sorted(raw_ground_truth.items(), key=lambda x: x[1], reverse=True)[:TOP_K]
    true_items = set([x[0] for x in true_topk])
    results["topk_accuracy_fd"].append(len(set([x[0] for x in fd.top_k(TOP_K, ts)]) & true_items) / TOP_K)
    results["topk_accuracy_bd"].append(len(set([x[0] for x in bd.top_k(TOP_K, ts)]) & true_items) / TOP_K)
    results["topk_accuracy_sw"].append(len(set([x[0] for x in sw.top_k(TOP_K, ts)]) & true_items) / TOP_K)

    # 3. 内存与时间 (逻辑不变)
    results["memory_fd"].append(len(fd.decayed_counts))
    results["memory_bd"].append(sum(len(v) for v in bd.timestamps.values()))
    results["memory_sw"].append(sum(len(v) for v in sw.timestamps.values()))
    
    import numpy as np
    results["fd_time"].append(np.mean(timing_data["fd_times"][-EVAL_EVERY:]))
    results["bd_time"].append(np.mean(timing_data["bd_times"][-EVAL_EVERY:]))
    results["sw_time"].append(np.mean(timing_data["sw_times"][-EVAL_EVERY:]))

    print(f"[{packet_count}] EPS: {eps:.2f} | FD Err: {results['fd_avg_error'][-1]:.4f} | Memory FD: {results['memory_fd'][-1]}")


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

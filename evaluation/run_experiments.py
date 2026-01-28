import csv
import time
from collections import defaultdict
import math
import json
import sys, os

# ------------------------------------
# PATH SETUP
# ------------------------------------
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(BASE_DIR, "..", "data", "generated_stream.csv")

CSV_PATH = OUTPUT_PATH

# ------------------------------------
# IMPORT ALGORITHMS & CONFIG
# ------------------------------------
from config import ALGORITHM_CONFIG, EXPERIMENT_CONFIG
from quix_app.utils.ForwardDecay import ForwardDecay
from quix_app.utils.BackwardDecay import BackwardDecay
from quix_app.utils.SlidingWindow import SlidingWindow

# Use config values
LAMBDA = ALGORITHM_CONFIG["lambda"]
WINDOW_SIZE = ALGORITHM_CONFIG["window_size"]
ADAPTIVE_T0 = ALGORITHM_CONFIG["adaptive_t0"]
ERROR_THRESHOLD = ALGORITHM_CONFIG["error_threshold"]

EVAL_EVERY = EXPERIMENT_CONFIG["eval_every"]
TRACK_ITEMS = EXPERIMENT_CONFIG["track_items"]
TOP_K = EXPERIMENT_CONFIG["top_k"]
CSV_PATH = os.path.join(ROOT_DIR, EXPERIMENT_CONFIG["data_file"])


# =========================================================
# UTILS
# =========================================================
def relative_error(est, truth):
    if truth == 0:
        return 0 if est == 0 else 1
    return abs(est - truth) / truth


# =========================================================
# MAIN EXPERIMENT
# =========================================================
def run_experiments():
    print("Loading stream...")
    stream = []

    with open(CSV_PATH, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = float(row["timestamp"])
            item = int(row["item_id"])
            stream.append((ts, item))

    print(f"Loaded {len(stream)} events.")

    # Initialize algos
    fd = ForwardDecay(lambda_=LAMBDA)
    bd = BackwardDecay(lambda_=LAMBDA)
    sw = SlidingWindow(window_size=WINDOW_SIZE)

    # Ground truth
    ground_truth = defaultdict(int)

    # Metrics
    results = {
        "timestamps": [],
        "fd_error": [],
        "bd_error": [],
        "sw_error": [],
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
        "sw_time": []
    }

    print("Running experiments...")

    for i, (ts, item) in enumerate(stream):

        # Update ground truth
        ground_truth[item] += 1

        # -----------------------------------
        # Measure UPDATE TIME for each algo
        # -----------------------------------

        # Forward
        t0 = time.perf_counter()
        fd.update(item, ts)
        fd_time = time.perf_counter() - t0

        # Backward
        t0 = time.perf_counter()
        bd.update(item, ts)
        bd_time = time.perf_counter() - t0

        # Sliding Window
        t0 = time.perf_counter()
        sw.update(item, ts)
        sw_time = time.perf_counter() - t0

        # ----------------------------------------------------
        # EVALUATION every EVAL_EVERY packets
        # ----------------------------------------------------
        if (i + 1) % EVAL_EVERY == 0:
            results["timestamps"].append(ts)

            # ---- Relative Error on TRACK_ITEMS ---
            fd_errs = []
            bd_errs = []
            sw_errs = []

            for it in TRACK_ITEMS:
                truth = ground_truth[it]

                fd_est = fd.query(it, ts)
                bd_est = bd.query(it, ts)
                sw_est = sw.query(it, ts)

                fd_errs.append(relative_error(fd_est, truth))
                bd_errs.append(relative_error(bd_est, truth))
                sw_errs.append(relative_error(sw_est, truth))

            # Average errors
            results["fd_avg_error"].append(sum(fd_errs) / len(fd_errs))
            results["bd_avg_error"].append(sum(bd_errs) / len(bd_errs))
            results["sw_avg_error"].append(sum(sw_errs) / len(sw_errs))

            # Error of first tracked item
            results["fd_error"].append(fd_errs[0])
            results["bd_error"].append(bd_errs[0])
            results["sw_error"].append(sw_errs[0])

            # --------------- TOP-K ACCURACY ----------------
            true_topk = sorted(
                ground_truth.items(), key=lambda x: x[1], reverse=True
            )[:TOP_K]
            true_items = set([x[0] for x in true_topk])

            fd_top = set([x[0] for x in fd.top_k(TOP_K, ts)])
            bd_top = set([x[0] for x in bd.top_k(TOP_K, ts)])
            sw_top = set([x[0] for x in sw.top_k(TOP_K, ts)])

            results["topk_accuracy_fd"].append(len(fd_top & true_items)/TOP_K)
            results["topk_accuracy_bd"].append(len(bd_top & true_items)/TOP_K)
            results["topk_accuracy_sw"].append(len(sw_top & true_items)/TOP_K)

            # --------------- MEMORY USAGE -------------------
            results["memory_fd"].append(len(fd.decayed_counts))
            results["memory_bd"].append(sum(len(v) for v in bd.timestamps.values()))
            results["memory_sw"].append(sum(len(v) for v in sw.timestamps.values()))

            # --------------- STORE UPDATE TIMES -------------
            results["fd_time"].append(fd_time)
            results["bd_time"].append(bd_time)
            results["sw_time"].append(sw_time)

            print(
                f"[{i}] FD avg err={results['fd_avg_error'][-1]:.4f}, "
                f"BD avg err={results['bd_avg_error'][-1]:.4f}, "
                f"SW avg err={results['sw_avg_error'][-1]:.4f}"
            )

    # Save results
    out_file = "evaluation/results.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=4)

    print(f"\nResults saved in {out_file}")
    return results


# =========================================================
# ENTRY POINT
# =========================================================
if __name__ == "__main__":
    run_experiments()

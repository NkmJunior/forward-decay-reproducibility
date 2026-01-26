import json
import os
import matplotlib.pyplot as plt
import numpy as np

# ------------------------------------
# PATH SETUP
# ------------------------------------
import sys
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

# Default paths
DEFAULT_RESULTS_PATH = "evaluation/results.json"
DEFAULT_OUTPUT_DIR = "evaluation/plots_system_costs/"


def ensure_output_dir(output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)


def load_results(results_path):
    with open(results_path, "r") as f:
        return json.load(f)


# --------------------------------------------------------
# SYSTEM COST PLOTS (STYLE PAPER)
# --------------------------------------------------------

def plot_cpu_load(results, output_dir):
    """
    Approximate CPU load by update time.
    We take the mean update time over all evaluation steps.
    """
    fd_time = np.mean(results["fd_time"])
    bd_time = np.mean(results["bd_time"])
    sw_time = np.mean(results["sw_time"])

    algos = ["Forward", "Backward", "Sliding"]
    values = [fd_time, bd_time, sw_time]

    plt.figure(figsize=(8, 6))
    plt.bar(algos, values, color=["black", "gray", "lightgray"])
    plt.ylabel("Avg Update Time (seconds)")
    plt.title("Optimized cost of SUM and COUNT queries")
    plt.grid(axis="y")
    plt.tight_layout()
    plt.savefig(output_dir + "cpu_load_bar.png")
    plt.close()


def plot_query_cost(results, output_dir):
    """
    Query cost: we approximate query time by evaluating the function.
    We'll measure time explicitly for each algo on tracked items.
    """
    import time
    import itertools

    fd_time_list, bd_time_list, sw_time_list = [], [], []

    ts_sample = results["timestamps"][-1]  # last timestamp for querying
    track_items = [1, 2, 3, 4, 5]

    # load algos to simulate queries
    from quix_app.utils.ForwardDecay import ForwardDecay
    from quix_app.utils.BackwardDecay import BackwardDecay
    from quix_app.utils.SlidingWindow import SlidingWindow

    fd = ForwardDecay()
    bd = BackwardDecay()
    sw = SlidingWindow()

    # reconstruct minimal state (we regenerate using results presence)
    # NOTE: This is optional — we will just measure function overhead
    for i in track_items:
        t0 = time.perf_counter()
        fd.query(i, ts_sample)
        fd_time_list.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        bd.query(i, ts_sample)
        bd_time_list.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        sw.query(i, ts_sample)
        sw_time_list.append(time.perf_counter() - t0)

    fd_cost = np.mean(fd_time_list)
    bd_cost = np.mean(bd_time_list)
    sw_cost = np.mean(sw_time_list)

    algos = ["Forward", "Backward", "Sliding"]
    values = [fd_cost, bd_cost, sw_cost]

    plt.figure(figsize=(8, 6))
    plt.bar(algos, values, color=["black", "gray", "lightgray"])
    plt.ylabel("Query Time (seconds)")
    plt.title("Unoptimized cost of SUM and COUNT queries")
    plt.grid(axis="y")
    plt.tight_layout()
    plt.savefig(output_dir + "query_cost_bar.png")
    plt.close()


def plot_memory_usage(results, output_dir):
    """
    Memory usage from results.json (already saved as counts).
    We take the last sample for each algo.
    """
    fd_mem = results["memory_fd"][-1]
    bd_mem = results["memory_bd"][-1]
    sw_mem = results["memory_sw"][-1]

    algos = ["Forward", "Backward", "Sliding"]
    values = [fd_mem, bd_mem, sw_mem]

    plt.figure(figsize=(8, 6))
    plt.bar(algos, values, color=["black", "gray", "lightgray"])
    plt.yscale("log")  # like paper (log scale)
    plt.ylabel("Space (entries ~ bytes)")
    plt.title("Space comparison for SUM/COUNT models")
    plt.grid(axis="y")
    plt.tight_layout()
    plt.savefig(output_dir + "memory_space_bar.png")
    plt.close()


# --------------------------------------------------------
# MAIN EXEC
# --------------------------------------------------------

def main(results_path=None, output_dir=None):
    """Generate system cost plots from evaluation results.

    Args:
        results_path: Path to results JSON file (default: evaluation/results.json)
        output_dir: Output directory for plots (default: evaluation/plots_system_costs/)
    """
    if results_path is None:
        results_path = DEFAULT_RESULTS_PATH
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR

    # Ensure output dir ends with /
    if not output_dir.endswith('/'):
        output_dir += '/'

    print(f"Loading results from {results_path}...")
    results = load_results(results_path)
    ensure_output_dir(output_dir)

    # Check if timing data is available
    has_timing_data = "fd_time" in results and "bd_time" in results and "sw_time" in results

    if has_timing_data:
        print("Plotting CPU load (update cost)...")
        plot_cpu_load(results, output_dir)

        print("Plotting SUM/COUNT query cost...")
        plot_query_cost(results, output_dir)
    else:
        print("⚠ Timing data not available (skipping CPU load and query cost plots)")

    print("Plotting memory usage...")
    plot_memory_usage(results, output_dir)

    print(f"\n✓ System-cost plots saved in {output_dir}")
    if not has_timing_data:
        print("  Note: Only memory plot generated (timing data not available in this dataset)")


if __name__ == "__main__":
    # Support command-line arguments
    if len(sys.argv) > 1:
        results_path = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else None
        main(results_path, output_dir)
    else:
        main()

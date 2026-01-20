import json
import os
import sys
import matplotlib.pyplot as plt


# Default paths
DEFAULT_RESULTS_PATH = "evaluation/results.json"
DEFAULT_OUTPUT_DIR = "evaluation/plots_advanced/"


def ensure_output_dir(output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)


def load_results(results_path):
    with open(results_path, "r") as f:
        return json.load(f)


# ---------------------------------------------------------
# PLOTTING FUNCTIONS
# ---------------------------------------------------------

def plot_relative_error(results, output_dir):
    ts = results["timestamps"]
    # Handle both offline (with fd_error) and realtime (without fd_error) results
    fd = results.get("fd_error", results.get("fd_avg_error", []))
    bd = results.get("bd_error", results.get("bd_avg_error", []))
    sw = results.get("sw_error", results.get("sw_avg_error", []))

    plt.figure(figsize=(10, 6))
    plt.plot(ts, fd, label="Forward Decay", linewidth=2)
    plt.plot(ts, bd, label="Backward Decay", linewidth=2)
    plt.plot(ts, sw, label="Sliding Window", linewidth=2)
    plt.xlabel("Timestamp")
    plt.ylabel("Relative Error")
    plt.title("Relative Error (Single Item)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_dir + "relative_error.png")
    plt.close()


def plot_avg_relative_error(results, output_dir):
    ts = results["timestamps"]
    fd = results["fd_avg_error"]
    bd = results["bd_avg_error"]
    sw = results["sw_avg_error"]

    plt.figure(figsize=(10, 6))
    plt.plot(ts, fd, label="Forward Decay", linewidth=2)
    plt.plot(ts, bd, label="Backward Decay", linewidth=2)
    plt.plot(ts, sw, label="Sliding Window", linewidth=2)
    plt.xlabel("Timestamp")
    plt.ylabel("Average Relative Error")
    plt.title("Average Relative Error over Items")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_dir + "average_relative_error.png")
    plt.close()


def plot_topk_accuracy(results, output_dir):
    ts = results["timestamps"]
    fd = results["topk_accuracy_fd"]
    bd = results["topk_accuracy_bd"]
    sw = results["topk_accuracy_sw"]

    plt.figure(figsize=(10, 6))
    plt.plot(ts, fd, label="Forward Decay", linewidth=2)
    plt.plot(ts, bd, label="Backward Decay", linewidth=2)
    plt.plot(ts, sw, label="Sliding Window", linewidth=2)
    plt.xlabel("Timestamp")
    plt.ylabel("Top-K Accuracy")
    plt.title("Top-K Accuracy Comparison")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_dir + "topk_accuracy.png")
    plt.close()


def plot_memory_usage(results, output_dir):
    ts = results["timestamps"]
    fd = results["memory_fd"]
    bd = results["memory_bd"]
    sw = results["memory_sw"]

    plt.figure(figsize=(10, 6))
    plt.plot(ts, fd, label="Forward Decay", linewidth=2)
    plt.plot(ts, bd, label="Backward Decay", linewidth=2)
    plt.plot(ts, sw, label="Sliding Window", linewidth=2)
    plt.xlabel("Timestamp")
    plt.ylabel("Memory (approx count of stored values)")
    plt.title("Approximate Memory Usage")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_dir + "memory_usage.png")
    plt.close()


def plot_combined_error(results, output_dir):
    """All 3 average errors in one graph."""
    ts = results["timestamps"]

    plt.figure(figsize=(12, 6))
    plt.plot(ts, results["fd_avg_error"], label="FD avg error", linewidth=2)
    plt.plot(ts, results["bd_avg_error"], label="BD avg error", linewidth=2)
    plt.plot(ts, results["sw_avg_error"], label="SW avg error", linewidth=2)
    plt.xlabel("Timestamp")
    plt.ylabel("Average Error")
    plt.title("Combined Error Comparison")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_dir + "combined_errors.png")
    plt.close()


# OPTIONAL EXTRA PLOTS
def plot_error_boxplot(results, output_dir):
    """Distribution of errors across the experiment."""
    # Use fd_error if available (offline), otherwise use fd_avg_error (realtime)
    fd_data = results.get("fd_error", results.get("fd_avg_error", []))
    bd_data = results.get("bd_error", results.get("bd_avg_error", []))
    sw_data = results.get("sw_error", results.get("sw_avg_error", []))

    data = [fd_data, bd_data, sw_data]

    plt.figure(figsize=(8, 6))
    plt.boxplot(data, labels=["FD", "BD", "SW"])
    plt.title("Distribution of Relative Error")
    plt.ylabel("Relative Error")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_dir + "error_boxplot.png")
    plt.close()


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main(results_path=None, output_dir=None):
    """Generate plots from evaluation results.

    Args:
        results_path: Path to results JSON file (default: evaluation/results.json)
        output_dir: Output directory for plots (default: evaluation/plots_advanced/)
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

    print("Plotting relative error...")
    plot_relative_error(results, output_dir)

    print("Plotting average error...")
    plot_avg_relative_error(results, output_dir)

    print("Plotting top-k accuracy...")
    plot_topk_accuracy(results, output_dir)

    print("Plotting memory usage...")
    plot_memory_usage(results, output_dir)

    print("Plotting combined errors...")
    plot_combined_error(results, output_dir)

    print("Plotting boxplot...")
    plot_error_boxplot(results, output_dir)

    print(f"\n✓ Plots saved in {output_dir}")


if __name__ == "__main__":
    # Support command-line arguments
    if len(sys.argv) > 1:
        results_path = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else None
        main(results_path, output_dir)
    else:
        main()

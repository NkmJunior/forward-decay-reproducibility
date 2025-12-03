import json
import os
import matplotlib.pyplot as plt
import numpy as np


RESULTS_PATH = "evaluation/results.json"
OUTPUT_DIR = "evaluation/plots_advanced/"


def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


def load_results():
    with open(RESULTS_PATH, "r") as f:
        return json.load(f)


# ---------------------------------------------------------
# PLOTTING FUNCTIONS
# ---------------------------------------------------------

def plot_relative_error(results):
    ts = results["timestamps"]
    fd = results["fd_error"]
    bd = results["bd_error"]
    sw = results["sw_error"]

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
    plt.savefig(OUTPUT_DIR + "relative_error.png")
    plt.close()


def plot_avg_relative_error(results):
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
    plt.savefig(OUTPUT_DIR + "average_relative_error.png")
    plt.close()


def plot_topk_accuracy(results):
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
    plt.savefig(OUTPUT_DIR + "topk_accuracy.png")
    plt.close()


def plot_memory_usage(results):
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
    plt.savefig(OUTPUT_DIR + "memory_usage.png")
    plt.close()


def plot_combined_error(results):
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
    plt.savefig(OUTPUT_DIR + "combined_errors.png")
    plt.close()


# OPTIONAL EXTRA PLOTS
def plot_error_boxplot(results):
    """Distribution of errors across the experiment."""
    data = [
        results["fd_error"],
        results["bd_error"],
        results["sw_error"],
    ]

    plt.figure(figsize=(8, 6))
    plt.boxplot(data, labels=["FD", "BD", "SW"])
    plt.title("Distribution of Relative Error")
    plt.ylabel("Relative Error")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR + "error_boxplot.png")
    plt.close()


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    print("Loading results...")
    results = load_results()
    ensure_output_dir()

    print("Plotting relative error...")
    plot_relative_error(results)

    print("Plotting average error...")
    plot_avg_relative_error(results)

    print("Plotting top-k accuracy...")
    plot_topk_accuracy(results)

    print("Plotting memory usage...")
    plot_memory_usage(results)

    print("Plotting combined errors...")
    plot_combined_error(results)

    print("Plotting boxplot...")
    plot_error_boxplot(results)

    print(f"\nAdvanced plots saved in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()

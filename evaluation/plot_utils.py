"""
Shared utilities for plotting and data handling.
"""
import json
import os
import matplotlib.pyplot as plt


def load_results(results_path):
    """
    Load results from JSON file.

    Args:
        results_path (str): Path to the results JSON file

    Returns:
        dict: Loaded results data
    """
    with open(results_path, "r") as f:
        return json.load(f)


def ensure_output_dir(output_dir):
    """
    Ensure the output directory exists.

    Args:
        output_dir (str): Directory path to create if it doesn't exist
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)


def save_plot(plt, filename, output_dir):
    """
    Save a matplotlib plot to file.

    Args:
        plt: Matplotlib pyplot instance
        filename (str): Output filename
        output_dir (str): Output directory
    """
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, filename))
    plt.close()


def get_experiment_config():
    """
    Get default experiment configuration.

    Returns:
        dict: Default configuration parameters
    """
    return {
        "lambda": 0.01,
        "window_size": 30.0,
        "eval_every": 5000,
        "track_items": [1, 2, 3, 4, 5],
        "top_k": 5
    }
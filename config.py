"""
Global configuration for the forward decay reproducibility project.
"""

# Kafka configuration
KAFKA_CONFIG = {
    "broker_address": "127.0.0.1:9092",
    "topic": "traffic",
    "consumer_groups": {
        "forward": "forward-decay-group",
        "backward": "backward-decay-group",
        "sliding": "sliding-window-group",
        "producer": "traffic-producer-group"
    }
}

# Algorithm parameters
ALGORITHM_CONFIG = {
    "lambda": 0.01,
    "window_size": 30.0,
    "t0_strategy": "first",  # "first", "middle", "fixed"
    "adaptive_t0": False,
    "error_threshold": 0.1
}

# Experiment parameters
EXPERIMENT_CONFIG = {
    "eval_every": 5000,
    "track_items": [1, 2, 3, 4, 5],
    "top_k": 5,
    "data_file": "data/generated_stream.csv",
    "results_file": "evaluation/results.json"
}

# Plotting configuration
PLOT_CONFIG = {
    "output_dirs": {
        "results": "evaluation/plots/",
        "costs": "evaluation/plots_system_costs/",
        "realtime": "evaluation/plots_realtime/",
        "system_costs_realtime": "evaluation/plots_system_costs_realtime/"
    }
}
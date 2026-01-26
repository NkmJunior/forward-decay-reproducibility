# Forward Decay Reproducibility

This repository contains code and tools for reproducing experiments related to forward and backward decay algorithms in streaming data processing using Apache Kafka.

## Overview

The project implements and evaluates algorithms for processing streaming data, comparing three approaches:
- **Forward Decay** - Forward-looking decay algorithm
- **Backward Decay** - Backward-looking decay algorithm
- **Sliding Window** - Traditional sliding window baseline

The system uses Kafka for message streaming and provides both real-time and offline evaluation capabilities.

## Prerequisites

- Python 3.x
- Docker and Docker Compose (for Kafka setup)
- pip (Python package manager)

## Project Structure

```
forward-decay-reproducibility/
├── quix_app/           # Core streaming processors and utilities
│   ├── producer.py                    # Kafka data producer
│   ├── forward_decay_processor.py     # Forward decay algorithm
│   ├── backward_decay_processor.py    # Backward decay algorithm
│   ├── sliding_window_processor.py    # Sliding window implementation
│   └── realtime_evaluator.py          # Real-time evaluation
├── evaluation/         # Scripts for plotting and evaluating results
│   ├── plot_results.py                # Generate result plots
│   └── plot_system_costs.py           # Analyze system costs
├── generator/          # Traffic/data generation scripts
│   └── traffic_generator.py           # Synthetic traffic generator
├── utils/              # Additional utilities
│   └── data_generator.py              # Data generation helpers
├── data/               # Generated data files
├── docker-compose.yml  # Docker services configuration
└── requirements.txt    # Python dependencies
```

## Installation

### Option 1: Docker Setup (Recommended)

1. **Start Kafka and services**
   ```bash
   docker-compose up -d
   ```

   This will start:
   - Zookeeper (port 2181)
   - Kafka broker (port 9092)
   - Producer service
   - Forward decay processor
   - Real-time evaluator

2. **View logs**
   ```bash
   docker-compose logs -f processor
   docker-compose logs -f evaluator
   ```

3. **Stop services**
   ```bash
   docker-compose down
   ```

### Option 2: Local Setup

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start Kafka locally** (or use existing Kafka instance)
   ```bash
   docker-compose up -d zookeeper kafka
   ```

3. **Run components individually**
   ```bash
   # Terminal 1: Start producer
   python -m quix_app.producer

   # Terminal 2: Start processor
   python -m quix_app.forward_decay_processor

   # Terminal 3: Start evaluator
   python -m quix_app.realtime_evaluator
   ```

## Usage

### Running Experiments

#### Offline Evaluation

1. **Generate synthetic traffic data**
   ```bash
   python -m generator.traffic_generator
   ```
   This generates 300,000 packets over 30 seconds in `data/generated_stream.csv`.

2. **Run offline experiments**
   ```bash
   python -m evaluation.run_experiments
   ```
   This processes the CSV data and saves results to `evaluation/results.json`.

3. **Generate plots**
   ```bash
   # Performance metrics (6 plots: error, accuracy, memory, etc.)
   python -m evaluation.plot_results

   # System costs (3 plots: CPU load, query cost, memory)
   python -m evaluation.plot_system_costs
   ```
   Plots are saved in `evaluation/plots_advanced/` and `evaluation/plots_system_costs/`.

#### Real-time Evaluation

1. **Start Kafka and services**
   ```bash
   docker-compose up -d
   ```

2. **Run specific processor**
   ```bash
   # Forward decay
   python -m quix_app.forward_decay_processor

   # Backward decay
   python -m quix_app.backward_decay_processor

   # Sliding window
   python -m quix_app.sliding_window_processor
   ```

3. **Generate plots from real-time data**
   ```bash
   # Performance metrics
   python -m evaluation.plot_results evaluation/realtime_results.json evaluation/plots_realtime/

   # System costs
   python -m evaluation.plot_system_costs evaluation/realtime_results.json evaluation/plots_system_costs_realtime/
   ```

## Results

### Output Files

- **Offline results**: `evaluation/results.json`
- **Real-time results**: `evaluation/realtime_results.json`
- **Generated data**: `data/generated_stream.csv`

### Generated Plots

| Plot Type | Offline | Real-time |
|-----------|---------|-----------|
| **Performance Metrics** (6 plots) | `evaluation/plots_advanced/` | `evaluation/plots_realtime/` |
| **System Costs** (3 plots) | `evaluation/plots_system_costs/` | `evaluation/plots_system_costs_realtime/` |

**Performance Metrics Plots:**
- `relative_error.png` - Single item relative error
- `average_relative_error.png` - Average relative error over items
- `topk_accuracy.png` - Top-K accuracy comparison
- `memory_usage.png` - Approximate memory usage
- `combined_errors.png` - Combined error comparison
- `error_boxplot.png` - Error distribution

**System Cost Plots:**
- `cpu_load_bar.png` - Average update time (CPU load)
- `query_cost_bar.png` - Query time comparison
- `memory_space_bar.png` - Space comparison (log scale)

## Dependencies

- `quixstreams>=2.0.0` - Kafka streaming library
- `pandas>=1.3.0` - Data manipulation
- `numpy>=1.21.0` - Numerical computing
- `matplotlib>=3.3.0` - Plotting and visualization

## Architecture

The system follows a streaming architecture:

1. **Producer** generates or reads data and publishes to Kafka topics
2. **Processors** consume data, apply decay algorithms, and publish results
3. **Evaluator** monitors results and computes performance metrics
4. **Evaluation scripts** generate plots and analyze offline results

## License

This project is for academic and research purposes.


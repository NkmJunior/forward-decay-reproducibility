# Forward Decay Replication

This project replicates the "Forward Decay" algorithm from Cormode et al. (2009) using Python and Quix Streams.

## Paper Reference

Cormode, G., Shkapenyuk, V., Srivastava, D., & Xu, B. (2009). "Forward decay: A practical time decay model for streaming systems."

## Architecture

```
┌─────────────┐    Kafka Topic     ┌─────────────┐
│   Producer  │ ──────────────────▶│  Processor  │
│ (packets)   │  "network-traffic" │ (decay alg) │
└─────────────┘                    └─────────────┘
       │                                  │
       └──────────┬───────────────────────┘
                  ▼
           ┌───────────┐
           │  Redpanda │
           │  (Kafka)  │
           └───────────┘
```

- **Redpanda:** Kafka-compatible message broker
- **Producer:** Generates synthetic network traffic packets
- **Processor:** Calculates time-decayed aggregates using Forward Decay

## Implemented Algorithms

| Algorithm | Decay Function | Description |
|-----------|---------------|-------------|
| `exponential` | g(t) = e^(α(t-L)) | Exponential decay, fast forgetting |
| `polynomial` | g(t) = (t-L+1)^β | Polynomial decay, adjustable speed |
| `sliding` | Exponential + Buckets | Sliding window with decay |

## How to Run

### Prerequisites
- Docker & Docker Compose installed

### Quick Start

```bash
# Start all services (first time or after Dockerfile changes)
docker-compose up --build

# Start services (after code changes only)
docker-compose up -d

# View logs
docker-compose logs -f producer processor

# Stop all services
docker-compose down
```

### Configuration

Edit `docker-compose.yml` to change algorithm parameters:

```yaml
processor:
  environment:
    # Algorithm type: exponential, polynomial, sliding
    - DECAY_TYPE=exponential
    # For exponential/sliding decay (higher = faster decay)
    - DECAY_RATE=0.001
    # For polynomial decay (degree)
    - POLY_DEGREE=2.0
    # For sliding window (seconds)
    - WINDOW_SIZE=60.0
```


## Development

### Local Development (without Docker)

```bash
# Install dependencies
pip install -r requirements.txt

# Set broker address
export KAFKA_BROKER_ADDRESS=localhost:19092

# Run producer
python producer.py

# Run processor (in another terminal)
python processor.py
```

### Switching Algorithms

```bash
# Use polynomial decay
docker-compose down
DECAY_TYPE=polynomial docker-compose up -d

# Or edit docker-compose.yml and restart
docker-compose restart processor
```

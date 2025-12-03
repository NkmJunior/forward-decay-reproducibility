# Forward Decay Replication

This project replicates the "Forward Decay" algorithm from Cormode et al. (2009) using Python and Quix Streams.

## Architecture
- **Redpanda:** Kafka-compatible message broker.
- **Producer:** Generates synthetic network traffic packets.
- **Processor:** Calculates the decayed sum using Forward Decay (Polynomial).

## How to Run (Using Docker)

1. **Build and Start:**
   ```bash
   docker-compose up --build
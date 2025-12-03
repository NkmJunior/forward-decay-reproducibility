# Use python 3.10 slim for a small image size
FROM python:3.10-slim

WORKDIR /app

# Install system utilities (optional, but good for debugging)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Default command (overridden by docker-compose)
CMD ["python3"]
Project for collecting and analyzing **GitHub Public Events** data, with both **streaming (Kafka)** and **batch/offline** analysis artifacts.

## What’s in this repo

- **Kafka producer** (`kafka_producer.py`)  
  Fetches/produces GitHub public events into a Kafka topic (streaming ingestion).

- **Kafka consumer** (`kafka_consumer.py`)  
  Consumes events from Kafka and writes/aggregates them for downstream use.

- **Repository metadata helper** (`repo_data.py`)  
  Collects/works with repository-level metadata used in analysis.

- **Data / outputs**
  - `github public events analysis.ipynb` — notebook analysis
  - `github public events analysis.html` — exported notebook report

## Getting started

### Prerequisites
- Python 3.10+ recommended
- A running Kafka broker (local or remote)

### 1) Create a virtual environment & install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Run streaming pipeline

Start the producer:
```bash
python kafka_producer.py
```

Start the consumer in another terminal:
```bash
python kafka_consumer.py
```


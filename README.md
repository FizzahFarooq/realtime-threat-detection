# Real-Time Cyber Threat Detection Pipeline

A streaming pipeline that ingests network traffic events, scores them for anomalies using a machine learning model, and shows results on a live dashboard.

---

## What This Does

Traffic events are generated and sent to Kafka. Spark Structured Streaming reads the stream, groups events into sliding time windows, and runs each window through an Isolation Forest model to flag anomalies. Flagged results are written to a Parquet data lake. A Flask API reads that data and a D3.js dashboard displays it, refreshing every few seconds.

> **Note:** The traffic used here is simulated, not real network data. This is a proof of concept built to test the pipeline end to end, not a production detection system.

---

## Architecture

* **`generator.py`** — Simulates network traffic and sends it to a Kafka topic.
* **Apache Kafka** — Message broker for the incoming event stream.
* **`stream_processor.py`** — Spark Structured Streaming job that windows the data and applies the ML model.
* **Parquet Data Lake** — Stores the flagged anomaly windows.
* **`app.py`** — Flask API that reads the data lake and serves it as JSON.
* **`templates/index.html`** — D3.js dashboard that polls the API and renders charts and alerts.

*All Kafka and Spark services run through Docker Compose.*

---

## ⚙️ Setup & Installation

### Requirements
* Docker
* Python 3.9+
* pip

### Steps
1. Start the infrastructure services:
docker-compose up -d

2. Install the required Python packages:
pip install -r requirements.txt

3. Running the Pipeline
Start these components in separate terminal windows:

# Terminal 1: Sends simulated traffic to Kafka
python generator.py

# Terminal 2: Reads from Kafka, scores anomalies, writes to Parquet
python stream_processor.py

# Terminal 3: Serves the dashboard
python app.py

Once all scripts are running, open your browser and navigate to:
http://localhost:8085

# Utility Files Description
train_model.py — Trains the Isolation Forest model on synthetic data and saves it.

export_for_d3.py — One-off script to export Parquet data lake snapshots to JSON format.

read_parquet.py — Utility script to inspect the internal data lake contents.

docker-compose.yml — Defines Zookeeper, Kafka, Spark master, and Spark worker container environments.

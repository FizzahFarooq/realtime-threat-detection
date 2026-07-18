Real-Time Cyber Threat Detection Pipeline
A streaming pipeline that ingests network traffic events, scores them for anomalies using a machine learning model, and shows results on a live dashboard.
What this does
Traffic events are generated and sent to Kafka. Spark Structured Streaming reads the stream, groups events into sliding time windows, and runs each window through an Isolation Forest model to flag anomalies. Flagged results are written to a Parquet data lake. A Flask API reads that data and a D3.js dashboard displays it, refreshing every few seconds.
Note: the traffic used here is simulated, not real network data. This is a proof of concept built to test the pipeline end to end, not a production detection system.
Architecture

generator.py — simulates network traffic and sends it to a Kafka topic
Kafka — message broker for the incoming event stream
stream_processor.py — Spark Structured Streaming job that windows the data and applies the ML model
Parquet data lake — stores the flagged anomaly windows
app.py — Flask API that reads the data lake and serves it as JSON
templates/index.html — D3.js dashboard that polls the API and renders charts and alerts

All Kafka and Spark services run through Docker Compose.
Setup
Requirements: Docker, Python 3.9+, pip
docker-compose up -d
pip install -r requirements.txt
Running it
Start these in separate terminals:
python generator.py          # sends simulated traffic to Kafka
python stream_processor.py   # reads from Kafka, scores anomalies, writes to Parquet
python app.py                # serves the dashboard at localhost:8085
Open http://localhost:8085 in a browser.
Files

train_model.py — trains the Isolation Forest model on synthetic data and saves it
export_for_d3.py — one-off script to export Parquet data to JSON
read_parquet.py — utility to inspect the data lake contents
docker-compose.yml — defines Zookeeper, Kafka, Spark master, and Spark worker

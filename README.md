# Real-Time CDC Streaming Data Pipeline

A lightweight, production-grade end-to-end Change Data Capture (CDC) streaming pipeline designed for continuous ingestion, processing, and real-time visualization of database transactions. Built following Medallion Architecture (Bronze, Silver, Gold) using zero-overhead embedded analytics.

---

## Architecture Overview

```text
+-----------------------+      +-------------------+      +-------------------+
|  Neon PostgreSQL DB   | ---> | Debezium Connect  | ---> |   Apache Kafka    |
| (Source Transactions) |      | (CDC / WAL Logs)  |      |  (Topic Buffers)  |
+-----------------------+      +-------------------+      +-------------------+
                                                                    |
                                                                    v
+-----------------------+      +-------------------+      +-------------------+
|   chDB / Streamlit    | <--- |   MinIO Object    | <--- |   Apache Spark    |
| (Gold Analytics / UI) |      | (Delta Lake S3)   |      | (Structured Stream|
+-----------------------+      +-------------------+      +-------------------+

```

* **Source Layer:** Neon PostgreSQL with Logical Replication (`pgoutput`) enabled.
* **Ingestion Layer:** Debezium PostgreSQL Connector monitoring WAL (Write-Ahead Logging) changes into Apache Kafka topics.
* **Processing Layer (Spark Structured Streaming):**
* **Bronze Layer:** Raw Kafka CDC event records stored as Delta Lake tables on MinIO.
* **Silver Layer:** Cleaned, schema-enforced, and deduplicated transaction data stored as Delta Lake tables.


* **Analytical / Gold Layer:** **chDB** (Embedded in-process ClickHouse SQL engine) querying Delta Parquet files directly from MinIO with **Streamlit** dark-mode UI rendering.

---

## Repository Structure

```text
cdc-streaming-pipeline/
├── chdb/
│   ├── app.py               # Streamlit real-time dashboard UI (Chart.js styled Altair)
│   ├── chdb_analytics.py    # CLI analytical script querying Delta files via chDB
│   └── Dockerfile           # Lightweight container configuration for UI
├── database/
│   └── neon/                # Database initialization scripts and schemas
├── debezium/
│   └── register_connector.sh # Shell script to register Debezium CDC source connector
├── infra/
│   └── docker-compose.yml   # Multi-container orchestration (Kafka, Spark, MinIO, UI)
├── scripts/                 # Utility scripts for environment setup
├── simulator/
│   └── seed_database.py     # Python WAL event generator (One-time or continuous stream)
├── streaming/
│   ├── bronze_cdc.py        # Spark streaming job: Kafka -> Bronze Delta
│   ├── silver_cdc.py        # Spark streaming job: Bronze Delta -> Silver Delta
│   ├── spark_session.py     # Centralized PySpark session builder with S3A credentials
│   └── runner.py            # Streaming pipeline runner (Bronze & Silver concurrently)
├── .env.example             # Environment variable template
├── docker-compose.yml       # Primary Docker Compose configuration
└── requirements.txt         # Python dependencies (chdb, streamlit, altair, pyspark)

```

---

## Quick Start & Deployment

### 1. Prerequisites

* **Docker** & **Docker Compose** installed.
* **Python 3.10+** environment.

### 2. Environment Configuration

Clone the repository and prepare your environment variables:

```bash
git clone https://github.com/abhirajadhikary/cdc-streaming-pipeline.git
cd cdc-streaming-pipeline
cp .env.example .env

```

### 3. Spin Up Infrastructure

Start all core streaming containers (Kafka, Zookeeper, Debezium, MinIO, Spark):

```bash
docker-compose up -d

```

---

## Running the Pipeline

### Step 1: Register Debezium CDC Connector

Register the PostgreSQL CDC connector to start publishing WAL events to Kafka:

```bash
./debezium/register_connector.sh

```

Verify connector health:

```bash
curl -s http://localhost:8083/connectors/neon-postgres-cdc/status | jq .

```

---

### Step 2: Launch Spark Streaming Job

Submit the Bronze $\rightarrow$ Silver PySpark streaming process inside the Spark Master container:

```bash
docker exec -it cdc-spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-apps/streaming/runner.py

```

---

### Step 3: Seed Transactions (Database Simulator)

Generate test database events to feed the pipeline in real-time:

* **One-Time Batch:**
```bash
python simulator/seed_database.py --seed 10

```

* **Continuous Streaming Stream (1 event/sec):**
```bash
python simulator/seed_database.py --continuous --interval 1.0

```



---

### Step 4: Run Real-Time Gold Dashboard (chDB)

Launch the dark-theme dashboard UI querying MinIO Parquet files directly using `chdb`:

```bash
pip install -r requirements.txt
streamlit run chdb/app.py

```

Access the UI at `http://localhost:8501`. Features include:

* **Interactive Chart Styles:** Dynamic switching between **Area**, **Line**, and **Bar** charts.
* **Color Themes:** Support for **Azure**, **Sunset**, and **Neon** styling.
* **Real-time Metrics:** Live updates for **Peak Max**, **Average**, and **Sum Total** spend stats.

---

## Useful Utilities & Verification Commands

* **List Active Kafka Topics:**
```bash
docker exec -it cdc-kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

```


* **Consume Live Kafka CDC Messages:**
```bash
/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic neon_cdc.public.customer_transactions --from-beginning

```


* **Run Standalone chDB Analytics via CLI:**
```bash
python chdb/chdb_analytics.py

```

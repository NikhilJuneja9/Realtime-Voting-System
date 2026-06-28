# Realtime Voting System

## Introduction

This project is a real-time voting analytics demo that simulates an election pipeline end to end. It generates synthetic voter and candidate data, stores it in PostgreSQL, streams vote events through Kafka, aggregates them with Apache Spark Structured Streaming, and displays live results in a Streamlit dashboard.

The system is useful for understanding how event-driven architectures can be used for real-time dashboards, analytics, and monitoring in data engineering projects.

## Features

- Synthetic generation of candidate and voter records
- PostgreSQL storage for candidates, voters, and vote transactions
- Kafka-based event streaming for vote events and aggregated outputs
- Apache Spark Structured Streaming for real-time aggregation
- Streamlit dashboard for monitoring live election metrics
- Docker-based setup for Kafka, Zookeeper, PostgreSQL, and Spark services

## Project Structure

- main.py — creates the PostgreSQL tables and seeds initial candidate and voter data
- voting.py — simulates voting activity and sends vote events to Kafka
- spark-streaming.py — consumes vote messages and produces aggregated streams
- streamlit-app.py — displays the live dashboard and charts
- docker-compose.yml — starts the supporting infrastructure services
- requirements.txt — Python dependencies for the project
- checkpoints/ — Spark checkpoint folders used by streaming jobs
- test.py — a small Flask sample app, not part of the main voting pipeline

## Prerequisites

Before running the project, make sure you have:

- Python 3.9 or newer
- pip
- Docker Desktop (for Kafka, PostgreSQL, and Spark services)
- Java 8 or 11 (required for local Spark execution)
- Internet access (the project fetches sample user data from a public API)

## Setup

1. Clone the repository and open it in your terminal.
2. Create and activate a virtual environment:

   On Windows:
   ```bash
   python -m venv .venv
   .\.venv\Scripts\activate
   ```

   On macOS/Linux:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Install the Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Start the required infrastructure services:
   ```bash
   docker compose up -d
   ```

5. Wait a few seconds for the containers to initialize. You can verify the services with:
   ```bash
   docker compose ps
   ```

## How to Run the Project

### 1. Seed the database

Run the data generation script to create the PostgreSQL tables and insert sample candidates and voters:

```bash
python main.py
```

### 2. Simulate voting activity

Start the voting simulation so that vote records are written into PostgreSQL and published to Kafka:

```bash
python voting.py
```

### 3. Start the Spark streaming job

Run the Spark processing script to consume the vote stream and generate aggregated outputs:

```bash
python spark-streaming.py
```

### 4. Launch the Streamlit app

Start the live dashboard:

```bash
streamlit run streamlit-app.py
```

Then open the URL shown by Streamlit, usually:

```text
http://localhost:8501
```

## Streamlit Dashboard

The Streamlit app provides a real-time view of election results, including:

- total number of registered voters and candidates
- leading candidate information
- vote counts by candidate
- turnout visualization by location

The dashboard refreshes periodically and displays the latest aggregated data from Kafka.

## Kafka Topics Used

The project uses the following Kafka topics:

- votes_topic — raw vote events
- aggregated_votes_per_candidate — aggregated vote counts by candidate
- aggregated_turnout_by_location — turnout counts by location

If needed, you can create them manually with Kafka CLI tools after the broker is running.

## Notes

- The project currently expects PostgreSQL and Kafka to be available at localhost on the default ports.
- Spark checkpoint paths are stored under the checkpoints folder.
- If you run the project from a different environment or machine, you may need to adjust the connection settings and checkpoint paths in the Python scripts.

## Example Workflow

1. Start Docker services
2. Run main.py
3. Run voting.py
4. Run spark-streaming.py
5. Open the Streamlit dashboard

This gives you a simple but complete example of how real-time data pipelines can be built and visualized.

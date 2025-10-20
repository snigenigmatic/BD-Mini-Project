# BD-Mini-Project

A real-time data processing system for monitoring network and disk performance metrics.

## Overview

This project implements a streaming data pipeline using Apache Kafka for data ingestion and Apache Spark for real-time analytics. The system processes network and disk usage data to generate performance alerts based on configurable thresholds.

## Components

- **Kafka Consumers**: Collect network and disk metrics from streaming sources
- **Spark Jobs**: Process data using sliding window aggregations to detect performance anomalies
- **Alert System**: Generate notifications when metrics exceed predefined thresholds

## Usage

Run the Kafka consumer to collect data:
```
uv run consumer2.py
```

Process the collected data:
```
uv run spark_job_2_new.py
```

## Output

The system generates CSV reports with windowed performance metrics and alert classifications for network flooding, disk thrashing, and potential DDoS scenarios.
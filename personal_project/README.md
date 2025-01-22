# Weather Analysis Project

## Overview

The Weather Analysis Project is a data engineering initiative designed to process and analyze weather data using batch and streaming approaches. It showcases the integration of modern data engineering tools to handle data ingestion, transformation, storage, and visualization. 

The project includes a batch data pipeline for processing historical weather data and a streaming pipeline for near real-time data processing. The insights are presented through dashboards built in Tableau.

---

## Project Features

### Batch Processing
- **Purpose**: Processes historical weather data for multiple cities.
- **Workflow**:
  - Data ingestion from AWS S3 into the **Landing Database** in Snowflake.
  - Data transformations using **dbt Core**.
  - Processed data loaded into the **Analytics Database** in Snowflake.
  - Dashboard creation in Tableau for visualizing trends and insights.
- **Tools & Technologies**:
  - AWS S3, Snowflake, dbt Core, Apache Airflow, Docker, Tableau.

### Streaming Processing
- **Purpose**: Handles real-time weather data ingestion and analysis.
- **Workflow**:
  - Data fetched from the **OpenWeatherMap API** for selected cities.
  - Real-time data delivery to **AWS Kinesis Firehose**.
  - Data stored in **Iceberg Tables** using AWS Glue.
  - External catalogs created in **Snowflake** and queried with **Athena** and Snowflake.
  - Real-time dashboard in Tableau.
- **Tools & Technologies**:
  - OpenWeatherMap API, AWS Kinesis Firehose, AWS Glue, Iceberg Tables, Amazon Athena, Snowflake, Tableau, Docker.

---

## Key Highlights
- **Integration of Batch and Stream Pipelines**: Demonstrates hybrid processing for weather data.
- **Automation**: Orchestrated workflows with **Apache Airflow**.
- **Containerized Environment**: Reproducible setup using Docker and Docker Compose.
- **Real-time Insights**: Interactive dashboards for real-time and historical weather trends.
- **Scalable Architecture**: Use of cloud services like AWS and Snowflake for scalability and performance.

---

## Folder Structure
- **`batch/`**: Contains resources for the batch processing pipeline, including:
  - `dags/`: Airflow DAGs for orchestrating the workflow.
  - `dbt/snowflake/`: dbt Core project for data transformations.
  - `Dockerfile` and `docker-compose.yml`: Setup for containerized batch environment.
- **`stream/`**: Contains resources for the streaming pipeline, including:
  - Python script for real-time data ingestion and delivery to AWS Kinesis Firehose.
  - Dockerized environment for running streaming jobs.

---

## Getting Started

### Prerequisites
- Docker and Docker Compose installed.
- Access to AWS services (S3, Kinesis Firehose, Glue, Athena).
- Snowflake account and API key for OpenWeatherMap.
- Tableau Desktop or Tableau Online for dashboard visualization.

### Steps to Run
1. Clone this repository.
2. Set up environment variables for API keys, AWS credentials, and database connections.
3. Start the batch processing pipeline:
   - Build and run the Docker containers using `docker-compose`.
   - Monitor Airflow for batch workflow execution.
4. Start the streaming pipeline:
   - Configure AWS Kinesis Firehose and Glue.
   - Run the streaming script to ingest real-time weather data.
5. View the dashboards in Tableau for insights.

---

## Results
- **Batch Dashboard**: Provides insights into historical weather patterns for selected cities.
- **Real-Time Dashboard**: Displays live weather updates and metrics in near real-time.

---

# ETL Sales Data Pipeline with Airflow

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Daily Workflow](#daily-workflow)
- [Installation and Setup](#installation-and-setup)
  - [Prerequisites](#prerequisites)
  - [Setup](#setup)
- [Usage](#usage)

## Overview

This project emulates a typical ETL process found in many companies where data files are sent daily to a data lake, then scanned and processed for further analysis.In this guide, you'll create an ETL (Extract/Transform/Load) data pipeline from data ingestion to a BI dashboard output using Apache Airflow for orchestration. The pipeline processes daily sales and inventory transactional tables sourced from a relational database. The transformed data is then stored in an S3 data warehouse and finally read into a BI dashboard. This entire pipeline is set to run daily at 2am UTC.

## Architecture

![Pipeline Diagram](./diagrams/pipeline_diagram.jpg)

The architecture leverages primarily AWS services, with a focus on a cost-effective and scalable approach:

- **Data Source**: Daily sales and inventory data from an OLTP database (e.g., Snowflake) are formatted as CSV files.
- **AWS S3**: Acts as our data lake storage. It holds both the input transactional tables and the transformed data.
- **Apache Airflow**: Orchestrates the ETL workflow.
- **AWS EC2**: Hosts the Airflow instance.
- **AWS CloudWatch**: Scheduled to trigger the EC2 start and stop actions and initiate the ETL pipeline daily at 2am UTC.
- **AWS Lambda**: Validates the incoming data files in S3 and controls the EC2 instance's state.
- **AWS EMR & Spark**: Processes and transforms the data.
- **AWS Glue & Athena**: Acts as our serverless data warehouse solution.
- **Superset**: Provides a Business Intelligence dashboard for visualizing and analyzing the transformed data.

## Daily Workflow:

1. At 2am UTC, the OLTP database (e.g., Snowflake) sends CSV-formatted transactional data tables to an S3 bucket.
2. AWS CloudWatch triggers the start of the EC2 instance that hosts our Airflow application.
3. Once the EC2 instance is up, AWS CloudWatch triggers our Lambda function to:
   - Validate the input files in S3.
   - Start the Airflow DAG if validation passes.
4. The Airflow DAG processes the data through several stages, from extraction to loading.
5. After the DAG execution, CloudWatch triggers the Lambda function to stop the EC2 instance to save costs.
  
## Installation and Setup

### Prerequisites

- AWS Account with necessary access privileges
- Snowflake Account
- Docker (for Superset and Airflow)
- Apache Airflow and Apache Superset installed on an EC2 instance (Ubuntu)

### Setup

### 1. Setting Up S3 Bucket

[Detailed instructions on setting up the S3 bucket structure and content placement.]

### 2. EC2 Instance Setup

[Instructions on creating an EC2 instance, installing necessary packages, and setting up Airflow.]

### 3. Lambda Function

This function has two primary responsibilities:
   - Validating the presence and integrity of input files in S3.
   - Controlling the EC2 instance state (start/stop).

### 4. CloudWatch Alarms and Scheduling

- Create a CloudWatch event to trigger the EC2 instance to start daily at 2am UTC.
- Set another event to trigger the Lambda function once the EC2 instance is fully operational.
- Optionally, configure CloudWatch to stop the EC2 instance after the Airflow DAG has completed its run to optimize costs.

### 5. Airflow DAG Setup

[Details on configuring the Airflow DAG, including the sequence of operators and tasks.]

### 6. AWS EMR & Spark Configuration

[Instructions on how to set up EMR clusters and integrate with Spark for data processing.]

### 7. AWS Glue & Athena

[Steps to set up AWS Glue Crawlers and integrating with Athena for querying the data.]

### 8. Superset Installation

[Instructions on setting up Superset for data visualization and connecting it to Athena.]

## Usage

- Run the ETL process through Airflow.
- Monitor and manage the ETL tasks through the Airflow UI.
- Once the data is processed and loaded, use Apache Superset to create and view various reports and dashboards.

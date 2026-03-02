# HR Data Lakehouse: Cloud-Native ELT Pipeline

A data engineering project that orchestrates the movement of HR data from source systems to an AWS Data Lake, utilizing **Apache Airflow** for orchestration and **AWS Athena** for serverless transformation and analytics.

## Architecture
The pipeline follows an **ELT (Extract, Load, Transform)** pattern:

1.  **Extract:** Python script generates mock HR data (simulating source API).
2.  **Load:** Airflow pushes raw CSV data to AWS S3 (Bronze Layer).
3.  **Transform:** Airflow triggers AWS Athena to convert CSVs into optimized Parquet tables (Silver/Gold Layer).

## Key Features
* **Orchestration:** Built with **Astronomer (Airflow 2.9)** to schedule and monitor workflows.
* **Infrastructure as Code:** Uses **Boto3** and **Airflow Operators** to manage AWS resources programmatically.
* **Data Lake Architecture:**
    * **Bronze Layer:** Raw CSV ingestion (Source of Truth).
    * **Silver/Gold Layer:** Optimized **Apache Parquet** tables via AWS Athena.
* **Performance Optimization:** Implemented **Partitioning** strategies and columnar storage to reduce query costs and improve performance by ~60%.
* **Security:** Environment variables and Airflow Connections used for secure credential management.

## Performance Optimization
Implemented a partitioned Data Lakehouse architecture to optimize query costs and speed.

### Benchmark Results
I compared a full table scan vs. a partition-pruned query on the Silver Layer.

| Query Type | Data Scanned | Cost Reduction |
| :--- | :--- | :--- |
| **Full Scan** (All Depts) | 76.63 KB | - |
| **Partitioned** (Engineering) | 20.38 KB | **~73.4%** |

*Evidence:*
> By partitioning the Silver layer by `department`, Athena only reads the relevant S3 folder, skipping 73% of the data. This directly translates to lower AWS costs.

## Tech Stack
* **Language:** Python 3.9+, SQL (Presto/Trino dialect)
* **Orchestration:** Apache Airflow (via Astronomer CLI)
* **Cloud Provider:** AWS (S3, Athena, Glue Catalog)
* **Containerization:** Docker

## Project Structure
```text
.
├── dags/                   # Airflow DAGs (Workflows)
│   └── extraction_example.py # Main ELT DAG (S3 Uploads & Logging)
├── include/                # SQL and Helper scripts
│   └── sql/
│       └── athena/         # DDL and DML scripts for Athena
├── src/                    # Utility Python scripts (Boto3 wrappers)
│   ├── upload_to_s3.py     # Local upload testing
│   └── list_files.py       # S3 verification script
├── Dockerfile              # Astro Runtime configuration
└── requirements.txt        # Python dependencies (boto3, amazon-providers)
```

## How to Run Locally

### 1. Prerequisites
* Docker Desktop (Running)
* Astronomer CLI (`astro`)
* An AWS Account with S3 and Athena access

### 2. Clone & Prepare
```bash
git clone [https://github.com/kucera-tomas/hr-data-lakehouse.git](https://github.com/kucera-tomas/hr-data-lakehouse.git)
cd hr-data-lakehouse
```

### 3. Local Environment Setup (For standalone scripts)
Create a `.env` file in the root directory to run the scripts in `src/` locally:
```text
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
AWS_BUCKET_NAME=your-bucket-name
```

### 4. Start Airflow
Run the Astronomer local development environment:
```bash
astro dev start
```
*Wait for the command to finish. It will spin up 4 Docker containers.*

### 5. Configure AWS Connection (Crucial Step)
Airflow does **not** read your `.env` file by default. You must configure the connection in the UI.

1.  Open your browser to `http://localhost:8080` (User: `admin`, Pass: `admin`).
2.  Go to **Admin** -> **Connections**.
3.  Click the **+** (Plus) button to add a new connection.
4.  Fill in the details:
    * **Connection Id:** `aws_default`
    * **Connection Type:** `Amazon Web Services`
    * **AWS Access Key ID:** *(Your Key)*
    * **AWS Secret Access Key:** *(Your Secret)*
5.  Click **Save**.

### 6. Run the Pipeline
1.  In the Airflow UI, find the `extraction_example` DAG.
2.  Toggle the switch to **Unpause** it.
3.  Click the **Trigger DAG** (Play button) to run it manually.
4.  Check your AWS S3 Console to see the new files appear!

---
*Created as a portfolio project for Data Engineering applications.*
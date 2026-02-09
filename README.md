# LogiSpark: End-to-End Logistics Analytics Pipeline 🚚

![Python](https://img.shields.io/badge/Python-3.9-green)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4-orange)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7-red)
![Docker](https://img.shields.io/badge/Docker-Custom%20Image-2496ED)
![GCP](https://img.shields.io/badge/Google_Cloud-BigQuery-yellow)
![Terraform](https://img.shields.io/badge/Terraform-IaC-purple)

## 📖 Project Overview
LogiSpark is a data engineering project simulating a **Logistics Analytics Platform**. It processes high-volume shipment data to calculate route efficiency and costs.

Unlike standard tutorials, this project solves the common **"Airflow + Spark" dependency hell** by building a **Custom Docker Image** that embeds OpenJDK and PySpark directly into the Airflow Worker. This ensures a stable, production-grade execution environment without relying on fragile external connections.

---

## 🏗 Architecture
![Project Workflow](./images/End-to-End%20Logistics%20Analytics%20Pipeline.drawio.svg)

### Key Components:
1.  **Infrastructure as Code:** Terraform provisions GCS Buckets and BigQuery Datasets.
2.  **Orchestration (Airflow):** Manages the ETL DAG.
    * *Custom Image:* Built on top of `apache/airflow:2.7.1` but injected with `openjdk-17` to enable native Spark execution.
3.  **Processing (Apache Spark):**
    * Runs in **Local Mode** within the Airflow container.
    * Performs data cleaning, aggregation, and Parquet conversion.
4.  **Storage & Warehousing:**
    * **GCS:** Stores processed Parquet files (Data Lake).
    * **BigQuery:** Serving layer for analytics (Data Warehouse).

---

## 🛠 Tech Stack
* **Language:** Python 3.9, SQL
* **Processing:** Apache Spark (PySpark 3.4)
* **Orchestration:** Apache Airflow 2.7
* **Containerization:** Docker (Custom Build)
* **Cloud:** Google Cloud Platform (GCS, BigQuery)
* **IaC:** Terraform

---

## 🚀 How to Run

### Prerequisites
* Docker Desktop installed (4GB+ RAM recommended)
* Google Cloud Platform Service Account Key (`google_credentials.json`)

### Step 1: Setup Infrastructure (Terraform)
```bash
cd terraform
# Update main.tf with your Project ID
terraform init
terraform apply
```

### Step 2: Build & Start Containers
Since we use a custom Dockerfile to fix Java dependencies, you must build the image first.

```bash
# Place your google_credentials.json in the project root
docker-compose build
docker-compose up -d
```

### Step 3: Configure Airflow
1. Go to http://localhost:8080 (User/Pass: admin/admin)
2. Navigate to Admin > Connections.
3. Edit google_cloud_default:
    * Project Id: Your GCP Project ID
    * Keyfile Path: /opt/airflow/google_credentials.json
4. Save.

### Step 4: Trigger the Pipeline
1. Unpause the logistics_spark_pipeline_v1 DAG.
2. Trigger the DAG manually.
3. Monitor the steps:
    * generate_data: Creates raw JSON.
    * spark_process: Runs PySpark to aggregate data.
    * upload_to_gcs: Pushes Parquet files to Cloud Storage.
    * load_to_bq: Loads data into BigQuery table route_summary.

---

## 💡 Design Decisions (Why I built it this way?)
### 1. Why Custom Docker Image?
Standard Airflow images lack Java/JDK, which is required for Spark. Instead of managing a complex multi-container network (Airflow + Spark Master + Spark Worker), I created a self-contained image. This reduces network latency and simplifies the development environment while maintaining the power of Spark's API.

### 2. Why Parquet?
I converted JSON to Parquet before uploading to BigQuery because Parquet is columnar (faster for analytics) and retains schema information, making the BigQuery load process more reliable than loading raw CSV/JSON.

---

## 👤 Author
Ananyot Damnoenkiat

[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/ananyot-damnoenkiat)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
import sys
import os
import glob
from google.cloud import storage

# --- CONFIGURATION ---
# *** UPDATE THESE VARIABLES ***
GCP_PROJECT_ID = "logisparks-analytics"
GCS_BUCKET_NAME = "logisparks-data-lake-ananyot" 
BQ_DATASET_NAME = "logisparks_warehouse"
BQ_TABLE_NAME = "route_summary"

LOCAL_DATA_DIR = "/opt/airflow/data"
RAW_FILE_NAME = "raw_data.json"
PARQUET_DIR_NAME = "processed_parquet"

# Define Paths
RAW_FILE_PATH = os.path.join(LOCAL_DATA_DIR, RAW_FILE_NAME)
PARQUET_OUTPUT_PATH = os.path.join(LOCAL_DATA_DIR, PARQUET_DIR_NAME)

# --- PYTHON FUNCTIONS ---

def _upload_parquet_to_gcs():
    """
    Uploads all parquet files from local directory to GCS.
    Parquet output from Spark is a directory containing multiple .parquet parts.
    """
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    
    # Find all .parquet files inside the output directory
    files = glob.glob(f"{PARQUET_OUTPUT_PATH}/*.parquet")
    
    if not files:
        raise Exception("No parquet files found to upload!")
        
    print(f"Found {len(files)} parquet files.")
    
    for file_path in files:
        file_name = os.path.basename(file_path)
        # Upload to 'processed/' folder in GCS
        blob_name = f"processed/{file_name}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
        print(f"Uploaded {file_name} to gs://{GCS_BUCKET_NAME}/{blob_name}")

# --- DAG DEFINITION ---

default_args = {
    'owner': 'DataEngineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1),
}

with DAG(
    'logistics_spark_pipeline_v1',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='End-to-End pipeline with Embedded Spark'
) as dag:

    # Task 1: Generate Data using Python script
    t1_generate = BashOperator(
        task_id='generate_data',
        bash_command=f'python /opt/airflow/scripts/data_generator.py'
    )

    # Task 2: Process Data using Spark
    # Since we installed Java & Spark in Dockerfile, we can just run python script
    # that imports pyspark. It will run in "Local Mode" automatically.
    t2_spark_process = BashOperator(
        task_id='spark_process',
        bash_command=f'python /opt/airflow/scripts/spark_transformation.py {RAW_FILE_PATH} {PARQUET_OUTPUT_PATH}'
    )

    # Task 3: Upload Parquet to GCS
    t3_upload_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=_upload_parquet_to_gcs
    )

    # Task 4: Load to BigQuery
    # Only loads the parquet files
    t4_load_bq = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket=GCS_BUCKET_NAME,
        source_objects=['processed/*.parquet'],
        source_format='PARQUET',
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='google_cloud_default'
    )

    # Flow
    t1_generate >> t2_spark_process >> t3_upload_gcs >> t4_load_bq
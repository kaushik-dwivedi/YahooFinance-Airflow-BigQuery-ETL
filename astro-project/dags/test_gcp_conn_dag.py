from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sandbox_bigquery_test",
    default_args=default_args,
    description="Transform and upload NSE stock CSVs to BigQuery 'Airflow' dataset",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bigquery", "upload", "stocks", "transform"],
) as dag:

    def transform_and_upload_to_bigquery():
        # Load credentials from Airflow connection
        conn = BaseHook.get_connection("my_gcp_conn")
        key_path = conn.extra_dejson.get("key_path")
        project_id = conn.extra_dejson.get("project", "internal-data-bigquery-kaushik")

        if not key_path or not os.path.exists(key_path):
            raise ValueError(f"Missing or invalid key_path: {key_path}")

        credentials = service_account.Credentials.from_service_account_file(key_path)
        client = bigquery.Client(project=project_id, credentials=credentials)

        # Dataset and file location
        dataset_id = "Airflow"
        data_folder = "/usr/local/airflow/include/data"

        if not os.path.exists(data_folder):
            raise FileNotFoundError(f"Data folder not found: {data_folder}")

        for filename in os.listdir(data_folder):
            if filename.endswith("_last30days.csv"):
                symbol = filename.replace("_last30days.csv", "")
                table_name = f"{symbol}_last30days"
                table_id = f"{project_id}.{dataset_id}.{table_name}"
                file_path = os.path.join(data_folder, filename)

                print(f"📤 Preparing {file_path} for upload to {table_id}...")

                try:
                    # Load and transform data
                    df = pd.read_csv(file_path)
                    df["Date"] = pd.to_datetime(df["Date"]).dt.date  # Strip time from datetime
                    df.to_csv(file_path, index=False)  # Overwrite with cleaned data

                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_TRUNCATE",
                        autodetect=False,
                        schema=[
                            bigquery.SchemaField("Date", "DATE"),
                            bigquery.SchemaField("Open", "FLOAT"),
                            bigquery.SchemaField("High", "FLOAT"),
                            bigquery.SchemaField("Low", "FLOAT"),
                            bigquery.SchemaField("Close", "FLOAT"),
                            bigquery.SchemaField("Volume", "INTEGER"),
                            bigquery.SchemaField("Dividends", "FLOAT"),
                            bigquery.SchemaField("Stock_Splits", "FLOAT"),
                        ],
                        source_format=bigquery.SourceFormat.CSV,
                        skip_leading_rows=1,
                    )

                    with open(file_path, "rb") as source_file:
                        job = client.load_table_from_file(
                            source_file,
                            table_id,
                            job_config=job_config
                        )
                        job.result()
                        print(f"✅ Successfully uploaded to {table_id}")

                except Exception as e:
                    print(f"❌ Failed to upload {filename}: {e}")

    upload_task = PythonOperator(
        task_id="transform_and_upload_csvs_to_bigquery",
        python_callable=transform_and_upload_to_bigquery
    )

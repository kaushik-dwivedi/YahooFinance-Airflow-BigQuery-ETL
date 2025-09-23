from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.utils import timezone
from datetime import timedelta

with DAG(
    dag_id="sandbox_bigquery_test",
    start_date=timezone.utcnow() - timedelta(days=1),
    schedule=None,
    catchup=False,
) as dag:

    get_data = BigQueryGetDataOperator(
        task_id="get_weather_data",
        dataset_id="austin_weather",
        table_id="austin_weather",
        max_results=5,
        selected_fields="date,temp",
        gcp_conn_id="my_gcp_conn",  # ✅ uses your custom connection
        location="US",
    )
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import os

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_nse_stock_data",
    default_args=default_args,
    description="Fetch NSE stock data for the last 30 days and trigger BigQuery DAG",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nse", "stocks", "bigquery"],
) as dag:

    def fetch_stock_data():
        data_folder = "/usr/local/airflow/include/data"
        os.makedirs(data_folder, exist_ok=True)

        symbols = ["ALPHA.NS", "JUNIORBEES.NS"]
        end_date = datetime.today()
        start_date = end_date - timedelta(days=30)

        for symbol in symbols:
            print(f"Fetching data for {symbol}...")
            stock = yf.Ticker(symbol)
            data = stock.history(
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d")
            )

            if not data.empty:
                # Reset index to move Date from index to column
                data.reset_index(inplace=True)

                # Rename columns to match BigQuery schema
                data.rename(columns={
                    "Date": "Date",
                    "Open": "Open",
                    "High": "High",
                    "Low": "Low",
                    "Close": "Close",
                    "Volume": "Volume",
                    "Dividends": "Dividends",
                    "Stock Splits": "Stock_Splits"
                }, inplace=True)

                # Ensure Volume is integer
                data["Volume"] = data["Volume"].fillna(0).astype(int)

                # Save to CSV without index
                filename = os.path.join(
                    data_folder,
                    f"{symbol.replace('.NS','')}_last30days.csv"
                )
                data.to_csv(filename, index=False)
                print(f"✅ Saved {symbol} data to {filename}")
            else:
                print(f"⚠️ No data found for {symbol}")

    fetch_task = PythonOperator(
        task_id="fetch_stock_data_task",
        python_callable=fetch_stock_data
    )

    trigger_bq_dag = TriggerDagRunOperator(
        task_id="trigger_sandbox_bigquery_test",
        trigger_dag_id="sandbox_bigquery_test",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    fetch_task >> trigger_bq_dag

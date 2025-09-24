# sandbox_bigquery_test

This Apache Airflow DAG automates the transformation and upload of NSE stock data (in CSV format) to Google BigQuery. It is designed for local development using Astronomer or Docker-based Airflow setups, and integrates securely with GCP via an Airflow-managed service account.

---

## 📦 Features

- Reads CSV files from a local folder (`include/data`)
- Transforms `Date` column to match BigQuery `DATE` type
- Uploads to BigQuery tables in the `Airflow` dataset
- Uses Airflow connection (`my_gcp_conn`) for secure GCP authentication
- Modular and production-ready for orchestration

---

## 🛠️ Setup

### 1. Prerequisites

- Apache Airflow (v2+)
- Python 3.8+
- Docker (if using Astronomer or local containers)
- Google Cloud service account with BigQuery access

### 2. Airflow Connection

Create a connection in Airflow UI:

- **Conn ID**: `my_gcp_conn`
- **Conn Type**: `Google Cloud`
- **Extras**:
  ```json
  {
    "key_path": "/usr/local/airflow/include/internal-data-bigquery-kaushik.json",
    "project": "internal-data-bigquery-kaushik"
  }


Mount the JSON key file into your container:

-v ./include/internal-data-bigquery-kaushik.json:/usr/local/airflow/include/internal-data-bigquery-kaushik.json

📂 File Structure

astro-project/
├── dags/
│   └── sandbox_bigquery_test.py
├── include/
│   ├── data/
│   │   ├── ALPHA_last30days.csv
│   │   └── JUNIORBEES_last30days.csv
│   └── internal-data-bigquery-kaushik.json



---

Let me know if you want to include screenshots, dbt chaining instructions, or a sample CSV in the repo. I can help you polish it for public or team use.
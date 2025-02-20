from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# API & Database details
URL = "https://web-api.tp.entsoe.eu/api"

load_dotenv("dags/.env")  # Use a forward slash for cross-platform paths

ENTSOE_API_KEY = os.getenv("ENTSOE_API_KEY")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID")

PARAMS = {
    "securityToken": ENTSOE_API_KEY,
    "documentType": "A44",
    "processType": "A01",
    "in_Domain": "10Y1001A1001A83F",  # Germany EIC
    "periodStart": "202401010000",
    "periodEnd": "202402010000",
}

# DAG Definition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    "entsoe_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for ENTSO-E API data (Historical + Daily Run)",
    schedule_interval="@daily",  # Daily Run
    catchup=True,  # Enable historical runs
    max_active_runs=1,  # Process one run at a time
)


def extract_data():
    """Extracts data from ENTSO-E API"""
    response = requests.get(URL, params=PARAMS)
    if response.status_code != 200:
        raise Exception(
            f"API request failed! Status: {response.status_code},"
            f"Response: {response.text}")
    return response.text


def transform_data(xml_data):
    """Parses XML data and converts it into a Pandas DataFrame"""
    soup = BeautifulSoup(xml_data, "lxml-xml")
    rows = []

    for time_series in soup.find_all("TimeSeries"):
        start_time = time_series.find("start").text
        end_time = time_series.find("end").text

        for point in time_series.find_all("Point"):
            position = point.find("position").text
            price_tag = point.find("price.amount")
            price = float(price_tag.text) if price_tag else None

            rows.append({"start_time": start_time,
                         "end_time": end_time,
                         "position": int(position),
                         "price": price})

    df = pd.DataFrame(rows)
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])

    if df.empty:
        raise Exception("Transformed DataFrame is empty!")

    csv_path = "/tmp/entsoe_data.csv"
    df.to_csv(csv_path, index=False)  # Save for next step
    return csv_path


def load_data(csv_path):
    """Loads data into PostgreSQL database"""
    pg_hook = PostgresHook(postgres_conn_id="entsoe_connections")
    engine = pg_hook.get_sqlalchemy_engine()

    df = pd.read_csv(csv_path)
    df.to_sql("balancing_reserves",
              con=engine,
              if_exists="append",
              index=False)

    print("✅ Data loaded successfully!")


def create_table():
    """Creates the target table if it does not exist"""
    pg_hook = PostgresHook(postgres_conn_id="entsoe_connections")
    sql_query = """
    CREATE TABLE IF NOT EXISTS balancing_reserves (
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        position INT,
        price FLOAT
    );
    """
    pg_hook.run(sql_query)


# Task Definitions


extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    op_args=["{{ ti.xcom_pull(task_ids='extract_data') }}"],
    dag=dag,
)

create_table_task = PythonOperator(
    task_id="create_table",
    python_callable=create_table,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    op_args=["{{ ti.xcom_pull(task_ids='transform_data') }}"],
    dag=dag,
)

# Define Task Dependencies
extract_task >> transform_task >> create_table_task >> load_task

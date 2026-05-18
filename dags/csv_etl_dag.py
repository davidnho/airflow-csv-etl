from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


INPUT_FILE = "/home/noelsdavid/airflow/data/input_sales.csv"
OUTPUT_FILE = "/home/noelsdavid/airflow/data/output_sales_cleaned.csv"


def extract():
    df = pd.read_csv(INPUT_FILE)

    print("Extracted Data:")
    print(df.head())


def transform_and_load():
    df = pd.read_csv(INPUT_FILE)

    # Example Transformations
    df["total_amount_with_tax"] = df["total_amount"] * 1.12
    df["customer"] = df["customer"].str.upper()

    # Save cleaned file
    df.to_csv(OUTPUT_FILE, index=False)

    print("Cleaned data saved!")
    print(df.head())


with DAG(
    dag_id="csv_etl_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "csv", "beginner"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_csv",
        python_callable=extract
    )

    transform_load_task = PythonOperator(
        task_id="transform_and_load_csv",
        python_callable=transform_and_load
    )

    extract_task >> transform_load_task
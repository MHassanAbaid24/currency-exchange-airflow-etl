"""
Currency Exchange Rate ETL Pipeline
------------------------------------
Extracts daily currency exchange rates from the Frankfurter API,
transforms them into a normalized document shape, and loads them
into MongoDB. Data is passed between tasks using named Airflow XComs.
"""
from __future__ import annotations

import os
from datetime import datetime

import requests
from airflow.decorators import dag
from airflow.operators.python import PythonOperator

BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY"]
FRANKFURTER_URL = "https://api.frankfurter.dev/v2/rates"

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB = os.environ.get("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "exchange_rates")

RAW_DATA_XCOM_KEY = "raw_exchange_rates"
TRANSFORMED_DATA_XCOM_KEY = "transformed_exchange_rates"


def extract_currency_exchange_rates(**context) -> None:
    """Call the Frankfurter API and push the raw response via a named XCom."""
    params = {"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)}
    response = requests.get(FRANKFURTER_URL, params=params, timeout=30)
    response.raise_for_status()
    raw_data = response.json()  # list of {"date", "base", "quote", "rate"}

    context["ti"].xcom_push(key=RAW_DATA_XCOM_KEY, value=raw_data)


def transform_exchange_rate_data(**context) -> None:
    """Pull the raw rates via named XCom, reshape them, push the result."""
    ti = context["ti"]
    raw_data = ti.xcom_pull(
        task_ids="extract_currency_exchange_rates", key=RAW_DATA_XCOM_KEY
    )

    retrieved_at = datetime.utcnow().isoformat()

    transformed_records = []
    for entry in raw_data:
        transformed_records.append(
            {
                "base_currency": entry["base"],
                "target_currency": entry["quote"],
                "exchange_rate": entry["rate"],
                "rate_date": entry["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
        )

    ti.xcom_push(key=TRANSFORMED_DATA_XCOM_KEY, value=transformed_records)


def load_data_into_mongodb(**context) -> None:
    """Pull the transformed records via named XCom and insert them into MongoDB."""
    from pymongo import MongoClient

    ti = context["ti"]
    transformed_records = ti.xcom_pull(
        task_ids="transform_exchange_rate_data", key=TRANSFORMED_DATA_XCOM_KEY
    )

    client = MongoClient(MONGO_URI)
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]
        result = collection.insert_many(transformed_records)
        print(f"Inserted {len(result.inserted_ids)} documents into "
              f"{MONGO_DB}.{MONGO_COLLECTION}")
    finally:
        client.close()


@dag(
    dag_id="currency_exchange_etl",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():
    extract = PythonOperator(
        task_id="extract_currency_exchange_rates",
        python_callable=extract_currency_exchange_rates,
    )

    transform = PythonOperator(
        task_id="transform_exchange_rate_data",
        python_callable=transform_exchange_rate_data,
    )

    load = PythonOperator(
        task_id="load_data_into_mongodb",
        python_callable=load_data_into_mongodb,
    )

    extract >> transform >> load


currency_exchange_etl()

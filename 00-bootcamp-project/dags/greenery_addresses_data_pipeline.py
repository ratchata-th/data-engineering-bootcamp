import csv
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
PROJECT_ID = "deb2-395909"
DAGS_FOLDER = "/opt/airflow/dags"
DATA = "addresses"


def _extract_data():
    url = f"http://34.87.139.82:8000/{DATA}/" #ดึง API
    response = requests.get(url)
    data = response.json()

    with open(f"{DAGS_FOLDER}/{DATA}.csv", "w") as f: #เอามาใส่ folder
        writer = csv.writer(f)
        header = [
            "address_id",
            "address",
            "zipcode",
            "state",
            "country",
        ]
        writer.writerow(header)
        for each in data: #วนลูปเขียน Data ใน file csv
            data = [
                each["address_id"],
                each["address"],
                each["zipcode"],
                each["state"],
                each["country"],
            ]
            writer.writerow(data)


def _load_data_to_gcs(ds):
    keyfile_gcs = f"{DAGS_FOLDER}/deb2-deb2-loading-file-to-gcs-deb2-395909-f7c5acac4d2d.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from Local to GCS
    bucket_name = "deb2-bootcamp-7777"
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = f"{DAGS_FOLDER}/{DATA}.csv" #เอา file load to GCS
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)


def _load_data_from_gcs_to_bigquery():
    keyfile_bigquery = f"{DAGS_FOLDER}/00-bootcamp-project/deb2-loading-data-to-gcs-then-bigquery-deb2-395909-5c0b85866983.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    bigquery_client = bigquery.Client( #สร้าง client and load data
        project=PROJECT_ID,
        credentials=credentials_bigquery,
        location=LOCATION,
    )

    table_id = f"{PROJECT_ID}.my_deb_project_bootscamp.{DATA}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    bucket_name = "deb2-bootcamp-7777"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

#เอา 3 ฟะงกชั่นมาเชื่อต่อกัน
default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
}
with DAG(
    dag_id="greenery_addresses_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["DEB", "2023", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_to_gcs,
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery
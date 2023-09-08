# Ref: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-gcs-csv

import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account


#keyfile = os.environ.get("KEYFILE_PATH") #ปกติจะสร้าง enirvon ไว้เก็บ key service
keyfile = "deb2-deb2-loadingdata-to-bigquery-395909-71ba5f4967f9.json" #ใช้ชื่อไฟล์ตรงๆเนื่องจากอยู่ใน path เดียวกันกับ script
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "deb2-395909"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)
#กำหนด schema ที่จะ load ขึ้นไป
job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    schema=[
        bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("first_name", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("last_name", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("email", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("phone_number", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("updated_at", bigquery.SqlTypeNames.TIMESTAMP),
        bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
    ],
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at",
    ),
    clustering_fields=["first_name", "last_name"],
)

file_path = "users.csv"
with open(file_path, "rb") as f:
    table_id = f"{project_id}.my_deb_workshop.users" #ชื่อ dataset
    job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
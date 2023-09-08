from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone


def _get_data(table, dt): #ส่ง parameter เข้าไปได้
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="greenery"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = f"select * from {table} where date(created_at) = '{dt}'"
    cursor.execute(sql)
    rows = cursor.fetchall()
    for each in rows:
        print(each)


def _dump_data(table: str):
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="greenery"
    )
    pg_hook.bulk_dump(table, f"/opt/airflow/dags/{table}_export")
    

with DAG(
    dag_id="play_with_airflow_connections_and_hooks_events",
    schedule="@daily",
    start_date=timezone.datetime(2023, 8, 27),
    catchup=False,
    tags=["DEB", "2023"],
):

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
        op_kwargs={
            "table": "events",
            "dt": "2021-02-10"
        },
    )

    dump_product_data = PythonOperator(
        task_id="dump_product_data",
        python_callable=_dump_data,
        op_kwargs={"table": "products"},
    )

    dump_event_data = PythonOperator(
        task_id="dump_event_data",
        python_callable=_dump_data,
        op_kwargs={"table": "events"},
    )
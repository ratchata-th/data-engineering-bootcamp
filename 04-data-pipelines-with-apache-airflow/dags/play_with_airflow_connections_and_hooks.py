# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils import timezone

# #เชื่อมต่อ posgress ผ่าน hook
# def _get_data():
#     pg_hook = PostgresHook(
#         postgres_conn_id="my_postgres_conn",
#         schema="greenery"
#     )
#     connection = pg_hook.get_conn()
#     cursor = connection.cursor()
    
#     # No Hook connection manual
#     # parser = configparser.ConfigParser()
#     # parser.read("pipeline.conf")
#     # dbname = parser.get("postgres_config", "database")
#     # user = parser.get("postgres_config", "username")
#     # password = parser.get("postgres_config", "password")
#     # host = parser.get("postgres_config", "host")
#     # port = parser.get("postgres_config", "port")

#     # conn_str = f"dbname={dbname} user={user} password={password} host={host} port={port}"
#     # conn = psycopg2.connect(conn_str)
#     # cursor = conn.cursor()

#     #โยน sql นี้ไป
#     #CH. ลองเปลี่ยนเป็นดึงข้อมูล events ของวันที่ 2021-02-10
#     sql = """
#         select product_id, name, price, inventory from products
#     """
#     cursor.execute(sql) #สั่งรัน sql
#     rows = cursor.fetchall() #get data มาทุกแถว
#     for each in rows:
#         print(each) #print ออกมาทุกแถว


# def _dump_data(table: str): #เอา table มาวางที่ path
#     pg_hook = PostgresHook(
#         postgres_conn_id="my_postgres_conn",
#         schema="greenery"
#     )
#     pg_hook.bulk_dump(table, f"/opt/airflow/dags/{table}_export")
    

# with DAG(
#     dag_id="play_with_airflow_connections_and_hooks",
#     schedule="@daily",
#     start_date=timezone.datetime(2023, 8, 27),
#     catchup=False,
#     tags=["DEB", "2023"],
# ):

#     get_data = PythonOperator( #function get data
#         task_id="get_data",
#         python_callable=_get_data,
#     )

#     dump_product_data = PythonOperator( #function dump data โดยโยน agreement เข้าไป
#         task_id="dump_product_data",
#         python_callable=_dump_data,
#         op_kwargs={"table": "products"}, #โดยโยน agreement เข้าไป ตรง {table} บรรทัด pg_hook.bulk_dump(table, f"/opt/airflow/dags/{table}_export") ด้วยคำว่า products
#         #op_kwargs={"table": "events"},
#     )

#Validate fucntion
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

import great_expectations as ge


def _get_data():
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="greenery"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        --select MAX(price) from products
        select product_id, name, price, inventory from products
    """
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


def _validate_data():
    columns = ["product_id", "name", "price", "inventory"]
    my_df = ge.read_csv("/opt/airflow/dags/products_export", names=columns, sep="\t")
    results = my_df.expect_column_values_to_be_between(
        column="price",
        min_value=0,    # Range fix, สามารถทำเป็น dynamic ได้ min max
        max_value=95,   # แต่จะทำให้ data diff ได้ เวลาเอา Data ไปทำ ML มันจะต้องดูที่ ML setting ไว้ด้วยต้องเทรน model ใหม่
    )
    assert results["success"] is True


with DAG(
    dag_id="play_with_airflow_connections_and_hooks",
    schedule=None,
    start_date=timezone.datetime(2023, 8, 27),
	  catchup=False,
    tags=["DEB", "2023"],
):

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
    )

    dump_product_data = PythonOperator(
        task_id="dump_product_data",
        python_callable=_dump_data,
        op_kwargs={"table": "products"},
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    dump_product_data >> validate_data
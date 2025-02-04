from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="my_first_dag",
    start_date=timezone.datetime(2023, 9, 2),
    # schedule=None,
    schedule="0 17 1,16 * *",
    tags=["DEB", "2023"],
):

    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")

    t1 >> t2
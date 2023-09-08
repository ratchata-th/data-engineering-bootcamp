from airflow import DAG
from airflow.macros import ds_format
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _get_date_part(ds, **context):
    print(context)
    #print(context["ds_nodash"])
    return ds_format(ds, "%Y-%m-%d", "%Y/%m/%d/")


with DAG(
    dag_id="play_with_templating",
    #schedule=None,
    schedule="@daily", #test backdate
    #start_date=timezone.datetime(2023, 8, 27),
    start_date=timezone.datetime(2023, 8, 1),  #test backdate
    catchup=False,
    tags=["DEB", "2023"],
):

    run_this = PythonOperator(
        task_id="get_date_part",
        python_callable=_get_date_part,
        op_kwargs={"ds": "{{ ds }}"},
        #op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    )
from airflow.decorators import dag
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def get_datetime():
    return datetime.today().isoformat()


def use_datetime(**kwargs):
    task_instance = kwargs.get("task_instance")
    str_today = task_instance.xcom_pull(task_ids="get_datetime_task")
    print(str_today)


@dag(start_date=datetime.today(), tags=['user'], schedule_interval="@daily")
def xcom_demo_dag():
    start_task = EmptyOperator(task_id="start")

    get_datetime_task = PythonOperator(task_id="get_datetime_task",
                                       python_callable=get_datetime)

    use_datetime_task = PythonOperator(task_id="use_datetime_task",
                                       python_callable=use_datetime,
                                       provide_context=True)

    end_task = EmptyOperator(task_id="end")

    start_task >> get_datetime_task >> use_datetime_task >> end_task


create_xcom_demo_dag = xcom_demo_dag()

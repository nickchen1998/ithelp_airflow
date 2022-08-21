from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


@dag(tags=['user'], start_date=datetime.today())
def simple_dag():
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")
    first_task = BashOperator(task_id="first_task",
                              bash_command=f"echo execute time: {datetime.now()}")

    start_task >> first_task >> end_task


my_dag = simple_dag()

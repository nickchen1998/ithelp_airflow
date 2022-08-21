from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(dag_id="first_dag", tags=['user'], start_date=datetime.today())
with dag:
    start_task = EmptyOperator(task_id="start_task", dag=dag)
    end_task = EmptyOperator(task_id="end_task", dag=dag)
    first_task = BashOperator(task_id="first_task",
                              bash_command=f"echo execute time: {datetime.now()}", dag=dag)

    start_task >> first_task >> end_task

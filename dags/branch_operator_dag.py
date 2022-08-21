import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime


class TaskID:
    start_task_id = "start_task"
    end_task_id = "end_task"
    crawler_task_id = "crawler_task"
    success_task_id = "crawl_success"
    failed_task_id = "crawl_failed"


def crawl_ptt():
    url = "https://www.ptt.cc/bbs/index.html"
    res = requests.get(url=url)
    if res.status_code == 200:
        print(res.text)


dag = DAG(dag_id="python_operator_demo_dag", start_date=datetime.today(), tags=['user'])
with dag:
    start_task = EmptyOperator(task_id=TaskID.start_task_id)

    crawl_task = PythonOperator(task_id=TaskID.crawler_task_id,
                                python_callable=crawl_ptt)

    end_task = EmptyOperator(task_id=TaskID.end_task_id)

    start_task >> crawl_task >> end_task

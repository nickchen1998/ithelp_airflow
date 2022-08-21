import requests
from airflow.decorators import dag
from airflow.utils.trigger_rule import TriggerRule
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


def crawl_ptt_branch():
    url = "https://www.ptt.cc/bbs/index.html"
    res = requests.get(url=url)
    if res.status_code == 200:
        return TaskID.success_task_id
    else:
        return TaskID.failed_task_id


def success_crawl():
    print("爬取成功")


def failed_crawl():
    print("爬取失敗")


@dag(start_date=datetime.today(), tags=['user'], schedule_interval="@daily")
def python_operator_demo_dag():
    start_task = EmptyOperator(task_id=TaskID.start_task_id)

    crawl_task = PythonOperator(task_id=TaskID.crawler_task_id,
                                python_callable=crawl_ptt)

    end_task = EmptyOperator(task_id=TaskID.end_task_id)

    start_task >> crawl_task >> end_task


@dag(start_date=datetime.today(), tags=['user'])
def branch_operator_demo_dag():
    start_task = EmptyOperator(task_id=TaskID.start_task_id)

    crawl_task = BranchPythonOperator(task_id=TaskID.crawler_task_id,
                                      python_callable=crawl_ptt_branch)

    success_task = PythonOperator(task_id=TaskID.success_task_id,
                                  python_callable=success_crawl)

    failed_task = PythonOperator(task_id=TaskID.failed_task_id,
                                 python_callable=failed_crawl)

    end_task = EmptyOperator(task_id=TaskID.end_task_id,
                             trigger_rule=TriggerRule.NONE_FAILED)

    start_task >> crawl_task >> [success_task, failed_task] >> end_task


create_python_dag = python_operator_demo_dag()
create_branch_python_dag = branch_operator_demo_dag()

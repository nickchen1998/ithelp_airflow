from airflow.decorators import dag
from operators.sqlalchemy_operator import SQLAlchemyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from sqlalchemy.orm import Session
from models.exhibitions import Exhibitions, Base
from sqlalchemy import inspect


class Settings:
    conn_id = "postgres_connection"
    start_task_id = "start_task"
    end_task_id = "end_task"
    crawler_task_id = "crawler_task"
    create_table_task_id = "create_table"
    success_task_id = "crawl_success"
    failed_task_id = "crawl_failed"


def create_table(session: Session):
    insp = inspect(session.get_bind())
    if not insp.has_table(Exhibitions.__tablename__):
        Base.metadata.tables[Exhibitions.__tablename__].create(session.get_bind())
        print("資料表建立成功")


@dag(start_date=datetime.today(), tags=['user'])
def create_table_dag():
    start_task = EmptyOperator(task_id=Settings.start_task_id)

    create_table_task = SQLAlchemyOperator(task_id=Settings.create_table_task_id,
                                           python_callable=create_table,
                                           conn_id=Settings.conn_id,
                                           trigger_rule=TriggerRule.ALWAYS)

    end_task = EmptyOperator(task_id=Settings.end_task_id,
                             trigger_rule=TriggerRule.ALWAYS)

    start_task >> create_table_task >> end_task


create_create_table_dag = create_table_dag()

"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
import datetime as dt

from airflow import DAG
from airflow.operators.dea_plugin import PostgresTableRowsCountOperator

default_args = {
    "owner": "podium",
    "depends_on_past": False,
    "start_date": dt.datetime.now() - dt.timedelta(minutes=15),
    "email": ["dan.conger@podium.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    "tutorial-one",
    default_args=default_args,
    schedule_interval=dt.timedelta(days=1)
)

query_table_task = PostgresTableRowsCountOperator(
    pool_conn_id='dea',
    db_name='dea',
    table_name='users',
    task_id='query_the_table',
    dag=dag
)

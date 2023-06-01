from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable

default_args = {
    "owner": "woorek",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
    "retries": 0,
}

#woorek_dag = DAG('woorek-hive-test', default_args=default_args, schedule_interval= '0 5 * * *')
woorek_dag = DAG('woorek-hive-test', default_args=default_args, schedule_interval= '@once')

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "woorek",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 24),
    "retries": 1
}

# define DAG
woorek_dag = DAG('check', default_args=default_args, schedule_interval='0 */6 */1 * *')

# define EmptyOperator tasks


# define BashOperator tasks
check_task = BashOperator(
		task_id='check',
		bash_command="aws s3 ls s3://pd24/_DONE",
		retries=5,
		retry_delay=timedelta(minutes=1),
		dag=woorek_dag
)

echo_task = BashOperator(
		task_id='echo',
		bash_command="echo 'pre task failed'",
		trigger_rule="one_failed",
		dag=woorek_dag
)

# Set task dependencies
check_task >> echo_task

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 0,
}
test_dag = DAG(
    'mario-simple',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)
# Define the BashOperator task
bash_task = BashOperator(
    task_id='bash_task_multiple_execute',
    bash_command="""
        echo "Start";
        echo "completed"
    """,
    dag=test_dag
)
# Set task dependencies
bash_task

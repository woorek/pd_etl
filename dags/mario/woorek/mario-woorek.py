from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 0,
}

test_dag = DAG(
    'mario-woorek',
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

bash_task_2 = BashOperator(
    task_id='bash_2',
    bash_command="echo '2'",
    dag=test_dag
)

start_task = EmptyOperator(
	task_id="start",
	dag=test_dag
)

end_task = EmptyOperator(
	task_id="end",
	dag=test_dag
)

# Set task dependencies
start_task >> [bash_task,bash_task_2] >> end_task

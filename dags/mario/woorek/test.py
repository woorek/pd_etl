from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
	"owner": "woorek",
	"depends_on_past": False,
	"start_date": datetime(2023, 1, 1),
	"retries": 0,
}

##  define DAG
#woorek_dag = DAG('woorek-dag', default_args=default_args, schedule_interval=timedelta(days=1))
woorek_dag = DAG('woorek-weblog-to-s3', default_args=default_args, schedule_interval= '0 5 * * *')

# define Datetime


# define EmptyOperator tasks
start_task = EmptyOperator(task_id="start", dag=woorek_dag)
end_task = EmptyOperator(task_id="end", dag=woorek_dag)

# define BashOperator tasks
bash_task_1 = BashOperator(
		task_id='cp_data',
		bash_command="""
		aws s3 cp s3://pd24/web.log /opt/airflow/dags/data/web.log
		""",
		dag=woorek_dag
)

bash_task_2 = BashOperator(
		task_id='copy_logs', 
		bash_command="""
		cd /opt/airflow/dags/data;
		cat /opt/airflow/dags/data/web.log | grep -v "INFO" | cut -d"," -f 1 | cut -d"=" -f 2 | sort -n | uniq -c > /opt/airflow/dags/data/SUM.log;
		cp /opt/airflow/dags/data/web.log /opt/airflow/dags/data/RAW.log;
		touch /opt/airflow/dags/data/DONE
		""",
		dag=woorek_dag
)

bash_task_3 = BashOperator(
		task_id='to_s3',
		bash_command="""
		today_date=${{ ds_nodash }}
		today_year=$(echo ${{ ds }} | cut -d'-' -f 1)
		today_month=$(echo ${{ ds }} | cut -d'-' -f 2)
		today_day=$(echo ${{ ds }} | cut -d'-' -f 3)
		aws s3 cp /opt/airflow/dags/data/RAW.log s3://pd24/savedata/woorek/$today_day/$today_month/$today_year/RAW.log;
		aws s3 cp /opt/airflow/dags/data/SUM.log s3://pd24/savedata/woorek/{{ ds_nodash }}/SUM.log;
		aws s3 cp /opt/airflow/dags/data/DONE s3://pd24/savedata/woorek/DONE/{{ ds_nodash }}/_DONE
		""",
		dag=woorek_dag
)

'''
bash_task_4 = BashOperator(
		task_id='cleansing',
		bash_command="""
		rm /opt/airflow/dags/data/RAW.log;
		rm /opt/airflow/dags/data/SUM.log;
		rm /opt/airflow/dags/data/DONE
		""",
		dag=woorek_dag
)
'''

# Set task dependencies
start_task >> bash_task_1 >> bash_task_2 >> bash_task_3 >> end_task

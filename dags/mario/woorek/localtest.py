from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable

default_args = {
	"owner": "woorek",
	"depends_on_past": False,
	"start_date": datetime(2023, 5, 27),
	"retries": 0,
}

##  define DAG
#woorek_dag = DAG('woorek-dag', default_args=default_args, schedule_interval=timedelta(days=1))
woorek_dag = DAG('woorek-weblog-to-s3-local', default_args=default_args, schedule_interval= '0 5 * * *')

# define PATH variables
to_server_PATH = Variable.get("to_server_PATH")
to_server_woorek_PATH = Variable.get("to_server_woorek_PATH")
access_token = Variable.get("access_token")

# define EmptyOperator tasks
start_task = EmptyOperator(task_id="start", dag=woorek_dag)
end_task = EmptyOperator(task_id="end", dag=woorek_dag)

# define BashOperator tasks
'''
bash_task_1 = BashOperator(
		task_id='cp_data',
		bash_command="""
		aws s3 cp s3://pd24/web.log /home/wj/airflow/dags/data/web.log
		""",
		dag=woorek_dag
)
'''
bash_task_start_alert = BashOperator(
		task_id='alert_bash_start',
		bash_command="""
		curl -X POST -H "Authorization: Bearer '%s'" -F "message=woorek-weblog-to-s3-local dag task has started" https://notify-api.line.me/api/notify
		""" %access_token,
		dag=woorek_dag
)

bash_task_1 = BashOperator(
		task_id='cp_data',
		bash_command="""
		aws s3 cp $'%s' /home/wj/airflow/dags/data/web.log;
		if [[ $? -eq 1]]; then
		curl -X POST -H "Authorization: Bearer '%s'" -F "message=bash_task_1 failed" https://notify-api.line.me/api/notify
		fi
		""" %(to_server_PATH,access_token),
		dag=woorek_dag
)

bash_task_2 = BashOperator(
		task_id='copy_logs', 
		bash_command="""
		cd /home/wj/airflow/dags/data;
		cat /home/wj/airflow/dags/data/web.log | grep -v "INFO" | cut -d"," -f 1 | cut -d"=" -f 2 | sort -n | uniq -c > /home/wj/airflow/dags/data/SUM.log;
		cp /home/wj/airflow/dags/data/web.log /home/wj/airflow/dags/data/RAW.log;
		touch /home/wj/airflow/dags/data/DONE;
		if [[ $? -eq 1]]; then
		curl -X POST -H "Authorization: Bearer '%s'" -F "message=bash_task_2 failed" https://notify-api.line.me/api/notify
		fi
		""" %access_token,
		dag=woorek_dag
)

bash_task_3 = BashOperator(
		task_id='to_s3',
		bash_command="""
		today_date=${{ ds_nodash }}
		today_year=$(echo ${{ ds }} | cut -d'-' -f 1)
		today_month=$(echo ${{ ds }} | cut -d'-' -f 2)
		today_day=$(echo ${{ ds }} | cut -d'-' -f 3)
		aws s3 cp /home/wj/airflow/dags/data/RAW.log '%s'/$today_day/$today_month/$today_year/RAW.log;
		aws s3 cp /home/wj/airflow/dags/data/SUM.log '%s'/{{ ds_nodash }}/SUM.log;
		aws s3 cp /home/wj/airflow/dags/data/DONE '%s'/DONE/{{ ds_nodash }}/_DONE;
		if [[ $? -eq 1]]; then
		curl -X POST -H "Authorization: Bearer '%s'" -F "message=bash_task_3 failed" https://notify-api.line.me/api/notify
		fi
		""" %(to_server_woorek_PATH,to_server_woorek_PATH,to_server_woorek_PATH,access_token),
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
bash_task_end_alert = BashOperator(
		task_id='end_bash_alert',
		bash_command="""
		curl -X POST -H "Authorization: Bearer '%s'" -F "message=woorek-weblog-to-s3-local dag task has ended" https://notify-api.line.me/api/notify
		""" %access_token,
		dag=woorek_dag
)

# Set task dependencies
start_task >> bash_task_start_alert >> bash_task_1 >> bash_task_2 >> bash_task_3 >> bash_task_end_alert >> end_task

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable

default_args = {
	"owner": "woorek",
	"depends_on_past": False,
	"start_date": datetime(2023, 5, 28),
	"retries": 0,
}

##  define DAG
#woorek_dag = DAG('woorek-dag', default_args=default_args, schedule_interval=timedelta(days=1))
woorek_dag = DAG('woorek-weblog-to-s3-local', default_args=default_args, schedule_interval= '0 5 * * *')
#woorek_dag = DAG('woorek-weblog-to-s3-local', default_args=default_args, schedule_interval= '@once')

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
		curl -X POST -H "Authorization: Bearer {{ var.value.access_token }}" -F "message=\nDAG이름 : {{dag.dag_id}},  Operator이름 : {{task.task_id}} 시작!" https://notify-api.line.me/api/notify
		""",
		dag=woorek_dag
)

bash_task_1 = BashOperator(
		task_id='cp_data',
		bash_command="""
		aws s3 cp {{ var.value.to_server_PATH }} /home/wj/airflow/dags/data/web.log;
		if [[ $? -eq 0 ]]; then
			echo "fine!"
		else
			curl -X POST -H "Authorization: Bearer {{ var.value.access_token }}" -F "message=\nDAG이름 : {{dag.dag_id}},  Operator이름 : {{task.task_id}} 실패!" https://notify-api.line.me/api/notify
		fi
		""",
		dag=woorek_dag
)

bash_task_2 = BashOperator(
		task_id='copy_logs', 
		bash_command="""
		cd /home/wj/airflow/dags/data;
		cat /home/wj/airflow/dags/data/web.log | grep -v "INFO" | cut -d"," -f 1 | cut -d"=" -f 2 | sort -n | uniq -c > /home/wj/airflow/dags/data/SUM.log;
		cp /home/wj/airflow/dags/data/web.log /home/wj/airflow/dags/data/RAW.log;
		touch /home/wj/airflow/dags/data/DONE;
		if [[ $? -eq 0 ]]; then
            echo "fine!"
        else
            curl -X POST -H "Authorization: Bearer {{ var.value.access_token }}" -F "message=\nDAG이름 : {{dag.dag_id}},  Operator이름 : {{task.task_id}} 실패!" https://notify-api.line.me/api/notify
        fi
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
		aws s3 cp /home/wj/airflow/dags/data/RAW.log {{ var.value.to_server_woorek_PATH }}/$today_day/$today_month/$today_year/RAW.log;
		aws s3 cp /home/wj/airflow/dags/data/SUM.log {{ var.value.to_server_woorek_PATH }}/{{ ds_nodash }}/SUM.log;
		aws s3 cp /home/wj/airflow/dags/data/DONE {{ var.value.to_server_woorek_PATH }}/DONE/{{ ds_nodash }}/_DONE;
		if [[ $? -eq 0 ]]; then
            echo "fine!"
        else
            curl -X POST -H "Authorization: Bearer {{ var.value.access_token }}" -F "message=\nDAG이름 : {{dag.dag_id}},  Operator이름 : {{task.task_id}} 실패!" https://notify-api.line.me/api/notify
        fi
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
bash_task_end_alert = BashOperator(
		task_id='end_bash_alert',
		bash_command="""
		curl -X POST -H "Authorization: Bearer {{ var.value.access_token }}" -F "message=\nDAG이름 : {{dag.dag_id}},  Operator이름 : {{task.task_id}} 끝!" https://notify-api.line.me/api/notify
		""",
		dag=woorek_dag
)

# Set task dependencies
start_task >> bash_task_start_alert >> bash_task_1 >> bash_task_2 >> bash_task_3 >> bash_task_end_alert >> end_task

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

bash_task_start_alert = BashOperator(
        task_id='start_bash_alert',
        bash_command="""
        curl -X POST -H "Authorization: Bearer {{ var.value.access_token }}" -F "message= \n날짜: {{ ds_nodash }} \nDAG이름 : {{dag.dag_id}} \nOperator이름 : {{task.task_id}} \n시작!" https://notify-api.line.me/api/notify
        """,
        dag=woorek_dag
)

bash_task_cp_data_from_s3 = BashOperator(
		task_id='cp_data_from_s3',
		bash_command="""
		aws s3 cp {{ var.value.OARLP_from_s3 }} /home/wj/airflow/dags/data/OARLP_2022.csv;
		if [[ $? -eq 0 ]]; then
            echo "fine!"
        else
			curl -X POST -H "Authorization: Bearer {{ var.value.access_token }}" -F "message= \n날짜: {{ ds_nodash }} \nDAG이름 : {{    dag.dag_id}} \nOperator이름 : {{task.task_id}} \n시작!" https://notify-api.line.me/api/notify
		fi
		""",
		dag=woorek_dag
)

bash_task_cp_data_from_local_to_server = BashOperator(
		task_id='data_from_local_to_server',
		bash_command="""
		sshpass -p {{ var.value.server_psswd }} scp /home/wj/airflow/dags/data/OARLP_2022.csv yoda@192.168.90.128:~/data/OARLP_2022.csv;
		if [[ $? -eq 0 ]]; then
            echo "fine!"
        else
            curl -X POST -H "Authorization: Bearer {{ var.value.access_token }}" -F "message= \n날짜: {{ ds_nodash }} \nDAG이름 : {{    dag.dag_id}} \nOperator이름 : {{task.task_id}} \n시작!" https://notify-api.line.me/api/notify
        fi
		""",
		dag=woorek_dag
)

bash_task_cp_data_from_server_to_hdfs = BashOperator(
		task_id='data_from_server_to_hdfs',
		bash_command="""
		sshpass -p {{ var.value.server_psswd }} ssh yoda@192.168.90.128 -o StrictHostKeyChecking=no;
		hdfs dfs -copyFromLocal ~/data/OARLP_2022.csv /user/woorek/hive/OARLP_2022.csv;
		if [[ $? -eq 0 ]]; then
            echo "fine!"
        else
            curl -X POST -H "Authorization: Bearer {{ var.value.access_token }}" -F "message= \n날짜: {{ ds_nodash }} \nDAG이름 : {{    dag.dag_id}} \nOperator이름 : {{task.task_id}} \n시작!" https://notify-api.line.me/api/notify
        fi
		""",
		dag=woorek_dag
)

'''
bash_task_create_table_hql = BashOperator(
		task_id='create_table_by_hql',
		bash_command="""
		sshpass -p {{ var.value.server_psswd }} ssh yoda@192.168.90.128 -o StrictHostKeyChecking=no;
		cd ~/hive/query;
		hive -f OARLP_2022.hql
		if [[ $? -eq 0 ]]; then
            echo "fine!"
        else
            curl -X POST -H "Authorization: Bearer {{ var.value.access_token }}" -F "message= \n날짜: {{ ds_nodash }} \nDAG이름 : {{    dag.dag_id}} \nOperator이름 : {{task.task_id}} \n시작!" https://notify-api.line.me/api/notify
        fi
		""",
		dag=woorek_dag
)
'''

bash_task_end_alert = BashOperator(
        task_id='end_bash_alert',
        bash_command="""
        curl -X POST -H "Authorization: Bearer {{ var.value.access_token }}" -F "message= \n날짜: {{ ds_nodash }} \nDAG이름 : {{dag.dag_id}} \nOperator이름 : {{task.task_id}} \n끝!" https://notify-api.line.me/api/notify
		""",
        dag=woorek_dag
)

# Set task dependencies
#bash_task_start_alert >> bash_task_cp_data_from_s3 >> bash_task_cp_data_from_local_to_server >> bash_task_cp_data_from_server_to_hdfs >> bash_task_create_table_hql >> bash_task_end_alert

bash_task_start_alert >> bash_task_cp_data_from_s3 >> bash_task_cp_data_from_local_to_server >> bash_task_cp_data_from_server_to_hdfs >> bash_task_end_alert















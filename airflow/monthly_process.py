from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import SqlSensor
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from scheduling import readFile, schedule

defaultArgs = {
	"owner": "Nicholas",
	"retries": 1,
	"retry_delay": timedelta(minutes = 5),
	"start_date": datetime(2019, 10, 15),
	"depends_on_past": False,
	"email": ["nickjpena5@gmail.com"],
	"email_on_failure": True
}

dag_id = "monthly_processing"
dag = DAG(dag_id = dag_id,
	 	default_args = defaultArgs,
	 	schedule_interval = timedelta(hours = 7))

sql_sensor = SqlSensor(
		task_id = "check_for_requests",
		conn_id = "insight",
		sql = "SELECT * FROM requests;",
		poke_interval = 30,
		dag = dag
	)

spark_submit_task = PythonOperator(
		task_id = "spark_job",
		python_callable = schedule,
		dag = dag
	)

sql_sensor >> spark_submit_task
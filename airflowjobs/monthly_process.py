from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

defaultArgs = {
	"owner": "ubuntu",
	"retries": 1,
	"retry_delay": timedelta(minutes = 5),
	"start_date": datetime.now(),
	"depends_on_past": False
}

# the user will schedule the spark job in order to count occurrences
#	words: a list of words that they would like to query
def schedule(words):

	template = '''
		spark-submit \
		--master spark://ec2-3-230-62-227.compute-1.amazonaws.com:7077 \
		--conf spark.driver.maxResultSize=6g \
		--executor-memory 4g \
		--driver-memory 6g \
		--executor-cores 6 \
		/home/ubuntu/InsightProject/spark/ingestion.py'''

	dag = DAG(dag_id = 'Monthly_Processing',
	 	default_args = defaultArgs,
	 	schedule_interval = timedelta(hours = 1))

	months = [
		"January", "February", "March", "April", "May", "June",
		"July", "August", "September", "October", "November", "December"
	]

	for word in words:
		for i in range(len(months)):
			bash_command = template + " " + str(i) + " " + word
			task = BashOperator(
					task_id = months[i],
					bash_command = template + " " + str(i) + " " + word,
					dag = dag
			)

def __init__():
	pass

# def main():
# 	words = ["test", "example"]
# 	schedule(words)

# if __name__ == '__main__':
# 	main()
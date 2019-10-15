from flask import Blueprint, Flask, render_template, request
import psycopg2
import pandas as pd
import sys
import subprocess
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta

app = Flask(__name__)

'''
 gather lines from the corresponding file
	filename: string of the name of the file
	return: a list of lines from the file
'''
def readFile(filename):
	try:
		file = open(filename, "r")
	except IOError:
		print("Unable to open file %s" % filename)
		return

	lines = []
	for line in file.readlines():
		lines.append(''.join(line.split()))

	file.close()
	return lines


databaseCredentials = readFile("/home/ubuntu/InsightProject/database.txt")
host = databaseCredentials[1]
database = databaseCredentials[2]
password = databaseCredentials[3]

dictionaryWords = {line : 0 for line in readFile("/home/ubuntu/InsightProject/spark/words_alpha.txt")}
stopWords = {line : 0 for line in readFile("/home/ubuntu/InsightProject/spark/stop_words.txt")}

connection = psycopg2.connect(
	host = host, 
	database = database, 
	user = "postgres",
	password = password
)

# main webpage
@app.route('/')
def default():
	return render_template("main.html")


# user submits their words
@app.route('/submit', methods = ['GET', 'POST'])
def submit():
	words = request.form.getlist('goodWords[]')

	goodWords = []
	badWords = []
	stops = []

	cursor = connection.cursor()

	for word in words:
		if word == "":
			continue

		if word in stopWords:
			stops.append(word)
			continue

		query = '''
		 	SELECT COUNT(*) FROM frequenciesthree WHERE word = '{}'
		'''.format(word.lower())

		cursor.execute(query)
		count = cursor.fetchall()[0][0]

		if count > 0:
			goodWords.append(word)
		else:
			badWords.append(word)


	if len(badWords) > 0 or len(stops) > 0:
		cursor.close()
		return render_template('submit.html', 
			invalidWords = badWords, 
			goodWords = goodWords,
			stopWords = stops)


	wordFrequencies = []
	largestValue = 0
	smallestValue = sys.maxsize

	for word in goodWords:
		query = '''
			SELECT to_char(date, 'Mon') AS month, SUM(frequency) AS frequency, EXTRACT(MONTH FROM DATE) AS number
			FROM frequenciesthree
			WHERE word = '{}'
			GROUP BY month, number
			ORDER BY number;
		'''.format(word.lower())

		cursor.execute(query)

		frequencies = cursor.fetchall()

		freq = []
		for tup in frequencies:
			largestValue = max(largestValue, tup[1])
			smallestValue = min(smallestValue, tup[1])
			freq.append(list(tup))

		wordFrequencies.append(freq)

	cursor.close()

	largestValue = round(largestValue, 1 - len(str(largestValue)))

	return render_template('submit.html', 
		words = words,
		frequencies = wordFrequencies,
		max = largestValue,
		min = smallestValue)


'''
	when a user chooses to schedule a new job with their words
	insert rows into requests table so airflow will detect it
'''
@app.route('/schedule', methods = ['GET', 'POST'])
def airflowScheduler():
	words = request.form.getlist('badWords[]')
	email = request.form.get('email')
	cursor = connection.cursor()
	for word in words:
		query = """INSERT INTO requests VALUES ('{}', '{}');
		""".format(word, email)

		cursor.execute(query)
		connection.commit()


	cursor.close()
	return render_template('schedule.html',
		words = words,
		email = email)

def main():
	app.run(host = "0.0.0.0", port = 80, debug = True)

if __name__ == "__main__":
	main()

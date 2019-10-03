from flask import Blueprint, Flask, render_template, request
import psycopg2
import pandas as pd
import sys

sys.path.insert(1, '../')

from airflowjobs.monthly_process import schedule
from airflowjobs.email import email

server = Flask(__name__)

# gather lines from the corresponding file
#	filename: string of the name of the file
#	return: a list of lines from the file
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


databaseCredentials = readFile("../database.txt")
host = databaseCredentials[1]
database = databaseCredentials[2]
password = databaseCredentials[3]

dictionaryWords = {line : 0 for line in readFile("../spark/words_alpha.txt")}
stopWords = {line : 0 for line in readFile("../spark/stop_words.txt")}

connection = psycopg2.connect(
	host = host, 
	database = database, 
	user = "postgres",
	password = password
)

# main webpage
@server.route('/')
def default():
	return render_template("main.html")


# user submits their words
@server.route('/submit', methods = ['GET', 'POST'])
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
		'''.format(word)

		cursor.execute(query)
		count = cursor.fetchall()[0][0]

		if count > 0:
			goodWords.append(word)
		else:
			badWords.append(word)

		# if word.lower() not in dictionaryWords:
		# 	badWords.append(word)
		# elif word.lower() in dictionaryWords: 
		# 	goodWords.append(word)

	# print(badWords)
	# print(goodWords)
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
			SELECT to_char(to_timestamp(month::text, 'MM'), 'Mon'), \
			 		SUM(f) AS frequency
			FROM (
				SELECT SUM(frequency) AS f, EXTRACT(MONTH FROM date) AS month
				FROM frequenciesthree
				WHERE word = '{}'
				GROUP BY EXTRACT(MONTH FROM date)
			) AS test
			GROUP BY month
			ORDER BY month;
		'''.format(word)

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


# when the user chooses to schedule a new search job with their words
@server.route('/schedule', methods = ['GET', 'POST'])
def airflowScheduler():
	words = request.form.getlist('badWords[]')
	email = request.form.get('email')
	print(email)
	print(words)
	# schedule(words)

	return render_template('schedule.html',
		words = words)


def main():
	server.run(host = "0.0.0.0", port = 8000, debug = True)

if __name__ == "__main__":
	main()

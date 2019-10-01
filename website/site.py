from flask import Blueprint, Flask, render_template, request
import dash
import dash_core_components as dcc 
import dash_html_components as html
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys

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
	words = request.form.getlist('words[]')

	dictionaryWords = {line : 0 for line in readFile("../spark/words_alpha.txt")}
	stopWords = {line : 0 for line in readFile("../spark/stop_words.txt")}
	for word in words:
		if word in stopWords or word not in dictionaryWords:
			return "The following word cannot be used: {}".format(word)

	print(words)

	cursor = connection.cursor()

	wordFrequencies = []
	largestValue = 0
	smallestValue = sys.maxsize

	for word in words:
		query = '''
			SELECT to_char(to_timestamp(month::text, 'MM'), 'Mon'), SUM(f) AS frequency
			FROM (
				SELECT SUM(frequency) AS f, EXTRACT(MONTH FROM date) AS month
				FROM frequenciestwo
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

	# wordFrequencies = [item for sublist in wordFrequencies for item in sublist]
	print(wordFrequencies)

	largestValue = round(largestValue, 1 - len(str(largestValue)))

	return render_template('submit.html', 
		words = words,
		frequencies = wordFrequencies,
		max = largestValue,
		min = smallestValue)


def main():
	server.run(host = "0.0.0.0", port = 8000, debug = True)

if __name__ == "__main__":
	main()

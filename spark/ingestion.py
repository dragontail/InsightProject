import os
import sys
import re
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from collections import OrderedDict, Counter
from itertools import islice
import psycopg2

# gather lines from the corresponding file
#	filename: string of the name of the file
#	return: a list of lines from the file
def readFile(filename: str):
	try:
		file = open(filename, "r")
	except IOError:
		print("Unable to open file %s" % filename)
		return

	lines = []
	for line in file.readlines():
		lines.append(''.join(line.split()))

	return lines


# create a Spark Context with the proper Spark Configurations
#	credentials: a list of the AWS credentials
#	return: a SparkContext object with configurations determined by credentials
def configureSpark(credentials) -> SparkContext:
	accessKey = credentials[0]
	secretKey = credentials[1]
	masterNode = credentials[2]

	region = "s3.us-east-1.amazonaws.com"

	conf = SparkConf().setAppName("Spark App")
	conf.setMaster("spark://" + masterNode + ":7077")

	sc = SparkContext(conf = conf)

	# Hadoop configurations
	sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", accessKey)
	sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", secretKey)
	sc._jsc.hadoopConfiguration().set("fs.s3.endpoint", region)
	sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

	return sc


# helper function to map the RDD 
# 	rddLine: a string of the current webpage's contents
#	searchTerms: a list of words to look for within rddLine
#	return: a list of tuples of the format
#			( 
#				date, 	 -> the date
#				webpage, -> the URL of the website
#				word, 	 -> the corresponding search term
#				count 	 -> the frequency of said word
#			)
def count(searchTerms, rddLine: str):
	occurrences = dict.fromkeys(searchTerms, 0)
	webpage = rddLine.split("\r\n")[0]
	date = rddLine.split("WARC-Date: ")[1].split("\r\n")[0]

	words = re.split(" \n", rddLine)

	# for word in words:
	# 	if word in occurrences and word in searchTerms:
	# 		occurrences[word] += 1
	# 	elif word in searchTerms:
	# 		occurrences[word] = 1

	for s in searchTerms:
		occurrences[s] = rddLine.count(s)

	return [(date, webpage, k, v) for k, v in occurrences.items()]


# version 2.0 of the helper function to map the RDD
#	rddLine: a string of the current webpage's contents
#	dictionary: a dictionary of words in the english dictionary
#	stopWords: a dictionary of stop words that should be ignored
#	return: a list of tuples of the format:
#			( 
#				date, 	 -> the date
#				webpage, -> the URL of the website
#				word, 	 -> a word not found in the dictionary
#				count 	 -> the frequency of said word
#			)
def count2(dictionary, stopWords, rddLine: str):
	webpage = rddLine.split("\r\n")[0].replace("'", "")

	date = rddLine.split("WARC-Date: ")[1].split("\r\n")[0]

	words = re.split("\W+", rddLine)

	occurrences = {}

	for word in words:
		if word.lower() not in stopWords and word.lower() in dictionary:
			if word in occurrences:
				occurrences[word] += 1
			else:
				occurrences[word] = 1

	return [(date, webpage, k, v) for k, v in occurrences.items()]

# once we have our data from the files, store it into PostgreSQL database
#	df: the dataframe containing the frequencies of words
#	return: 1 for success, 0 for failure
def databaseStore(df):
	postgresCredentials = readFile("../database.txt")

	host = postgresCredentials[0]
	database = postgresCredentials[1]
	password = postgresCredentials[2]

	connection = psycopg2.connect(
			host = host,
			database = database,
			user = "postgres",
			password = password
		)

	cursor = connection.cursor()

	for line in df:
		query = """INSERT INTO frequencies (time, webpage, word, frequency)
				VALUES ('{}', '{}', '{}', {});
				""".format(line[0], line[1], line[2], line[3])

		try:
			cursor.execute(query)
		except Exception as e:
			print("There was an error trying to execute query: ", e.message)
			return 0

	connection.commit()
	cursor.close()
	connection.close()

	return 1


# perform the collecting of the text files per month
#	sc: the SparkContext that will be performing the operations
#	monthlyPaths: the URL that leads to the corresponding month's files
#	dictionary: a dictionary of english words
#	stopWords: a list of stop words to ignore
def monthlyReading(sc, monthlyPaths, dictionary, stopWords):
	directory = sc.textFile(monthlyPaths)
	monthlyFiles = directory.collect()

	numberToRead = 1

	sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "WARC-Target-URI: ")

	search = ["cat", "dog", "bird", "fish"]
	columns = ["time", "webpage", "word", "frequency"]

	filePaths = []

	for i in range(numberToRead):
		filePaths.append("s3://commoncrawl/" + monthlyFiles[i])

	files = ",".join(filePaths)
	rdd = sc.textFile(files)

	# pageFrequencies = rdd.map(lambda line: count(search, line))
	pageFrequencies = rdd.map(lambda line: count2(dictionary, stopWords, line))

	frequencies = pageFrequencies.collect()
	frequencies = [item for sublist in frequencies for item in sublist]

	# databaseStore(frequencies)

	# df = sc.parallelize(frequencies).toDF(columns)

	# df.show(truncate = False)
	# print("\n\n\n\n")	


def main():
	credentials = readFile("../credentials.txt")

	sc = configureSpark(credentials)
	spark = SparkSession.builder.appName("test").getOrCreate()

	monthlyPaths = readFile("paths.config")
	dictionaryWords = {line : 0 for line in readFile("words_alpha.txt")}
	stopWords = {line : 0 for line in readFile("stop_words.txt")}

	monthlyReading(sc, monthlyPaths[0], dictionaryWords, stopWords)

	# for i in range(len(monthlyPaths)):
	# 	monthlyReading(sc, monthlyPaths[i], dictionaryWords)

if __name__ == "__main__":
    main()
import os
import sys
import re
import io
import pandas as pd
from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import to_timestamp
from collections import OrderedDict, Counter
from itertools import islice
import psycopg2

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


# create a Spark Context with the proper Spark Configurations
#	credentials: a list of the AWS credentials
#	return: a SparkContext object with configurations determined by credentials
def configureSpark(credentials):
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
	sc._jsc.hadoopConfiguration().set("fs.s3.impl",
	 "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

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
def count(searchTerms, rddLine):
	occurrences = dict.fromkeys(searchTerms, 0)
	webpage = rddLine.split("\r\n")[0]
	if webpage == "WARC/1.0":
		return []

	date = rddLine.split("WARC-Date: ")[1].split("\r\n")[0]

	words = re.split("\W+", rddLine)
	totalWords = len(words)

	for s in searchTerms:
		occurrences[s] = rddLine.count(s)

	return [(date, webpage, k, v, "{:0.3f}".format(v / totalWords * 100)) 
			for k, v in occurrences.items()]

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
def count2(dictionary, stopWords, rddLine):
	webpage = rddLine.split("\r\n")[0].replace("'", "")
	if webpage == "WARC/1.0":
		return []

	date = rddLine.split("WARC-Date: ")[1].split("\r\n")[0]

	words = re.split("\W+", rddLine)
	totalWords = len(words)

	occurrences = {}

	for word in words:
		word = word.lower()
		if word not in stopWords and word in dictionary:
			if word in occurrences:
				occurrences[word] += 1
			else:
				occurrences[word] = 1


	return [(date, webpage[:100], k, v)	for k, v in occurrences.items()]


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

	query = """
			INSERT INTO frequencies (time, webpage, word, frequency, ratio)
			VALUES ('{}', '{}', '{}', {}, {});
			""".format(df.time, df.webpage, df.word, df.frequency, df.ratio)

	try:
		cursor.execute(query)
	except Exception as e:
		print("There was an error trying to execute query: ", e.message)
		return 0

	connection.commit()
	cursor.close()
	connection.close()

	return 1


# version 2.0 of the databaseStore function
def databaseStore2(df):
	postgresCredentials = readFile("../database.txt")
	host = postgresCredentials[0]
	database = postgresCredentials[1]
	password = postgresCredentials[2]

	url = "postgresql://{}/{}".format(host, database)
	properties = {
		"user": "postgres",
		"password": password,
		"driver": "org.postgresql.Driver",
		"stringtype": "unspecified"
	}

	df.write.option("batchSize", 100000).jdbc(
		url = "jdbc:{}".format(url),
		table = "frequencies",
		mode = "append",
		properties = properties)


# perform the collecting of the text files per month
#	sc: the SparkContext that will be performing the operations
#	monthlyPaths: the URL that leads to the corresponding month's files
#	dictionary: a dictionary of english words
#	stopWords: a dictionary of stop words to ignore
def monthlyReading(sc, monthlyPaths, dictionary, stopWords):
	directory = sc.textFile(monthlyPaths)
	monthlyFiles = directory.collect()

	numberToRead = 1

	sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter",
									"WARC-Target-URI: ")

	search = ["cat", "dog", "bird", "fish"]
	columns = ["date", "webpage", "word", "frequency"]

	filePaths = []

	for i in range(numberToRead):
		filePaths.append("s3://commoncrawl/" + monthlyFiles[i])

	files = ",".join(filePaths)
	rdd = sc.textFile(files)

	# pageFrequencies = rdd.flatMap(lambda line: count(search, line))
	pageFrequencies = rdd.flatMap(lambda line: count2(dictionary, stopWords, line))

	df = pageFrequencies.toDF(columns)

	databaseStore2(df)

	df.show(truncate = False)
	print("\n\n\n\n")	


def main():
	if len(sys.argv) == 1:
		print("There needs to be an index for the month.")
		return

	credentials = readFile("../credentials.txt")
	monthIndex = int(sys.argv[1])

	sc = configureSpark(credentials)
	spark = SparkSession.builder.appName("test").getOrCreate()

	monthlyPaths = readFile("paths.config")
	dictionaryWords = {line : 0 for line in readFile("words_alpha.txt")}
	stopWords = {line : 0 for line in readFile("stop_words.txt")}

	monthlyReading(sc, monthlyPaths[monthIndex], dictionaryWords, stopWords)

if __name__ == "__main__":
    main()
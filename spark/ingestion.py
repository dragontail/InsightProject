import os
import sys
import re
import io
import pandas as pd
import random
from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp, count, avg, sum
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
#	rddLine: a string of the current webpage's contents
#	dictionary: a dictionary of words to look for
#	stopWords: a dictionary of stop words that should be ignored
#	return: a list of tuples of the format:
#			( 
#				date, 	 -> the date
#				word, 	 -> the words that are found
#				count 	 -> the frequency of said word
#			)
def count(dictionary, stopWords, rddLine):
	webpage = rddLine.split("\r\n")[0].replace("'", "")
	if webpage == "WARC/1.0":
		return []

	date = rddLine.split("WARC-Date: ")[1].split("T")[0]

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


	return [(date, k, v) for k, v in occurrences.items()]

# once we have our data from the files, store it into PostgreSQL database
#	df: the dataframe containing the frequencies of words
def databaseStore(df):
	postgresCredentials = readFile("../database.txt")
	host = postgresCredentials[0]
	database = postgresCredentials[2]
	password = postgresCredentials[3]

	url = "postgresql://{}/{}".format(host, database)
	properties = {
		"user": "postgres",
		"password": password,
		"driver": "org.postgresql.Driver",
		"stringtype": "unspecified"
	}

	df.write.option("batchSize", 100000).jdbc(
		url = "jdbc:{}".format(url),
		table = "frequenciestwo",
		mode = "append",
		properties = properties)


# perform the collecting of the text files per month
#	sc: the SparkContext that will be performing the operations
#	monthlyPaths: the URL that leads to the corresponding month's files
#	dictionary: a dictionary of words to find
#	stopWords: a dictionary of stop words to ignore
def monthlyReading(sc, monthlyPaths, dictionary, stopWords):
	directory = sc.textFile(monthlyPaths)
	monthlyFiles = directory.collect()

	numberToRead = 250

	sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter",
									"WARC-Target-URI: ")

	columns = ["date", "word", "frequency"]

	paths = random.sample(monthlyFiles, numberToRead)
	filePaths = []

	for i in range(numberToRead):
		filePaths.append("s3://commoncrawl/" + paths[i])

	files = ",".join(filePaths)
	rdd = sc.textFile(files)

	pageFrequencies = rdd.flatMap(lambda line: count(dictionary, stopWords, line))
	pageFrequencies = pageFrequencies.filter(lambda line: len(line) != 0)

	df = pageFrequencies.toDF(columns)
	df = df.groupBy(df.date, df.word).agg(sum(df.frequency)).withColumnRenamed('sum(frequency)', 'frequency')
	
	databaseStore(df)


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

	if len(sys.argv) == 3:
		word = {sys.argv[2] : 0 }
		monthlyReading(sc, monthlyPaths[monthIndex], word, stopWords)
		return
	elif len(sys.argv) == 2:
		monthlyReading(sc, monthlyPaths[monthIndex], dictionaryWords, stopWords)

if __name__ == "__main__":
    main()
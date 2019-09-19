import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from collections import OrderedDict

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

	for s in searchTerms:
		occurrences[s] = rddLine.count(s)

	return [(date, webpage, k, v) for k, v in occurrences.items()]


def main():
	credentials = readFile("../credentials.txt")

	sc = configureSpark(credentials)
	spark = SparkSession.builder.appName("test").getOrCreate()

	monthlyPaths = readFile("paths.config")
	januaryPaths = monthlyPaths[0]

	# Read file to determine paths where the files are located
	directory = sc.textFile(januaryPaths)

	januaryFiles = directory.collect()

	numberToRead = 3

	sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "WARC-Target-URI: ")

	search = ["cat", "dog", "bird", "fish"]
	columns = ["date", "webpage", "word", "frequency"]

	filePaths = []

	for i in range(numberToRead):
		filePaths.append("s3://commoncrawl/" + januaryFiles[i])

	files = ",".join(filePaths)
	
	rdd = sc.textFile(files)

	pageFrequencies = rdd.map(lambda line: count(search, line))

	frequencies = pageFrequencies.collect()
	frequencies = [item for sublist in frequencies for item in sublist]

	df = sc.parallelize(frequencies).toDF(columns)

	df.show(truncate = False)
	print("\n\n\n\n")

if __name__ == "__main__":
    main()
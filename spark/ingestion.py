import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

# gather lines from the corresponding file
def readFile(filename):
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
def configureSpark(credentials):
	bucketName = credentials[0]
	accessKey = credentials[1]
	secretKey = credentials[2]
	masterNode = credentials[3]

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

# helper function to map the RDD based on search terms
def count(searchTerms, rddLine):
	occurrences = dict.fromkeys(searchTerms, 0)

	for s in searchTerms:
		occurrences[s] = rddLine.count(s)

	return occurrences

def main():
	credentials = readFile("../credentials.txt")

	sc = configureSpark(credentials)
	spark = SparkSession.builder.appName("test").getOrCreate()

	monthlyPaths = readFile("paths.config")
	januaryPaths = monthlyPaths[0]

	# Read file to determine paths where the files are located
	directory = sc.textFile(januaryPaths)

	januaryFiles = directory.collect()

	numberToRead = 1

	sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "WARC-TARGET-URI:")

	files = "s3://commoncrawl/" + str(januaryFiles[0])
	# filePaths = []
	# for i in range(numberToRead):
	# 	filePaths.append("s3://commoncrawl/" + januaryFiles[i])
		
	# # gather filepaths together
	# files = ",".join(filePaths)
	
	search = ["test", "example", "attempt", "word"]
	rdd = sc.textFile(files)

	pageFrequencies = rdd.map(lambda line: count(search, line))

	print(pageFrequencies.first())


if __name__ == "__main__":
    main()
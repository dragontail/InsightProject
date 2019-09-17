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


def main():
	credentials = readFile("../credentials.txt")

	sc = configureSpark(credentials)
	spark = SparkSession.builder.appName("test").getOrCreate()

	monthlyPaths = readFile("paths.config")
	januaryPaths = monthlyPaths[0]

	# Read file to determine paths where the files are located
	directory = sc.textFile(januaryPaths)

	januaryFiles = directory.collect()

	print(januaryFiles)

if __name__ == "__main__":
    main()
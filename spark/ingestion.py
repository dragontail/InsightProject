import subprocess
import sys
from pyspark import SparkContext, SparkConf

# gather lines from the corresponding file
def readFile(fileName):
	try:
		file = open(filename, "r")
	except IOError:
		print("Unable to open file paths.config")
		return

	lines = []
	for line in file.readlines():
		lines.append(line)

	return lines


# create a Spark Context with the proper Spark Configurations
def configureSpark(masterNode):
	conf = SparkConf().setAppName("Spark App")
	conf.setMaster("spark://" + masterNode + ":7077")

	sc = SparkContext(conf = conf)

	# Hadoop configurations
	sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", accessKey)
	sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretKeyId", secretKey)
	sc._jsc.hadoopConfiguration().set("fs.s3.endpoint", region)
	sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "WARC-TARGET-URI:")

	return sc


def main():
	credentials = readFile("../credentials.txt")
	bucketName = credentials[0]
	accessKey = credentials[1]
	secretKey = credentials[2]
	masterNode = credentials[3]
	region = "s3.us-east-1-amazonaws.com"

	monthlyPaths = readFile("paths.config")
	januaryPaths = monthlyPaths[0]

	sc = configureSpark(masterNode)

	# Read file to determine paths where the files are located
	januaryFiles = sc.textFile(januaryPaths).collect()

	numberToRead = 100

	filePaths = []
	for i in range(numberToRead):
		filePaths.append("s3://commoncrawl/" + januaryFiles[i])

	for path in filePaths:
		print(path)
		print(sc.textFile(path))

if __name__ == "__main__":
    main()
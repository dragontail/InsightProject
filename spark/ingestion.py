import boto3
import subprocess
import sys
import pyspark

prefix = "https://commoncrawl.s3.amazonaws.com/"
bucket_name = ""
access_key = ""
secret_key = ""

credentials = []

def readCredentials():
	file = open("../credentials.txt", "r")

	for line in file.readlines():
		credentials.append(line)

def main():
	readCredentials()
	sc = pyspark.SparkContext("local", "test app")



if __name__ == "__main__":
    main()
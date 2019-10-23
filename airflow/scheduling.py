import psycopg2
import sys
import subprocess

'''
 gather lines from the corresponding file
	filename: string of the name of the file
	return: a list of lines from the file
'''
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

'''
	once there are requests to fill, remove a request and submit
	the appropriate spark jobs to fulfill it
'''
def schedule():
	databaseCredentials = readFile("/home/ubuntu/InsightProject/database.txt")
	host = databaseCredentials[1]
	database = databaseCredentials[2]
	password = databaseCredentials[3]

	connection = psycopg2.connect(
		host = host, 
		database = database, 
		user = "postgres",
		password = password
	)

	cursor = connection.cursor()
	query = """
		SELECT * FROM requests LIMIT 1;
	"""

	cursor.execute(query)
	results = cursor.fetchone()

	word = results[0]
	email = results[1]

	query = """
		SELECT COUNT(*) FROM frequenciesthree WHERE word = '{}';
		""".format(word.lower())

	cursor.execute(query)
	results = cursor.fetchone()[0]

	if results != 0:
		return 

	query = """
		DELETE FROM requests WHERE word = '{}';
		""".format(word)

	cursor.execute(query)
	connection.commit()

	template = '''
			spark-submit
			--master spark://ec2-3-230-62-227.compute-1.amazonaws.com:7077
			--conf spark.driver.maxResultSize=6g
			--executor-memory 4g
			--driver-memory 6g
			--executor-cores 6
			--jars /home/ubuntu/postgresql-42.2.8.jar
			/home/ubuntu/InsightProject/src/ingestion.py '''

	months = [
		"January", "February", "March", "April", "May", "June",
		"July", "August", "September", "October", "November", "December"
	]

	for i in range(len(months)):
		bashCommand = template + str(i) + " " + word
		process = subprocess.check_call(bashCommand.split())

	cursor.close()
	connection.close()

	send_email(
			to = email,
			subject = "Processing Job Complete",
			html_content = """Your word '{}' has finished processing!
 		 	 Go query it here:
 		 	 http://lensoftruth.me
 		 	 """.format(word),
		)


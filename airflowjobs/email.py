import smtplib
from email.message import EmailMessage
import datetime

def email(words):
	message = EmailMessage()

	message.set_content("Your words have been finished querying!")

	message["Subject"] = "Words have been queried!"
	message["From"] = "service"
	message["To"] = user

	sock = smtplib.SMTP("localhost")
	sock.send_message(message)
	sock.quit()
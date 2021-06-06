import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

ADMIN_EMAIL_ADDRESS = 'kaustubhashravan@gmail.com'

def send_email(email, subject, body):
    message = Mail(
        from_email=ADMIN_EMAIL_ADDRESS,
        to_emails=email,
        subject=subject,
        plain_text_content=body)
    print('Create SendGridAPIClient')
    sg = SendGridAPIClient(os.environ['SENDGRID_API_KEY'])
    sg.send(message)


def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database
    # Update connection string information
    host = "migrationserver4568.postgres.database.azure.com"
    dbname = "techconfdb"
    user = "azureuser@migrationserver4568"
    password = "Rama4568"
    sslmode = "require"

    # Construct connection string
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    print("Connection established")
    print("Notification ID:" + str(notification_id))

    try:
        # TODO: Get notification message and subject from database using the notification_id
        query_notification = '''SELECT subject, message 
                                FROM notification
                                WHERE id = %s;'''

        cur.execute(query_notification, (notification_id,))
        row = cur.fetchone()
        subject = row[0]
        message = row[1]
        while row is not None:
            print(row)
            row = cur.fetchone()

        # TODO: Get attendees email and name
        cur.execute('''SELECT first_name, last_name, email 
                       FROM attendee;''')
        attendees = cur.fetchall()
        print(attendees)
        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in attendees:
            print('{}, {}, {}'.format({'kaustubhashravan@gmail.com'}, {attendee[2]}, {query_notification}))
            email = attendee[2]
            custom_subject = '{}: {}'.format(attendee[0], subject)
            custom_message = 'Dear {}:\n\n{}'.format(attendee[0], message)
            send_email(email, custom_subject, custom_message)

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        print("The Function has executed correctly")
        notification_date = datetime.utcnow()
        status = 'Notified {} attendees'.format(len(attendees))

        query_update = '''UPDATE notification 
                          SET completed_date = %s, status = %s 
                          WHERE id = %s;'''
        cur.execute(query_update, (notification_date, status, notification_id))
        conn.commit()
        #count = cursor.rowcount
        print("Record Updated successfully ")

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        if(conn):
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")

        print("Sentence finally")
import os
import base64
from email.message import EmailMessage

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import mimetypes
from email.message import EmailMessage
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText

def gmail_send_message(attached_file):

    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/gmail.send']

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    try:
        service = build('gmail', 'v1', credentials=creds)
        message = EmailMessage()

        message['To'] = 'test_to@gmail.com'
        message['From'] = 'test_from@gmail.com'
        message['Subject'] = 'Fraudulent Transactions'
         # message text
        message.set_content('List of Fraud Transactions')
        # guessing the MIME type
        type_subtype, encoding = mimetypes.guess_type(attached_file)
        if type_subtype is None or encoding is not None:
            type_subtype = 'application/octet-stream' 
        main_type, sub_type = type_subtype.split('/',1)

        if main_type == 'text':
            with open(attached_file, 'r') as f:
                msg = MIMEText(f.read(), _subtype=sub_type)
        elif main_type == 'image':
            with open(attached_file, 'r') as f:
                msg = MIMEImage(f.read(), _subtype=sub_type)
        elif main_type == 'audio':
            with open(attached_file, 'r') as f:
                msg = MIMEAudio(f.read(), _subtype=sub_type)
        else:
            with open(attached_file, 'r'):
                msg = MIMEBase(main_type, sub_type)
                msg.set_payload(attached_file.read())
        filename = os.path.basename(attached_file)
        msg.add_header('Content-Disposition', 'attachment', filename=filename)
        message.add_attachment(msg,main_type, sub_type)
        # encoded message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()


        create_message = {
            'raw': encoded_message
        }
        # pylint: disable=E1101
        send_message = (service.users().messages().send
                        (userId="test_from@gmail.com", body=create_message).execute())
        print(F'Message Id: {send_message["id"]}')
    except HttpError as error:
        print(F'An error occurred: {error}')
        send_message = None
    return send_message

if __name__ == "__main__":
    
    email_message = "This is a test message"
    with open('fraud_transactions.txt','w') as f:
        f.writelines(email_message)
    with open('fraud_transactions.txt','r') as f:
        print(f.readlines())
    send_msg = gmail_send_message('fraud_transactions.txt')
    print(send_msg)


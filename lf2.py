import json
import boto3
import re
import requests
from boto3.dynamodb.conditions import Key

import smtplib
import email.utils
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch,RequestsHttpConnection

def fetch_id(awsauth,cuisine):
    headers = { "Content-Type": "application/json" }
    url = 'https://search-search-yelp-es-ffavxbcjvz4kji2mpcsnkfflpe.us-east-1.es.amazonaws.com/cuisine/_search?q=category:' + cuisine
    esdata = requests.get(url,auth=awsauth,headers=headers)
    esdjson = esdata.json()
    ids = []
    
    for data in esdjson['hits']['hits']:
        ids.append(data['_source']['id'])
    
    return ids
    
def get_rest_info(r_id,table_name='yelpDatabase'):
    #Creating the DynamoDB Table Resource
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.query(KeyConditionExpression=Key('id').eq(r_id))
    return response
    
def lambda_handler(event, context):
    
    region = 'us-east-1'
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/436554765084/sqs-queue'
    
    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['SentTimestamp'],
        MaxNumberOfMessages=10,
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    if 'Messages' in response:
        msg_dic = {}
        msg_dic['Messages'] = response['Messages']
        for query in msg_dic['Messages']:
            receipt_handle = query['ReceiptHandle']
            response =sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
        for query in msg_dic['Messages']:
            data = query['MessageAttributes']
            cuisine = data['Cuisine']['StringValue']
            number = data['PhoneNo']['StringValue']
            time = data['Time']['StringValue']
            people = data['NoOfPeople']['StringValue']
            date = data['Date']['StringValue']
            location = data['Location']['StringValue']
            email = data['Email']['StringValue']
            receipt_handle = query['ReceiptHandle']
            ids = fetch_id(awsauth,cuisine)
            rest_info = []
            count = 1
            for r_id in ids:
                present = get_rest_info(r_id,table_name='yelpDatabase')['Items'][0]
                if present:
                    rest_info.append(str(count)+'. '+ present['name']+', located at '+present['display_address'][0]+'\n\n') 
                    count+=1
                if count==4:
                    break
            send_email(cuisine,people, date, time,rest_info,email)
    else:
        print('No message in queue!')
    return {
        'statusCode': 200,
        'body': json.dumps('Message sent successfully!')
    }
    
def send_email(cuisine,people,date,time,rest_info,to_email='vishnuthakral@gmail.com'):
    SENDER = 'vvt223@nyu.edu'  
    SENDERNAME = 'Vishnu Thakral'
    
    RECIPIENT  = to_email
    
    USERNAME_SMTP = #"username"
    
    # Replace smtp_password with your Amazon SES SMTP password.
    PASSWORD_SMTP = #"password"
    
    HOST = "email-smtp.us-east-1.amazonaws.com"
    PORT = 587
    
    # The subject line of the email.
    SUBJECT = 'Restaurant Recommendation'
    tag = 'person'
    if int(people) > 1:
        tag = 'people'

    BODY_TEXT = (f"Hello Guest!\n\nWe have the following {cuisine} cuisine restaurant recommendation for {people} {tag} at {time} on {date}\n\n" + ",".join(rest_info) + "\n\nStay Safe and Enjoy your meal!\n\n\nRegards,\nTeam vvt223das968")
    msg = MIMEMultipart('alternative')
    msg['Subject'] = SUBJECT
    msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
    msg['To'] = RECIPIENT
    part1 = MIMEText(BODY_TEXT, 'plain')
    
    msg.attach(part1)
    try:  
        server = smtplib.SMTP(HOST, PORT)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(USERNAME_SMTP, PASSWORD_SMTP)
        server.sendmail(SENDER, RECIPIENT, msg.as_string())
        server.close()
    except Exception as e:
        print ("Error: ", e)
    else:
        print ("Email sent!")

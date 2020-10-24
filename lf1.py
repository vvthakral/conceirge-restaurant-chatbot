import json
from botocore.vendored import requests
import sys
import urllib
import datetime
from datetime import datetime
import boto3
import logging
import os
import time
import re

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def lambda_handler(event, context):
    
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    print(event)
    return dispatch(event)


def dispatch(intentRequest):
    intentName = intentRequest['currentIntent']['name']
    
    # Dispatch to your bot's intent handlers
    if intentName == 'greetingIntent':
        return greetingIntent(intentRequest)
    elif intentName == 'dinningIntent':
        return dinningIntent(intentRequest)
    elif intentName == 'thankYouIntent':
        return thankYouIntent(intentRequest)

    raise Exception('Intent ' + intentName + ' not supported')

def build_response(message):
    
    response = {
        
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": "Fulfilled",
            "message": {
              "contentType": "PlainText",
              "content": message
            },
        }
    }
    
    return response

    
def greetingIntent(intentRequest):
    
    return build_response('Hi there, how can I help you?')
    

def thankYouIntent(intentRequest):
    
    return build_response('Thank you for chosing our service. Happy to serve you again!')
    
    
def elicitSlot(pSessionAttributes, pIntentName, pSlots, pSlotsToElicit, pMessage):
    response =  {
        'sessionAttributes': pSessionAttributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': pIntentName,
            'slots': pSlots,
            'slotToElicit': pSlotsToElicit,
            'message': {'contentType':'PlainText', 'content':pMessage}
        }
    }
    
    return response
 
def delegate(pSessionAttributes, pSlots):
    response = {
        'sessionAttributes': pSessionAttributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': pSlots
        }
    }
    
    return response

def validDate(date):
    
    date = datetime.strptime(date,'%Y-%m-%d')
    curr_date = datetime.strptime(str(datetime.now().date()),'%Y-%m-%d')
    
    user_ts = datetime.timestamp(date)
    curr_ts = datetime.timestamp(curr_date)
    
    if(curr_ts > user_ts):
        return False,'I understand, you may want to relive past memories. Please select relevant date'
        
    if((user_ts - curr_ts) // (60*60*24) > 2):
        return False,'You seems to be futuristic person. Please select date closer to current date'

    return True,'All set'

def validPhoneNumber(num):
    
    num = re.sub('-','',num)
    num_temp = num
    
    if('+' in num):
        num_temp = num[2:]
        
    if(len(num_temp) != 10):
        return False
        
    return num_temp.isdigit()
    

def validTime(date,time):
    
    if(not ':' in time):
        return False,'What time is it?'
    
        
    hr,minutes = time.split(':')
    hr,minutes = int(hr),int(minutes)
    
    if(minutes >= 60):
        hr += minutes // 60
        minutes = minutes % 60
    
    if(hr < 7 or hr > 24):
        return False,'We support business hours morning 8 am - 12 am'
    
    datetime_ = date + " " + time
    curr_date = str(datetime.now().date()) + ' ' + str(datetime.now().hour) + ':' + str(datetime.now().minute)
    user_ts = datetime.timestamp(datetime.strptime(datetime_,'%Y-%m-%d %H:%M'))
    curr_ts = datetime.timestamp(datetime.strptime(curr_date,'%Y-%m-%d %H:%M'))
    #curr_ts -= 5*60*60
    
    if(curr_ts > user_ts):
        return False,'Time has passed, please chose timings post this time'
    
    return True,'All set'

def ValidEmail(email):
    
    pattern = re.compile("^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$")
    if(pattern.match(email)):
        return True
    
    return False
    
def dinningIntent(intentRequest):
    
    sA = intentRequest['sessionAttributes']
    intentName = intentRequest['currentIntent']['name']
    slots =intentRequest['currentIntent']['slots']
    
    location = slots['area']
    cusine = slots['cusine']
    numOfPeople = slots['people']
    date = slots['date']
    phoneNumber = slots['phone_num']
    time = slots['time']
    email = slots['email']
    
    source = intentRequest['invocationSource']
    
    if(source == 'DialogCodeHook'):
        
    
        if(location is not None):
            
            location_supported = ['new york city','nyc','brooklyn','manhattan','new york']
            
            if(location.lower() not in location_supported):
                
                slots['area'] = None
                msg = f'Sorry {location} provided is currently not being supported. Please mention something else.'
                return elicitSlot(sA,intentName,slots,'area',msg)
         
        if(cusine is not None):
            
            cusine_type = ['indian','chinese','japanese','thai','italian','mexican']
            
            if(cusine.lower() not in cusine_type):
                
                slots['cusine'] = None
                types = ' '.join(cusine_type)
                msg = f'Sorry, {cusine} is not supported. You should definetly try {types}'
                
                return elicitSlot(sA,intentName,slots,'cusine',msg)
                
        
        if(numOfPeople is not None):
            
            
            numOfPeople = int(numOfPeople)
            
            if(numOfPeople <= 0 or numOfPeople >= 50):
                
                slots['people'] = None
                msg = f'Sorry,please enter between 0 to 50. I cannot provide suggestions that accomodates {numOfPeople} people'
                
                return elicitSlot(sA,intentName,slots,'people',msg)
        
        if(date is not None):
            
            status,msg = validDate(date)
            
            if(not status):
                
                slots['date'] = None
                return elicitSlot(sA,intentName,slots,'date',msg)
                
                
        if(time is not None):
            
            status, msg = validTime(date,time)
            
            if(not status):
                slots['time'] = None
                return elicitSlot(sA,intentName,slots,'time',msg)
        
        if(phoneNumber is not None):
            
            if(not validPhoneNumber(phoneNumber)):
                
                slots['phone_num'] = None
                msg = 'Don\'t worry, we will not spam you. Please enter valid number to receive suggestions!'
                return elicitSlot(sA,intentName,slots,'phone_num',msg)
        
        if(email is not None):
            
            if (not ValidEmail(email)):
                
                slots['email'] = None
                msg = 'Enter Valid Email'
                return elicitSlot(sA,intentName,slots,'email',msg)
                
        outputSessionAttributes = intentRequest['sessionAttributes'] if intentRequest['sessionAttributes'] is not None else {}
            
        return delegate(outputSessionAttributes, slots)
         
    #Prepare SQS message json
    msgQueueResponse = {
        "loc":location,
        "cuisine":cusine,
        "noofpeople":numOfPeople,
        "date": date,
        "time": time,
        "phoneno": phoneNumber,
        "email":email
    }
        
    messageId = reqResSQS(msgQueueResponse)
    
    
    res = {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText', 
                'content': 'You’re all set. Expect my suggestions shortly! Have a good day.'}
        }
    }  
    return res
    

def reqResSQS(pData):
    
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/436554765084/sqs-queue'
    delaySeconds = 5
    
    messageAttributes = {
        'Location': {
            'DataType': 'String',
            'StringValue': pData['loc']
        },
        'Cuisine': {
            'DataType': 'String',
            'StringValue': pData['cuisine']
        },
        'NoOfPeople': {
            'DataType': 'String',
            'StringValue': pData['noofpeople']
        },
        "Date": {
            'DataType': "String",
            'StringValue': pData['date']
        },
        "Time": {
            'DataType': "String",
            'StringValue': pData['time']
        },
        'PhoneNo': {
            'DataType': 'String',
            'StringValue': pData['phoneno']
        },
        'Email': {
            'DataType': 'String',
            'StringValue': pData['email']
        }
    }
    
    messageBody=('Recommendation for restaurants')
    
    response = sqs.send_message(
        QueueUrl = queue_url,
        DelaySeconds = delaySeconds,
        MessageAttributes = messageAttributes,
        MessageBody = messageBody
        )

    return response['MessageId']

    
import json
import boto3

client = boto3.client('lex-runtime')

def lambda_handler(event, context):
    
    print(event)
    
    response = client.post_text(
        botName='testBot',
        botAlias='$LATEST',
        userId='admin',
        inputText=event["message"])
        
    return {
        "statusCode": 200,
        "headers": {
        'Content-Type':'application/json',    
        'Access-Control-Allow-Origin' : '*'
        },
        "body": response
    }
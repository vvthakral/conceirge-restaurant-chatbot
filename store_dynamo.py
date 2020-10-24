import json
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key

def get_query(table_name,r_id):
    #TABLE_NAME = "yelpDatabase"
    #Creating the DynamoDB Table Resource
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    print('table')
    response = table.query(KeyConditionExpression=Key('id').eq(r_id) & Key('category').eq('japanese'))
    return response

def insert_query(data,dynamodb=None):
    
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('yelpDatabase')
        count = 0
        for i,row in enumerate(data):
            row['id'] = str(row['id'])
            row['name'] = str(row['name'])
            row['coordinates']['latitude'] = Decimal(str(row['coordinates']['latitude']))
            row['coordinates']['longitude'] = Decimal(str(row['coordinates']['longitude']))
            row['rating'] = Decimal(str(row['rating']))
            table.put_item(Item=row)
            count+=1
        print(count)
        return table

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket = 'yelp-data-res'
    key = 'data.json'
    dynamodb = boto3.resource('dynamodb')
    try:
        data = s3.get_object(Bucket=bucket, Key=key)
        json_data = data['Body'].read().decode('utf-8')
        json_content = json.loads(json_data)
        #table = insert_query(json_content['restaurants'])
        print(get_query('yelpDatabase','EvAJBgBdgOt1jx1EufRN7w'))
        #table_name = 'test'
    except Exception as e:
        print(e)
        raise e
        
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
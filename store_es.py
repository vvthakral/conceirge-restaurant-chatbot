import boto3
import re
import requests
import json
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch,RequestsHttpConnection


def pushData(es,awsauth):
    
    s3 = boto3.client('s3')
    
    bucket = 'yelp-data-res'
    key = 'data.json'
    data = s3.get_object(Bucket=bucket, Key=key)
    json_data = data['Body'].read().decode('utf-8')
    json_content = json.loads(json_data)
    
    print(len(json_content['restaurants']))
    
    for idx,data in enumerate(json_content['restaurants'][0:200]):
        document = {"id": data["id"], "category":data["category"]}
        es.index(index='cuisine',doc_type = '_doc',body=document)
        
    return idx
    
        
def fetchData(awsauth):
    
    cuisine = 'chinese'
    headers = { "Content-Type": "application/json" }
    url = 'https://search-search-yelp-es-ffavxbcjvz4kji2mpcsnkfflpe.us-east-1.es.amazonaws.com/cuisine/_search?q=category:' + cuisine
    esdata = requests.get(url,auth=awsauth,headers=headers)
    esdjson = esdata.json()
    
    ids = []
    
    for data in esdjson['hits']['hits']:
        ids.append(data['_source']['id'])
        
    return ids
    
def handler(event, context):
    
    region = 'us-east-1'
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    host = 'search-search-yelp-es-ffavxbcjvz4kji2mpcsnkfflpe.us-east-1.es.amazonaws.com'
    
    
    es = Elasticsearch(
        hosts = [{'host':host,'port':443}],
        http_auth = awsauth,
        use_ssl=True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
        )
    
    
    #Alread pushed data don't execute again
    #print(pushData(es,awsauth))
    
    #Bydefault will fetch 10 records
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

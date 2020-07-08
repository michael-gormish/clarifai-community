import boto3
import json
import os
import urllib.parse
import urllib3

#Note print statements will be logged to CloudWatch
print('Loading function')
s3 = boto3.client('s3')

def cl_predict(model_id, api_key, image_bytes=None, image_url=None):
    """call clarifai predict for the given model and return a list of concepts and scores"""
    #format the request (note it is easier to use Clarifai's grpc library but that is harder with lambda
    api_key = "Key {}".format(api_key)
    headers = {"Authorization":api_key, "Content-Type":"application/json"}
    url = "https://api.clarifai.com/v2/models/{}/outputs".format(model_id)
    oneinput = {"data": { "image": { "url": image_url}}}
    data = {"inputs": [oneinput]}
    encoded_data = json.dumps(data).encode('utf-8')

    http = urllib3.PoolManager()
    r = http.request('POST',  url, headers=headers, body=encoded_data)

    print("predict http status: " + r.status)
    #print(r.headers)                                                                                                                        response = json.loads(r.data.decode('utf-8'))

    #unpack the list
    clconcepts = response['outputs'][0]['data']['concepts']
    #print(clconcepts)                                                                                                                    
    concepts = {}
    concepts = {concept['name']:concept['value'] for concept in clconcepts}

    return concepts


def lambda_handler(event, context):
    """function called by AWS lambda, we assume will be called because of upload, must be called lambda_handler"""
    print("Received event: " + json.dumps(event, indent=2))

    api_key = os.environ['CLARIFAI_API_KEY'] #key should be set in lambda environment
    bucket_url = "http://stormishclarifai.s3-website.us-east-2.amazonaws.com/" # REPLACE -- this could be computed from region
    model_id = 'aaa03c23b3724a16a56b629203edc62c' # general model could set from environment as well

    #Get info about the uploaded object
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    #convert to a URL for the bucket to pass to Clarifai
    image_url = bucket_url+key
    concepts = cl_predict(model_id, api_key, None, image_url=image_url)

    writekey = key[:-4] + ".json"
    response = s3.put_object(Bucket=bucket, Key=writekey, Body=json.dumps(concepts).encode('utf-8'))

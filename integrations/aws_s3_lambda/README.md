
This directory contains code to integrate AWS S3 and Lambda with Clarifai.


## Prerequisites:
1) AWS account with sufficient premissions for S3 and Lambda.
2) Clarifai Account with a model

## Step 1: S3 folder

Create a folder in the S3 bucket.
Make sure the bucket has http access and read permission. Can be by
making it a web server.

## Step 2: Create Lambda function
Copy the python file from repo
https://us-east-2.console.aws.amazon.com/lambda/home?region=us-east-2#/functions
- Put the URL for the bucket in the lambda function
- Put the Clarifai API Key in as an Environmental variable: CLARIFAI_API_KEY


## Step 3: Trigger Lambda Function on File Upload
Make a trigger for the lambda function that on file upload.
- suffix restriction to .jpg is more restrictive than Clarifai can handle, but without that,
there would be a second call when we create the json file.

## Step 4: Upload an image
Upload an image -- make sure it is publicly readable.

##Notes:
Debugging is available at CloudWatch. https://us-east-2.console.aws.amazon.com/cloudwatch/
This function didn't use Clarifai-grpc because it has to be installed in Lambda.

## Improvements:

Functions can upload to an app instead of calling predict, this will allow items to be viewed in explorer.
Labels could be obtained from S3 metadata so that annotations could be uploaded.



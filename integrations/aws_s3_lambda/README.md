
This directory contains code to integrate AWS S3 and Lambda with Clarifai.


Prerequists:
1) AWS account with sufficient premissions for S3 and Lambda.
2) Clarifai Account with a model


Step 1:

Create a folder in the S3 bucket. Make sure the bucket has http access and read permission.

Step 2:
Create a lambda function (Copy from repo)
- Put the API Key in as an Environmental variable
- Put the url for the bucket in the lambda function

Step 3:
Make a trigger for the lambda function that on file upload.
- suffix restriction to .jpg is more restrictive than Clarifai, but without that,
there would be a second call when we create the json file.

Step 4:
Upload an image (make sure it is publicly readable).

Notes:
Debugging is available at CloudWatch.
This function didn't use Clarifai-grpc because it has to be installed in Lambda.

Improvements:

Functions can upload to an app instead of calling predict, this will allow items to be viewed in explorer.
Labels could be obtained from S3 metadata so that annotations could be uploaded.



This directory contains code using the clarifai_grpc python client for uploading images to an application.


## Prerequisites:
1) Clarifai Python gRPC Client (Downloadable here: https://pypi.org/project/clarifai-grpc/)
2) Clarifai Account with an application


## image_url_w_bboxes_upload.py

#### Expected inputs:
- An API key with the minimum scopes of PostInputs, PostConcepts, PostAnnotations, and ListWorkflows
- A .csv file with the following column(s):
  - "url" - Required. One publicly accessible image url per line
  - "left_col" - Required. Float value corresponding to the left edge of the bounding box.
  - "top_row" - Required. Float value corresponding to the top edge of the bounding box.
  - "right_col" - Required. Float value corresponding to the right edge of the bounding box.
  - "bottom_row" - Required. Float value corresponding to the bottom edge of the bounding box.
  - "pos_concepts" - Optional. A pipe "|" separated string of positive concept(s), ex: "cat|dog|fish"
  - "neg_concepts" - Optional. A pipe "|" separated string of negative concept(s), ex: "axolotl"
  - "metadata" - Optional. A JSON string representation of a python dictionary, ex: "{""license"":""CC0""}"

  Note1:
  This script assumes that the csv has been normalized so that each line has one set of region info per line.
  For images with multiple bounding boxes, list the image url multiple times, once per bounding box. Example below.

  Note2:
  Bounding box coordinates are specified as float values between 0 and 1, relative to the image size;
  the top-left coordinate of the image is (0.0, 0.0), and the bottom-right of the image is (1.0, 1.0).
  Note that if the image is rescaled (by the same amount in x and y), then box coordinates remain the same.

#### Example CSV:
url,top_row,left_col,bottom_row,right_col,pos_concepts,neg_concepts,metadata
https://samples.clarifai.com/metro-north.jpg,0.4883,0.64131,0.6732,0.70772,person,,"{""source"":""clarifai""}"
https://samples.clarifai.com/face-det.jpg,0.14118,0.4287,0.99406,0.7967,person,,{}
https://samples.clarifai.com/face-det.jpg,0.33939,0.6687,0.99707,0.9947,person,,{}
https://samples.clarifai.com/face-det.jpg,0.20425,0.0007,0.99707,0.3307,person,,{}
https://samples.clarifai.com/food.jpg,,,,,,,

#### Example usage:
python image_url_w_bboxes_upload.py --api_key YOUR_API_KEY --csv_file /PATH/TO/YOUR_CSV_FILE.CSV

#### Comments:
tqdm is being used to display progress bars. to avoid it, remove the import and "tqdm()" wrapped around any for loops


## image_url_upload.py
A script for uploading a batch of image urls with image level concepts using the clarifai-grpc client.

#### Expected inputs:
- An API key with the minimum scopes of PostInputs, PostConcepts, and PostAnnotations
- A .csv file with the following column(s):
  - "url" - Required. One publicly accessible image url per line
  - "pos_concepts" - Optional. A pipe "|" separated string of positive concept(s), ex: "cat|dog|fish"
  - "neg_concepts" - Optional. A pipe "|" separated string of negative concept(s), ex: "axolotl"
  - "metadata" - Optional. A JSON string representation of a python dictionary, ex: "{""license"":""CC0""}"

#### Example CSV:
url,pos_concepts,neg_concepts,metadata
https://samples.clarifai.com/metro-north.jpg,"subway station",food|people,"{""source"":""clarifai""}"
https://samples.clarifai.com/food.jpg,,"subway station|people",{}
https://samples.clarifai.com/face-det.jpg,,,

#### Example usage:
python image_url_upload.py --api_key YOUR_API_KEY --csv_file /path/to/my_csv_file.csv

#### Comments:
tqdm is being used to display progress bars. to avoid it, remove the import and "tqdm()" wrapped around any for loops

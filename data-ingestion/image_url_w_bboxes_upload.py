"""
A script for uploading image urls plus bounding box / region data using the clarifai-grpc client.

## Expected inputs:
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

## Example CSV:
url,top_row,left_col,bottom_row,right_col,pos_concepts,neg_concepts,metadata
https://samples.clarifai.com/metro-north.jpg,0.4883,0.64131,0.6732,0.70772,person,,"{""source"":""clarifai""}"
https://samples.clarifai.com/face-det.jpg,0.14118,0.4287,0.99406,0.7967,person,,{}
https://samples.clarifai.com/face-det.jpg,0.33939,0.6687,0.99707,0.9947,person,,{}
https://samples.clarifai.com/face-det.jpg,0.20425,0.0007,0.99707,0.3307,person,,{}
https://samples.clarifai.com/food.jpg,,,,,,,

## Example usage:
python image_url_w_bboxes_upload.py --api_key YOUR_API_KEY --csv_file /PATH/TO/YOUR_CSV_FILE.CSV

## Comments:
tqdm is being used to display progress bars. to avoid it, remove the import and "tqdm()" wrapped around any for loops
"""

# imports
import argparse
import json
import pandas as pd
import time
from tqdm import tqdm

from google.protobuf.struct_pb2 import Struct

from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import service_pb2_grpc, service_pb2, resources_pb2
from clarifai_grpc.grpc.api.status import status_code_pb2

from concurrent.futures import ThreadPoolExecutor, as_completed


def chunker(seq, size):
  """
  :param seq: any sequence structure
  :param size: the size of the chunk to return when looping through the generator
  :return: a generator which produces smaller chunks of the seq based on the size parameter
  """
  return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def initial_csv_wrangling(csv_file):
  """
  Takes a formatted csv, parses concepts into lists, metadata into a dict, then returns a dataframe.
  :param csv_file: input csv with a required "url" column and optional "pos_concepts", "neg_concepts", and "metadata"
  :return: a Pandas DataFrame
  """
  df = pd.read_csv(csv_file)
  columns = list(df.columns)

  # check that "url" column exists (required)
  if 'url' not in columns:
    raise Exception('Input csv file requires a "url" column, which does not seem to exist. Exiting.')

  # check that coordinate columns exist
  if all(col in columns for col in ['left_col', 'top_row', 'right_col', 'bottom_row']) is False:
    raise Exception(
      'The following columns are required: "left_col", "top_row", "right_col", and "bottom_row". Exiting.')
  else:
    # check that values are between between 0-1 or that value is empty (ie. upload specific image without regions)
    if not (df['left_col'].between(0,1,inclusive=True).any() | df['left_col'].isnull()).any():
      raise Exception('Note: Not all specified values in the "left_col" column are between 0 and 1. Exiting')
    if not (df['top_row'].between(0,1,inclusive=True).any() | df['top_row'].isnull()).any():
      raise Exception('Note: Not all specified values in the "top_row" column are between 0 and 1. Exiting')
    if not (df['right_col'].between(0,1,inclusive=True).any() | df['right_col'].isnull()).any():
      raise Exception('Note: Not all specified values in the "right_col" column are between 0 and 1. Exiting')
    if not (df['bottom_row'].between(0,1,inclusive=True).any() | df['bottom_row'].isnull()).any():
      raise Exception('Note: Not all specified values in the "bottom_row" column are between 0 and 1. Exiting')

    # check that left_col is less than right_col and top_row is less than bottom_row, ignore empty values
    if not (df['left_col'].dropna() < df['right_col'].dropna()).any():
      raise Exception('Note: Not all specified values in "left_col" are less than the specified values in "right_col".')
    if not (df['top_row'].dropna() < df['bottom_row'].dropna()).any():
      raise Exception('Note: Not all specified values in "top_row" are less than the specified values in "bottom_row".')

  # check if "pos_concepts" column exists and parse accordingly (not required)
  if 'pos_concepts' in columns:
    print('Found "pos_concepts" column. Values will be split by pipe/vertical bar "|" into a python list.')
    df['pos_concepts'] = df['pos_concepts'].fillna('').map(lambda x: list(set(x.split('|'))))

  # check if "neg_concepts" column exists and parse accordingly (not required)
  if "neg_concepts" in columns:
    print('Found "neg_concepts" column. Values will be split by pipe/vertical bar "|" into a python list.')
    df['neg_concepts'] = df['neg_concepts'].fillna('').map(lambda x: list(set(x.split('|'))))

  # check if "metadata" column exists and load accordingly (not required)
  if "metadata" in columns:
    print('Found "metadata" column. Attempting to ingest.')
    try:
      df['metadata'] = df['metadata'].fillna('{}').map(json.loads)
    except:
      raise Exception('Value in "metadata" column does not seem to be a properly JSON formatted str.')

  return df


def process_and_upload_chunk(stub, auth_metadata, chunk, embed_model_version_id, batch_size):
  """
  :param stub: the grpc client stub
  :param auth_metadata: authorization metadata
  :param chunk: a subset of the dataframe created from the csv file
  :param embed_model_version_id: version id of the embed model, used to assign embeddings to specific regions
  :param batch_size: batch sizes to work with when making batchable API calls
  :return: status codes for the API calls
  """

  def process_input_proto(df_row):
    """
    :param df_row: an individual dataframe row
    :return: an Input proto
    """

    metadata = Struct()

    if 'metadata' in list(df_row.keys()):
      metadata.update(df_row['metadata'])

    # create Input proto using the url + any concepts and metadata
    input_proto = resources_pb2.Input(
      data=resources_pb2.Data(
        image=resources_pb2.Image(
          url=df_row['url']
        ),
        metadata=metadata))

    return input_proto

  inputs = []

  # iterate through lines and convert into Input protos
  # here we'll be filtering out the bbox related columns, then uploading unique urls and any metadata (if applicable)
  related_input_columns = [col for col in chunk.columns if col in ['url','metadata']]

  # Author's Note: "this probably isn't the cleanest way to handle this, but since Pandas can't handle dictionaries
  # when dropping duplicate rows, we're working around that by converting it back into a JSON string, deduping,
  # then converting it back into a dictionary again."
  if 'metadata' in chunk.columns:
    chunk_inputs = chunk[related_input_columns]
    chunk_inputs['metadata'] = chunk_inputs['metadata'].map(json.dumps)
    chunk_inputs = chunk_inputs.drop_duplicates()
    chunk_inputs['metadata'] = chunk_inputs['metadata'].map(json.loads)
  else:
    chunk_inputs = chunk[related_input_columns].drop_duplicates()

  for i, each in chunk_inputs.iterrows():
    single_input = process_input_proto(df_row=each)
    inputs.append(single_input)

  # build PostInputsRequest
  request = service_pb2.PostInputsRequest(inputs=inputs)

  # upload the batch of input protos using the PostInputs call
  response = stub.PostInputs(request, metadata=auth_metadata)

  # get input_ids and store as dict. these will be used to populate input_ids in the post annotations requests
  url2id_dict = {}
  for inp in response.inputs:
    url2id_dict[inp.data.image.url] = inp.id

  # parse annotations
  def process_annotation_proto(df_row):
    """
    :param df_row: an individual dataframe row
    :return: an annotation proto
    """
    concept_protos = []

    # parse pos_concepts
    if 'pos_concepts' in list(df_row.keys()):
      for concept in df_row['pos_concepts']:
        if concept != '':
          concept_proto = resources_pb2.Concept(
            id=concept,
            name=concept,
            value=1)
          concept_protos.append(concept_proto)

    # parse neg_concepts
    if 'neg_concepts' in list(df_row.keys()):
      for concept in df_row['neg_concepts']:
        if concept != '':
          concept_proto = resources_pb2.Concept(
            id=concept,
            name=concept,
            value=0)
          concept_protos.append(concept_proto)

    annotation_proto = resources_pb2.Annotation(
      input_id=url2id_dict[df_row['url']],
      data=resources_pb2.Data(
        regions=[
          resources_pb2.Region(
            region_info=resources_pb2.RegionInfo(
              bounding_box=resources_pb2.BoundingBox(
                left_col=df_row['left_col'],
                top_row=df_row['top_row'],
                right_col=df_row['right_col'],
                bottom_row=df_row['bottom_row'],
              )
            ),
            data=resources_pb2.Data(
              concepts=concept_protos
            )
          ),
        ]
      ),
      embed_model_version_id=embed_model_version_id
    )

    return annotation_proto

  annotations = []

  # iterate through lines and convert into Annotation protos
  for i, each in chunk.iterrows():
    single_annotation = process_annotation_proto(df_row=each)
    annotations.append(single_annotation)

  # check input urls' upload statuses
  # this check is made since bbox annotations can only be added to inputs that have finished processing
  inps_done = False
  ids = [x.id for x in response.inputs]

  while not inps_done:
    time.sleep(1)
    listinp_res = stub.ListInputs(service_pb2.ListInputsRequest(ids=ids), metadata=auth_metadata)
    inps_status = [x.status.code for x in listinp_res.inputs]
    inps_done = all(
      [(x == status_code_pb2.INPUT_DOWNLOAD_SUCCESS or x == status_code_pb2.INPUT_DOWNLOAD_FAILED) for x in
       inps_status])

  # upload annotations
  # Note: currently (Sept 2020) PostAnnotations has a hard limit of 20
  annot_batch_size = batch_size
  if annot_batch_size >= 20:
    annot_batch_size = 20

  response_annots = []

  for chunk_annots in chunker(annotations, annot_batch_size):
    request = service_pb2.PostAnnotationsRequest(annotations=chunk_annots)
    response_annot = stub.PostAnnotations(request, metadata=auth_metadata)
    response_annots.append(response_annot)

  return response.status.code, response_annots


def main():
  # the parser lines below are used to take in user arguments through the command line
  parser = argparse.ArgumentParser(
    description='Given an API key and a properly formatted csv file, upload image urls to an application.')
  parser.add_argument(
    '--api_key',
    required=True,
    help='An application\'s API key with PostInput scopes',
    type=str)
  parser.add_argument(
    '--csv_file',
    required=True,
    help='Full pathname to csv file with a "url", "pos_concepts", "neg_concepts", and "metadata" header columns',
    type=str)
  parser.add_argument(
    '--batch_size',
    default=16,
    help='The size of the batches to process and upload at once. Batch size 16 is recommended. \
    This can be scaled up to a max of 128, although that will not necessarily make the uploads go quicker.',
    type=int)
  args = parser.parse_args()

  # construct a client stub using the grpc_channel
  channel = ClarifaiChannel.get_json_channel()
  stub = service_pb2_grpc.V2Stub(channel)

  auth_metadata = (('authorization', f'Key {args.api_key}'),)

  # read in csv file as a pandas dataframe and do some initial wrangling
  # specifically: checks that a "url" column exists and splits the pipe-separated list of concepts into python lists
  dataframe = initial_csv_wrangling(args.csv_file)

  # get embed_model_version_id
  # this assumes that we'll use the first workflow listed (usually the only one / the base workflow)
  app_workflows = stub.ListWorkflows(service_pb2.ListWorkflowsRequest(), metadata=auth_metadata)

  try:
    embed_model_version_id = [x.model.model_version.id for x in app_workflows.workflows[0].nodes if 'embed' in x.id][0]
  except:
    print('Note: a default embed_model_version_id was not found in your workflows. Attempting to continue without one.')
    embed_model_version_id = None

  # group the data together by the 'url' column, so that image urls only get uploaded once
  dfg = dataframe.groupby('url')

  # iterate through the dataframe in chunks, process the items, and upload them in batches
  threads = []
  expected_batch_nums = len(dataframe) // args.batch_size + 1

  with ThreadPoolExecutor(max_workers=10) as executor:
    for chunk_urls in chunker(list(dfg.groups), args.batch_size):
      chunk_df = dataframe[dataframe['url'].isin(chunk_urls)]
      threads.append(
        executor.submit(process_and_upload_chunk, stub, auth_metadata, chunk_df, embed_model_version_id, args.batch_size))

      for task in tqdm(as_completed(threads), total=expected_batch_nums):
        if task.result()[0] == status_code_pb2.SUCCESS:
          continue
        elif task.result()[0] == status_code_pb2.CONN_INSUFFICIENT_SCOPES:
          raise Exception('The API key provided does not have enough scopes to post inputs/annotations/concepts.')
        elif task.result()[
          0] == status_code_pb2.CONN_KEY_INVALID or task.result() == status_code_pb2.CONN_KEY_NOT_FOUND:
          raise Exception('The API key provided is either invalid or does not exist.')
        else:
          # generic catch all.
          print(f'Received status code: {task.result()[0]}. Attempting to continue. \
          Visit https://docs.clarifai.com/api-guide/api-overview/status-codes to learn more.')
          continue


if __name__ == '__main__':
  main()

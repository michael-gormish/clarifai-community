"""
A script for uploading a batch of image urls with image level concepts using the clarifai-grpc client.

## Expected inputs:
- An API key with the minimum scopes of PostInputs, PostConcepts, and PostAnnotations
- A .csv file with the following column(s):
  - "url" - Required. One publicly accessible image url per line
  - "pos_concepts" - Optional. A pipe "|" separated string of positive concept(s), ex: "cat|dog|fish"
  - "neg_concepts" - Optional. A pipe "|" separated string of negative concept(s), ex: "axolotl"
  - "metadata" - Optional. A JSON string representation of a python dictionary, ex: "{""license"":""CC0""}"

## Example CSV:
url,pos_concepts,neg_concepts,metadata
https://samples.clarifai.com/metro-north.jpg,"subway station",food|people,"{""source"":""clarifai""}"
https://samples.clarifai.com/food.jpg,,"subway station|people",{}
https://samples.clarifai.com/face-det.jpg,,,

## Example usage:
python image_url_upload.py --api_key YOUR_API_KEY --csv_file /path/to/my_csv_file.csv

## Comments:
tqdm is being used to display progress bars. to avoid it, remove the import and "tqdm()" wrapped around any for loops
"""

# imports
import argparse
import json
import pandas as pd
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
  Takes in a formatted csv, parses concepts into lists, metadata into a dict, then returns a dataframe.
  :param csv_file: input csv with a required "url" column and optional "pos_concepts", "neg_concepts", and "metadata"
  :return: a Pandas DataFrame
  """
  df = pd.read_csv(csv_file)
  df = df.fillna('')
  columns = list(df.columns)

  # check that "url" column exists (required)
  if 'url' not in columns:
    raise Exception('Input csv file requires a "url" column, which does not seem to exist. Exiting.')

  # check if "pos_concepts" column exists and parse accordingly (not required)
  if 'pos_concepts' in columns:
    print('Found "pos_concepts" column. Values will be split by pipe/vertical bar "|" into a python list.')
    df['pos_concepts'] = df['pos_concepts'].map(lambda x: list(set(x.split('|'))))

  # check if "neg_concepts" column exists and parse accordingly (not required)
  if "neg_concepts" in columns:
    print('Found "neg_concepts" column. Values will be split by pipe/vertical bar "|" into a python list.')
    df['neg_concepts'] = df['neg_concepts'].map(lambda x: list(set(x.split('|'))))

  # check if "metadata" column exists and load accordingly (not required)
  if "metadata" in columns:
    print('Found "metadata" column. Attempting to ingest.')
    try:
      df['metadata'] = df['metadata'].replace('','{}').map(json.loads)
    except:
      raise Exception('Value in "metadata" column does not seem to be a properly JSON formatted str.')

  return df


def process_and_upload_chunk(stub, api_key, chunk, allow_duplicate_url):
  """
  :param stub: the grpc client stub
  :param api_key: the API key for the app to upload inputs into
  :param chunk: a subset of the dataframe created from the csv file
  :param allow_duplicate_url: boolean - True/False
  :return:
  """
  def process_one_line(df_row, allow_duplicate_url):
    """
    :param df_row: an individual dataframe row
    :param allow_duplicate_url: boolean - True/False
    :return: an Input proto
    """
    concepts = []
    metadata = Struct()

    if 'metadata' in list(df_row.keys()):
      metadata.update(df_row['metadata'])

    # parse pos_concepts
    if 'pos_concepts' in list(df_row.keys()):
      for concept in df_row['pos_concepts']:
        if concept != '':
          concept_proto = resources_pb2.Concept(
            id=concept,
            name=concept,
            value=1)
          concepts.append(concept_proto)

    # parse neg_concepts
    if 'neg_concepts' in list(df_row.keys()):
      for concept in df_row['neg_concepts']:
        if concept != '':
          concept_proto = resources_pb2.Concept(
            id=concept,
            name=concept,
            value=0)
          concepts.append(concept_proto)

    # create Input proto using the url + any concepts and metadata
    input_proto = resources_pb2.Input(
      id = str(df_row['input_id']),
      data=resources_pb2.Data(
        image=resources_pb2.Image(
          url=df_row['url'],
          allow_duplicate_url=allow_duplicate_url
        ),
        concepts=concepts,
        metadata=metadata))

    return input_proto

  inputs = []

  # iterate through lines and convert into Input protos
  for i, each in chunk.iterrows():
    single_input = process_one_line(df_row=each, allow_duplicate_url=allow_duplicate_url)
    inputs.append(single_input)

  # build PostInputsRequest
  request = service_pb2.PostInputsRequest(inputs=inputs)
  auth_metadata = (('authorization', f'Key {api_key}'),)

  # upload the batch of input protos using the PostInputs call
  response = stub.PostInputs(request, metadata=auth_metadata)
  return response.status.code


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
    default=32,
    help='The size of the batches to process and upload at once. Batch size 32 is recommended. \
    This can be scaled up to a max of 128, although that will not necessarily make the uploads go quicker.',
    type=int)
  parser.add_argument(
    '--allow_duplicate_url',
    default=True,
    help='If True, any duplicate urls found will be uploaded as a separate input. \
    If False, only the first encountered url (and any additional concepts/metadata) will be uploaded.',
    type=bool
  )
  args = parser.parse_args()

  # construct a client stub using the grpc_channel
  channel = ClarifaiChannel.get_json_channel()
  stub = service_pb2_grpc.V2Stub(channel)

  # read in csv file as a pandas dataframe and do some initial wrangling
  # specifically: checks that a "url" column exists and splits the pipe-separated list of concepts into python lists
  dataframe = initial_csv_wrangling(args.csv_file)

  # iterate through the dataframe in chunks, process the items, and upload them in batches
  # multi-threaded version
  threads = []
  expected_batch_nums = len(dataframe)//args.batch_size + 1

  with ThreadPoolExecutor(max_workers=10) as executor:
    for chunk in chunker(dataframe, args.batch_size):
      threads.append(executor.submit(process_and_upload_chunk, stub, args.api_key, chunk, args.allow_duplicate_url))

    for task in tqdm(as_completed(threads), total=expected_batch_nums):
      if task.result() == status_code_pb2.SUCCESS:
        continue
      elif task.result() == status_code_pb2.CONN_INSUFFICIENT_SCOPES:
        raise Exception('The API key provided does not have enough scopes to post inputs/annotations/concepts.')
      elif task.result() == status_code_pb2.CONN_KEY_INVALID or task.result() == status_code_pb2.CONN_KEY_NOT_FOUND:
        raise Exception('The API key provided is either invalid or does not exist.')
      else:
        # generic catch all.
        print(f'Received status code: {task.result()}. Attempting to continue. \
        Visit https://docs.clarifai.com/api-guide/api-overview/status-codes to learn more.')
        continue


if __name__ == '__main__':
  main()

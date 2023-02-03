import sys
import argparse
from pydomo import Domo
from pydomo.streams import CreateStreamRequest,UpdateMethod
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType
import shipyard_utils as shipyard
import pandas as pd
import os
import ast
import json
from random import random, randrange
from itertools import islice
from io import StringIO
from math import exp, log, floor
try:
    import errors as ec
except BaseException:
    from . import errors as ec

CHUNKSIZE= 50000

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client-id', dest='client_id', required=True)
    parser.add_argument('--secret-key', dest='secret_key', required=True)
    parser.add_argument('--file-name', dest='file_name', required=True)
    parser.add_argument('--dataset-name', dest='dataset_name', required=True)
    parser.add_argument('--dataset-description', dest = 'dataset_description', required=False)
    parser.add_argument("--folder-name", dest = 'folder_name', required = False)
    ## TODO add the --domo-schema as an argument and then use the infer_schema method if no argument is provided
    # parser.add_argument("--domo-schema", dest = 'domo_schema',required = False)
    parser.add_argument("--insert-method", dest = 'insert_method' ,default = 'REPLACE',choices={"REPLACE","APPEND"},required=True)
    parser.add_argument("--dataset-id", required=False, default='',dest='dataset_id')
    args = parser.parse_args()

    return args

def reservoir_sample(iterable, k=1):
    """Select k items uniformly from iterable.

    Returns the whole population if there are k or fewer items

    from https://bugs.python.org/issue41311#msg373733
    """
    iterator = iter(iterable)
    values = list(islice(iterator, k))

    W = exp(log(random())/k)
    while True:
        # skip is geometrically distributed
        skip = floor( log(random())/log(1-W) )
        selection = list(islice(iterator, skip, skip+1))
        if selection:
            values[randrange(k)] = selection[0]
            W *= exp(log(random())/k)
        else:
            return values

def infer_schema(file_name:str, folder_name, domo_instance:Domo, k=10000):
    """ Will return the Domo schema and datatypes of a sampled pandas dataframe

    Args:
        filepath (str): _description_
        k (_type_): the number of random rows to sample
        domo_instance (Domo): _description_

    Returns:
        _type_: _description_
    """
    file_path = file_name
    if folder_name is not None:
        file_path = os.path.normpath(os.path.join(os.getcwd(),folder_name,file_name))
    with open(file_path, 'r') as f:
        header = next(f)
        result = [header] + reservoir_sample(f, k)
    df = pd.read_csv(StringIO(''.join(result)))
    schema = domo_instance.utilities.data_schema(df)
    return Schema(schema)


def make_schema(data_types:list, file_name:str, folder_name:str):
    """Constructs a domo schema which is required for the stream upload

    Args:
        data_types (list): _description_
        file_name (str): _description_
        folder_name (str): _description_

    Returns:
        _type_: _description_
    """
    file_path = file_name
    if folder_name is not None:
        file_path = os.path.normpath(os.path.join(os.getcwd(),folder_name,file_name))
    df = pd.read_csv(file_path,nrows=1)
    cols = list(df.columns)
    if len(cols) != len(data_types):
        print("Error: The number data types does not equal the number of columns. Please number of domo data types provided matches the number of columns")
        sys.exit(ec.EXIT_CODE_COLUMN_MISMATCH)
    domo_schema = []
    for dt, col in zip(data_types, cols):
        dt_upper = str(dt).upper()
        if dt_upper not in ['STRING','DECIMAL','LONG','DOUBLE','DATE','DATETIME']:
            print(f"Error: {dt_upper} is not a valid domo data type. Please ensure one of STRING, DECIMAL, LONG, DOUBLE, DATE, DATETIME is selected")
            sys.exit(ec.EXIT_CODE_INVALID_DATA_TYPE)
        domo_schema.append(Column(dt_upper,col))

    schema = Schema(domo_schema)
    return schema

def dataset_exists(datasets, dataset_name):
    return datasets.name.str.contains(dataset_name).any()

def upload_stream(domo_instance:Domo, file_name:str, dataset_name:str, update_method:str, dataset_id:str, folder_name=None, dataset_description:str=None, domo_schema=None):
    file_path = file_name
    if folder_name is not None:
        file_path = os.path.normpath(os.path.join(os.getcwd(),folder_name,file_name))
    # Check to see if the dataset name exists and the id is not provided. If it exists, then exit
    if dataset_id == '':
        all_datasets = domo_instance.ds_list()
        if dataset_exists(all_datasets,dataset_name):
            print(f"Error: A dataset with the name {dataset_name} already exists. Please provide a unique name to create a new dataset or provide the dataset id to update an existing one")
            sys.exit(ec.EXIT_CODE_DUPLICATE_DATASET)

    streams = domo_instance.streams
    dsr = DataSetRequest()
    dsr.name = dataset_name
    if dataset_description is not None:
        dsr.description = dataset_description
    if domo_schema is not None:
        dsr.schema = domo_schema

    if dataset_id != '':
        stream_property = 'dataSource.id:' + dataset_id
        stream_id = streams.search(stream_property)[0]['id']
        stream_request = CreateStreamRequest(dsr, update_method)
        updated_stream = streams.update(stream_id, stream_request)
    ## if the dataset is provided, it needs to update an existing dataset
    else:
        stream_request = CreateStreamRequest(dsr, update_method)
        stream = streams.create(stream_request)
        stream_property = 'dataSource.name:' + dsr.name
        # stream_id = streams.search(stream_property)[-1]['id']
        stream_id = stream['id']


    execution = streams.create_execution(stream_id)
    execution_id = execution['id']

    # Load the data into domo by chunks and parts
    for part, chunk in enumerate(pd.read_csv(file_path,chunksize=CHUNKSIZE),start = 1):
        execution = streams.upload_part(stream_id, execution_id,part,chunk.to_csv(index=False))
    
    commited_execution = streams.commit_execution(stream_id,execution_id)
    print("Successfully loaded dataset to domo")


def stream_upload(domo_instance:Domo, file_name:str, dataset_name:str, update_method:str, dataset_id:str, folder_name=None, dataset_description:str = None, domo_schema:list = None ):
    file_path = file_name
    if folder_name is not None:
        file_path = os.path.normpath(os.path.join(os.getcwd(),folder_name,file_name))
    # Check to see if the dataset name exists and the id is not provided. If it exists, then exit
    if dataset_id == '':
        all_datasets = domo_instance.ds_list()
        if dataset_exists(all_datasets,dataset_name):
            print(f"Error: A dataset with the name {dataset_name} already exists. Please provide a unique name to create a new dataset or provide the dataset id to update an existing one")
            sys.exit(ec.EXIT_CODE_DUPLICATE_DATASET)

    streams = domo_instance.streams
    dsr = DataSetRequest()
    dsr.name = dataset_name
    if dataset_description is not None:
        dsr.description = dataset_description
    if domo_schema is not None:
        dsr.schema = make_schema(domo_schema,file_name,folder_name)

    # dsr.schema = Schema([
    #     Column(ColumnType.LONG,'rank'),
    #     Column(ColumnType.LONG,'prev_rank'),
    #     Column(ColumnType.STRING,'name'),
    #     Column(ColumnType.STRING,'league'),
    #     Column(ColumnType.DECIMAL,'off'),
    #     Column(ColumnType.DECIMAL,'def'),
    #     Column(ColumnType.DECIMAL,'spi')]
    # )
    
    # stream_request = CreateStreamRequest(dsr,UpdateMethod.REPLACE)
    # stream_request = CreateStreamRequest(dsr,update_method)

    # stream = streams.create(stream_request)
    
    ## if the dataset id is not provided, then it must be created
    if dataset_id != '':
        # stream_property = 'dataSource.id:' + dataset_id
        # stream_id = streams.search(stream_property)[0]['id']
        stream_property = 'dataSource.id:' + dataset_id
        stream_id = streams.search(stream_property)[0]['id']
        stream_request = CreateStreamRequest(dsr, update_method)
        updated_stream = streams.update(stream_id, stream_request)
    ## if the dataset is provided, it needs to update an existing dataset
    else:
        stream_request = CreateStreamRequest(dsr, update_method)
        stream = streams.create(stream_request)
        stream_property = 'dataSource.name:' + dsr.name
        stream_id = streams.search(stream_property)[-1]['id']

        # stream_id = streams.get(stream['id'])['id']
    
    # new_stream = CreateStreamRequest(dsr,update_method)
    # updated_stream = streams.update(stream_id,new_stream)

    execution = streams.create_execution(stream_id)
    execution_id = execution['id']

    # Load the data into domo by chunks and parts
    for part, chunk in enumerate(pd.read_csv(file_path,chunksize=CHUNKSIZE),start = 1):
        execution = streams.upload_part(stream_id, execution_id,part,chunk.to_csv(index=False))
    
    commited_execution = streams.commit_execution(stream_id,execution_id)
    print("Successfully loaded dataset to domo")




def get_csv(file_name:str, folder_name = None):
    """
    Reads in a local file as a csv
    @params: 
        file_name : str - the name of the file to be loaded (provided by the --file-to-load flag)
    """
    file_path = file_name
    if folder_name is not None:
        file_path = os.path.normpath(os.path.join(os.getcwd(),folder_name,file_name))
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"File {file_path} was not found, ensure that the name of the file and folder are entered correctly")
        sys.exit(ec.EXIT_CODE_FILE_NOT_FOUND)


def upload_csv(dataframe:pd.DataFrame, domo_instance, dataset_name:str, dataset_description = None):
    """
    Uploads a pandas dataframe as a dataset in Domo.
    @params:
        dataframe : DataFrame - The pandas dataframe desired to load into Domo
        domo_instance : Domo - The Domo instance you are connected to 
        dataset_namme : str - The name that the dataset will be given in Domo
        dataset_description : str | None - Optional argument that provides a description for the dataset in Domo
    """
    try:
        if dataset_description is None:
            df_dataset = domo_instance.ds_create(dataframe,dataset_name)
        else:
            df_dataset = domo_instance.ds_create(dataframe,dataset_name,dataset_description)
        print(f"Successfully created dataset {dataset_name} with id {df_dataset}")
        return df_dataset
    except Exception as e:
        print(f"Error in uploading the csv to domo")
        print(e)
        sys.exit(ec.EXIT_CODE_UNKNOWN_ERROR)


def main():
    args = get_args()
    client_id = args.client_id
    secret = args.secret_key
    file_to_load = args.file_name
    dataset_name = args.dataset_name
    dataset_description = args.dataset_description
    folder_name = args.folder_name
    # domo_schema = args.domo_schema
    # domo_schema = ast.literal_eval(domo_schema)
    insert_method = args.insert_method
    dataset_id = args.dataset_id
    
    try:
        domo = Domo(
            client_id,
            secret,
            api_host='api.domo.com'
        )
    except Exception as e:
        print(
            'The client_id or secret_key you provided were invalid. Please check for typos and try again.')
        print(e)
        sys.exit(ec.EXIT_CODE_INVALID_CREDENTIALS)
    sample_schema = infer_schema(file_to_load,folder_name,domo,k = 10000)    
    # stream_upload(domo, file_to_load, dataset_name,insert_method ,dataset_id, folder_name = folder_name, dataset_description=dataset_description, domo_schema= domo_schema)
    upload_stream(domo, file_to_load, dataset_name, insert_method, dataset_id, folder_name, dataset_description, sample_schema)
    # df = get_csv(file_to_load, folder_name) ## will exit if not found

    # ## if the dataset already exists
    # if dataset_exists(all_datasets, dataset_name):
    #     matches = all_datasets[all_datasets.name.str.lower() == str(dataset_name).lower()]
    #     if matches.shape[0] > 1:
    #         print(f"Error in inserting new data because the dataset name is not unique. There are {matches.shape[0]} datasets with the same name {dataset_name}. Either provide a new name in the Blueprint or rename the other datasets in the Domo UI.")
    #         sys.exit(ec.EXIT_CODE_DUPLICATE_DATASET)
    #     dataset_id = matches.id.values[0]
    #     # get the dataset id
    #     try:
    #         domo.ds_update(dataset_id,df)
    #         print(f"Successfully replaced dataset {dataset_name}")
    #     except Exception as e:
    #         print("Dataset could not be updated")
    #         print(e)
    #         sys.exit(ec.EXIT_CODE_NON_API_DATASET)


    # ## if it doesn't exist, then create a new one
    # else: 
    #     dataset_id = upload_csv(df, domo_instance = domo, dataset_name = dataset_name, dataset_description = dataset_description)
    #     ## store the response as a variable
    #     base_folder_name = shipyard.logs.determine_base_artifact_folder(
    #     'domo')
    #     artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
    #         base_folder_name)
    #     shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    #     # save dataset id as variable
    #     shipyard.logs.create_pickle_file(artifact_subfolder_paths,
    #                                     'dataset_id', dataset_id)

if __name__ == "__main__":
    main()
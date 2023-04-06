import sys
import argparse
from pydomo import Domo
from pydomo.streams import CreateStreamRequest,UpdateMethod
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType
import shipyard_utils as shipyard
import pandas as pd
import os
import ast
import re
import typing
from random import random, randrange
from itertools import islice
from io import StringIO
from math import exp, log, floor, ceil
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
    parser.add_argument("--domo-schema", dest = 'domo_schema',required = False, default = '')
    parser.add_argument("--insert-method", dest = 'insert_method' ,default = 'REPLACE',choices={"REPLACE","APPEND"},required=True)
    parser.add_argument("--dataset-id", required=False, default='',dest='dataset_id')
    parser.add_argument("--source-file-match-type", dest = "source_file_match_type", choices= {'regex_match', 'exact_match'}, default = 'exact_match', required = False)
    args = parser.parse_args()

    return args

def map_domo_to_pandas(domo_schema) -> dict:
    """Maps the domo datatypes to the associated pandas datatype

    Args:
        domo_schema (list(dict)): Takes in a list of the columns and their associated domo data types
    """
    pandas_dtypes = {}

    for schema in domo_schema:
        dtype = schema['type']
        col = schema['name']
        if dtype == 'BOOLEAN':
            pandas_dtypes[col] = 'bool'
        elif dtype == 'DATETIME':
            pandas_dtypes[col] = 'datetime64[ns]'
        elif dtype == 'DECIMAL':
            pandas_dtypes[col] = 'float64'
        elif dtype == 'LONG':
            pandas_dtypes[col] = 'int64'
        elif dtype == 'STRING':
            pandas_dtypes[col] = 'object'
        else:
            pandas_dtypes[col] = 'object'
    return pandas_dtypes

def reservoir_sample(iterable, k=1):
    """Select k items uniformly from iterable.
    Returns the whole population if there are k or fewer items

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
        filepath (str): the filepath of the file to read
        k (int): the number of random rows to sample
        domo_instance (Domo): the connection to Domo

    Returns:
        Schema: Schema object of the dataset
    """
    if isinstance(file_name, list):
        dataframes = []
        n_files = len(file_name)
        rows_per_file = ceil(k/n_files)
        for file in file_name:
            file_path = file
            if folder_name is not None:
                file_path = os.path.normpath(os.path.join(os.getcwd(),folder_name,file)) 
            with open(file_path, 'r') as f:
                header = next(f)
                result  = [header] + reservoir_sample(f, rows_per_file)
            df = pd.read_csv(StringIO(''.join(result)))
            dataframes.append(df)
        merged = pd.concat(dataframes, axis = 0, ignore_index=True)
        schema = domo_instance.utilities.data_schema(merged)
        return Schema(schema)

    else:
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
        data_types (list): The column name as well as the Domo data types in the form of [['Column1', 'STRING'],['Column2','DECIMAL']]
        file_name (str): The path for the file to read
        folder_name (str): _description_

    Returns:
        Schema: Schema object of the dataset
    """
    if isinstance(file_name, list):
        schemas = []
        for file in file_name:
            file_path = file
            if folder_name is not None:
                file_path = os.path.normpath(os.path.join(os.getcwd(),folder_name,file))
            df = pd.read_csv(file_path,nrows=1)
            cols = list(df.columns)
            if len(cols) != len(data_types):
                print("Error: The number data types does not equal the number of columns. Please number of domo data types provided matches the number of columns")
                sys.exit(ec.EXIT_CODE_COLUMN_MISMATCH)
            domo_schema = []
            for pair in data_types:
                col = pair[0]
                dtype = pair[1]
                dt_upper = str(dtype).upper()
                if dt_upper not in ['STRING','DECIMAL','LONG','DOUBLE','DATE','DATETIME']:
                    print(f"Error: {dt_upper} is not a valid domo data type. Please ensure one of STRING, DECIMAL, LONG, DOUBLE, DATE, DATETIME is selected")
                    sys.exit(ec.EXIT_CODE_INVALID_DATA_TYPE)
                domo_schema.append(Column(dt_upper,col))
                schema = Schema(domo_schema)
                schemas.append(schema)
        assert all(schemas[0] == s for s in schemas)
        return schemas[0]
    else:

        file_path = file_name
        if folder_name is not None:
            file_path = os.path.normpath(os.path.join(os.getcwd(),folder_name,file_name))
        df = pd.read_csv(file_path,nrows=1)
        cols = list(df.columns)
        if len(cols) != len(data_types):
            print("Error: The number data types does not equal the number of columns. Please number of domo data types provided matches the number of columns")
            sys.exit(ec.EXIT_CODE_COLUMN_MISMATCH)

        domo_schema = []
        for pair in data_types:
            col = pair[0]
            dtype = pair[1]
            dt_upper = str(dtype).upper()
            if dt_upper not in ['STRING','DECIMAL','LONG','DOUBLE','DATE','DATETIME']:
                print(f"Error: {dt_upper} is not a valid domo data type. Please ensure one of STRING, DECIMAL, LONG, DOUBLE, DATE, DATETIME is selected")
                sys.exit(ec.EXIT_CODE_INVALID_DATA_TYPE)
            domo_schema.append(Column(dt_upper,col))
        
        return Schema(domo_schema)


def dataset_exists(datasets, dataset_name):
    return datasets.name.str.contains(dataset_name).any()

def upload_stream(domo_instance:Domo, file_name:str, dataset_name:str, update_method:str, dataset_id:str, folder_name=None, dataset_description:str=None, domo_schema=None):
    """Uploads the dataset using the Stream API

    Args:
        domo_instance (Domo): connection to Domo
        file_name (str | list): The file path of the dataset. If Regex match is selected, then this will be a list
        dataset_name (str): The name of the dataset
        update_method (str): The update method (REPLACE or APPEND)
        dataset_id (str): The id of the dataset if modifying an existing one
        folder_name (_type_, optional): The name of the folder path if applicable
        dataset_description (str, optional): Optional description of the dataset 
        domo_schema (_type_, optional): Optional schema of the dataset. If omitted, then the data types will be inferred using sampling
    """
    file_path = file_name
    streams = domo_instance.streams
    dsr = DataSetRequest()
    dsr.name = dataset_name
    dsr.schema = domo_schema
    if dataset_description is not None:
        dsr.description = dataset_description

    if dataset_id != '': # if a dataset id has been provided, meaning an existing dataset will be modified
        # check to see if the schemas are identical
        schema_in_domo = domo_instance.utilities.domo_schema(dataset_id)
        dataset_schema = domo_schema['columns']
        pandas_dtypes = map_domo_to_pandas(dataset_schema)
        if not domo_instance.utilities.identical(c1=schema_in_domo, c2 = dataset_schema):
            url = '/v1/datasets/{ds}'.format(ds=dataset_id)
            change_result = domo_instance.transport.put(url,{'schema': {'columns': dataset_schema}})
            print("Schema updated")
        stream_property = 'dataSource.id:' + dataset_id
        stream_id = streams.search(stream_property)[0]['id']
        stream_request = CreateStreamRequest(dsr, update_method)
        updated_stream = streams.update(stream_id, stream_request)

    ## if the dataset is not provided, a new one will be created
    else:
        stream_request = CreateStreamRequest(dsr, update_method)
        stream = streams.create(stream_request)
        stream_property = 'dataSource.name:' + dsr.name
        stream_id = stream['id']
        pandas_dtypes = None

    execution = streams.create_execution(stream_id)
    execution_id = execution['id']

    # if the regex match is selected, load all the files to a single domo dataset
    if isinstance(file_name, list):
        index = 0
        for file in file_name:
            for part, chunk in enumerate(pd.read_csv(file, chunksize= CHUNKSIZE, dtype = pandas_dtypes),start = 1):
                index += 1
                print(f"Uploading file {file} part {part}")
                execution = streams.upload_part(stream_id, execution_id, index, chunk.to_csv(index = False, header = False))
    # otherwise load a single file
    else:
        # Load the data into domo by chunks and parts
        for part, chunk in enumerate(pd.read_csv(file_path,chunksize=CHUNKSIZE,dtype=pandas_dtypes),start = 1):
            execution = streams.upload_part(stream_id, execution_id,part,chunk.to_csv(index=False, header = False))

    # commit the stream 
    commited_execution = streams.commit_execution(stream_id,execution_id)
    print("Successfully loaded dataset to domo")
    return stream_id, execution_id

def main():
    args = get_args()
    client_id = args.client_id
    secret = args.secret_key
    file_to_load = args.file_name
    dataset_name = args.dataset_name
    dataset_description = args.dataset_description
    folder_name = args.folder_name
    insert_method = args.insert_method
    dataset_id = args.dataset_id
    match_type = args.source_file_match_type
    if args.domo_schema != '':
        domo_schema = args.domo_schema
        domo_schema = ast.literal_eval(domo_schema)
    
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
    
    if match_type == 'regex_match':
        file_names = shipyard.files.find_all_local_file_names(
        folder_name)
        matching_file_names = shipyard.files.find_all_file_matches(
        file_names, re.compile(file_to_load))
        print(f'{len(matching_file_names)} files found. Preparing to upload...')
        # if the schema is provided, then use that otherwise infer the schema using sampling
        if args.domo_schema != '':
            dataset_schema = make_schema(domo_schema, matching_file_names, folder_name)
        else:
            dataset_schema = infer_schema(matching_file_names, folder_name, domo, k = 10000)
        stream_id, execution_id = upload_stream(domo, matching_file_names, dataset_name,
                                                insert_method, dataset_id, 
                                                folder_name, dataset_description, dataset_schema)
        base_folder_name = shipyard.logs.determine_base_artifact_folder(
            'domo')
        artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
            base_folder_name)
        shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
        shipyard.logs.create_pickle_file(artifact_subfolder_paths, 'stream_id', stream_id)
        shipyard.logs.create_pickle_file(artifact_subfolder_paths, 'execution_id', execution_id)

    else:
        # if the schema is provided, then use that otherwise infer the schema using sampling
        if args.domo_schema != '':
            dataset_schema = make_schema(domo_schema, file_to_load, folder_name)
        else:
            dataset_schema = infer_schema(file_to_load, folder_name, domo, k = 10000)
        stream_id, execution_id = upload_stream(domo, file_to_load, dataset_name,insert_method, dataset_id, 
            folder_name, dataset_description, dataset_schema)

        base_folder_name = shipyard.logs.determine_base_artifact_folder(
            'domo')
        artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
            base_folder_name)
        shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
        shipyard.logs.create_pickle_file(artifact_subfolder_paths, 'stream_id', stream_id)
        shipyard.logs.create_pickle_file(artifact_subfolder_paths, 'execution_id', execution_id)

if __name__ == "__main__":
    main()
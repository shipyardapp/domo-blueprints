import sys
import argparse
import json
from pydomo import Domo
import requests
import shipyard_utils as shipyard
import pandas as pd
try:
    import errors as ec
except BaseException:
    from . import errors as ec


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client-id', dest='client_id', required=True)
    parser.add_argument('--secret-key', dest='secret_key', required=True)
    # parser.add_argument('--domo-instance', dest='domo_instance', required=True)
    parser.add_argument('--dataset-id', dest = 'dataset_id', required = True)
    parser.add_argument('--file-to-load', dest='file_to_load', required=True)
    args = parser.parse_args()

    return args


def get_csv(file_name:str):
    """
    Reads in a local file as a csv
    @params: 
        file_name : str - the name of the file to be loaded (provided by the --file-to-load flag)
    """
    try:
        df = pd.read_csv(file_name)
        return df
    except Exception as e:
        print(f"File {file_name} was not found, ensure that the name of the file is entered correctly")
        sys.exit(ec.EXIT_CODE_FILE_NOT_FOUND)


def update_dataset(df:pd.DataFrame, domo_instance, dataset_id:str):
    """
    Uploads a pandas dataframe as a dataset in Domo.
    @params:
        dataframe : DataFrame - The pandas dataframe desired to load into Domo
        domo_instance : Domo - The Domo instance you are connected to 
        dataset_id : str - The id tied to the dataset that you wish to update
    """
    try:
        response = domo_instance.ds_update(dataset_id,df)
        print(f"Successfully updated dataset {dataset_id}")
        return response
    except Exception as e:
        print(f"Error in updating dataset {dataset_id}")
        print(e)
        sys.exit(ec.EXIT_CODE_DATASET_NOT_FOUND)


def main():
    args = get_args()
    client_id = args.client_id
    secret_key = args.secret_key
    dataset_id = args.dataset_id
    file_to_load = args.file_to_load
    try:
        domo = Domo(
            client_id,
            secret_key,
            api_host='api.domo.com'
        )
    except Exception as e:
        print(
            'The client_id or secret_key you provided were invalid. Please check for typos and try again.')
        print(e)
        sys.exit(ec.EXIT_CODE_INVALID_CREDENTIALS)
    df = get_csv(file_to_load)
    response = update_dataset(df,domo,dataset_id = dataset_id)


if __name__ == '__main__':
    main()






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
    parser.add_argument('--file-to-load', dest='file_to_load', required=True)
    parser.add_argument('--dataset-name', dest='dataset_name', required=True)
    parser.add_argument('--dataset-description', dest = 'dataset_description', required=False)
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
    # domo_instance = args.domo_instance
    file_to_load = args.file_to_load
    dataset_name = args.dataset_name
    dataset_description = args.dataset_description
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

    df = get_csv(file_to_load) ## will exit if not found
    dataset_id = upload_csv(df, domo_instance = domo, dataset_name = dataset_name, dataset_description = dataset_description)
    ## store the response as a variable
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
    'domo')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # save dataset id as variable
    shipyard.logs.create_pickle_file(artifact_subfolder_paths,
                                     'dataset_id', dataset_id)

if __name__ == "__main__":
    main()
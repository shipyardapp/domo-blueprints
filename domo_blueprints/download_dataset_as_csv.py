import sys
import os
import argparse
from pydomo import Domo
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
    parser.add_argument('--dataset-id', dest= 'dataset_id',required = True)
    parser.add_argument('--destination-file-name',dest = 'dest_file_name',required = True)
    parser.add_argument('--destination-folder-path',dest = 'dest_folder_path', required = False)
    args = parser.parse_args()
    return args


def get_dataset(ds_id,domo_instance):
    """
    Wrapper function around the Domo `ds_get` function with some logic to gracefully exit
    """
    try:
        df = domo_instance.ds_get(ds_id)
        return df
    except Exception as e:
        print(f"Error in downloading the dataset {ds_id}. Please ensure that is the correct one and that the given API client and secret have the appropriate permissions to download datasets")
        sys.exit(ec.EXIT_CODE_DATASET_NOT_FOUND)


def clean_folder_name(folder_name):
    """
    Cleans folders name by removing duplicate '/' as well as leading and trailing '/' characters.
    """
    folder_name = folder_name.strip('/')
    if folder_name != '':
        folder_name = os.path.normpath(folder_name)
    return folder_name


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')
    combined_name = os.path.normpath(combined_name)

    return combined_name


def write_file(df:pd.DataFrame, file_name:str, folder_path:str):
    if folder_path is None:
        ## should just be put in the home directory 
        cwd = os.getcwd()
        full_path = combine_folder_and_file_name(cwd,file_name=file_name)
        pass
    else:
        full_path = combine_folder_and_file_name(folder_path,file_name=file_name)

    try:
        df.to_csv(full_path)
        print(f"Successfully wrote {file_name} to {full_path}")
    except Exception as e:
        print(f"Error in writing {file_name} to {full_path}.")
        print(e)
        sys.exit(ec.EXIT_CODE_FILE_NOT_FOUND)

def main():
    args = get_args()
    client_id = args.client_id
    secret_key = args.secret_key
    dataset_id = args.dataset_id
    dest_file_name = args.dest_file_name
    dest_folder_path = args.dest_folder_path
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

    df = get_dataset(dataset_id,domo)
    write_file(df, dest_file_name, dest_folder_path)

if __name__ == "__main__":
    main()

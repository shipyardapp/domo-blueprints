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
    parser.add_argument('--email', dest='email', required=False)
    parser.add_argument('--password', dest='password', required=False)
    parser.add_argument('--domo-instance', dest='domo_instance', required=True)
    parser.add_argument('--file-to-load', dest='file_to_load', required=True)
    parser.add_argument('--developer-token',
                        dest='developer_token',
                        required=False)
    args = parser.parse_args()

    if not args.developer_token and not (
            args.email or args.password):
        parser.error(
            """This Blueprint requires at least one of the following to be provided:\n
            1) --developer-token\n
            2) --email and --password""")
    if args.email and not args.password:
        parser.error('Please provide a password with your email.')
    if args.password and not args.email:
        parser.error('Please provide an email with your password.')
    return args


def get_csv(file_name):
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


def upload_csv(dataframe, domo_instance, dataset_name, dataset_description = None):
    try:
        if dataset_description is None:
            df_dataset = domo.ds_create(dataframe,dataset_name)
            return df_dataset
        else:
            df_dataset = domo.ds_create(dataframe,dataset_name,dataset_description)
            return df_dataset
    except Exception as e:
        print(f"Error in uploading the csv to domo")
        print(e)
        sys.exit(ec.EXIT_CODE_UNKNOWN_ERROR)


def main():
    pass


if __name__ == "__main__":
    pass
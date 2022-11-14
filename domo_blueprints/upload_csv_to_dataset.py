import sys
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
    parser.add_argument('--file-name', dest='file_name', required=True)
    parser.add_argument('--dataset-name', dest='dataset_name', required=True)
    parser.add_argument('--dataset-description', dest = 'dataset_description', required=False)
    args = parser.parse_args()

    return args


def dataset_exists(datasets, dataset_name):
    return datasets.name.str.contains(dataset_name).any()


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
    file_to_load = args.file_name
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

    all_datasets = domo.ds_list()
    df = get_csv(file_to_load) ## will exit if not found

    ## if the dataset already exists
    if dataset_exists(all_datasets, dataset_name):
        matches = all_datasets[all_datasets.name.str.lower() == str(dataset_name).lower()]
        if matches.shape[0] > 1:
            print(f"Error in inserting new data because the dataset name is not unique. There are {matches.shape[0]} datasets with the same name {dataset_name}. Either provide a new name in the Blueprint or rename the other datasets in the Domo UI.")
            sys.exit(ec.EXIT_CODE_DUPLICATE_DATASET)
        dataset_id = matches.id.values[0]
        # get the dataset id
        try:
            domo.ds_update(dataset_id,df)
            print(f"Successfully replaced dataset {dataset_name}")
        except Exception as e:
            print("Dataset could not be updated")
            print(e)
            sys.exit(ec.EXIT_CODE_NON_API_DATASET)


    ## if it doesn't exist, then create a new one
    else: 
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
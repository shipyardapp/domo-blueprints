import sys
import time
import json
import pydomo
import requests
import argparse
import shipyard_utils as shipyard

try:
    import errors
except BaseException:
    from . import errors


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client-id', dest='client_id', required=True)
    parser.add_argument('--secret-key', dest='secret_key', required=True)
    parser.add_argument('--dataset-id', dest='dataset_id', required=True)
    parser.add_argument("--wait-for-completion", dest="wait_for_completion", required = False, default = 'FALSE')
    args = parser.parse_args()

    return args

def determine_execution_status(execution_data):
    # check if execution has finished first
    status = execution_data['currentState']
    if status == 'ABORTED':
        print("Domo Refresh has been aborted")
        return errors.EXIT_CODE_FINAL_STATUS_CANCELLED
    elif status == 'ACTIVE':
        print(f"Domo Refresh is still currently ongoing with status {status}")
        return errors.EXIT_CODE_STATUS_INCOMPLETE
    elif status == 'INVALID':
        print("Domo Refresh is invalid either due to system conflict or error")
        return errors.EXIT_CODE_FINAL_STATUS_INVALID
    elif status == 'SUCCESS':
        print("Domo has refreshed successfully ")
        return errors.EXIT_CODE_FINAL_STATUS_SUCCESS
    else:
        print(f"Unknown Domo Refresh status: {status}")
        return errors.EXIT_CODE_UNKNOWN_STATUS


def get_execution_details(dataset_id, execution_id, domo):
    """
    Gets the Stream ID of a particular stream using the dataSet id.

    Returns:
        stream_id (int): the Id of the found stream
    """
    streams = domo.streams
    limit = 1000
    offset = 0
    # get all streams
    stream_list = streams.list(limit, offset)
    # return stream with matching dataset id
    for stream in stream_list:
        if stream['dataSet']['id'] == dataset_id:
            # get execution details from id
            try:
                return streams.get_execution(stream['id'], execution_id)
            except Exception as e:
                print(f"Error occurred - {e}")
                sys.exit(errors.EXIT_CODE_EXECUTION_ID_NOT_FOUND)
    print(f"stream with dataSet id:{dataset_id} not found!")
    sys.exit(errors.EXIT_CODE_DATASET_NOT_FOUND)


def create_pickle_file(execution_id):
    # create artifacts folder to save variable
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'domo')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # save execution id as variable
    shipyard.logs.create_pickle_file(artifact_subfolder_paths,
                                     'execution_id', execution_id)


def create_pass_token_header(access_token):
    """
    Generate Auth headers for DOMO private API using email/password
    authentication.

    Returns:
    auth_header -> dict with the authentication headers for use in
    domo api requests.
    """
    return {
        'Content-Type': 'application/json',
        'x-domo-authentication': access_token,
    }


def create_dev_token_header(developer_token):
    """
    Generate Auth headers for DOMO private API using developer
    access tokens found at the following url:
    https://<domo-instance>.domo.com/admin/security/accesstokens

    Returns:
    auth_header -> dict with the authentication headers for use in
    domo api requests.
    """
    return {
        'Content-Type': 'application/json',
        'x-domo-developer-token': developer_token,
    }


def get_stream_from_dataset_id(dataset_id, domo):
    """
    Gets the Stream ID of a particular stream using the dataSet id.

    Returns:
        stream_id (int): the Id of the found stream
    """
    streams = domo.streams
    limit = 1000
    offset = 0
    # get all streams
    stream_list = streams.list(limit, offset)
    try:
        stream_id = domo.utilities.get_stream_id(ds_id = 'aaa3604a-9f5d-4855-bc95-6700486e7bc1')
    except Exception as e:
        print(f"stream with dataSet ID:{dataset_id} not found. Ensure that a valid dataset ID is provided and is the ID of an outputted dataset from a dataflow")
        sys.exit(errors.EXIT_CODE_DATASET_NOT_FOUND)
    else:
        return stream_id


def run_stream_refresh(stream_id:str, domo_instance:pydomo.Domo):
    """
    Executes/starts a stream
    """
    streams = domo_instance.streams
    try:
        execution = streams.create_execution(stream_id)
    except Exception as e:
        print("Error in starting stream execution")
        print(e)
        sys.exit(errors.EXIT_CODE_REFRESH_ERROR)
    else:
        print("Refresh started successfully")
        return execution

def main():
    args = get_args()
    # initialize domo with auth credentials
    try:
        domo = pydomo.Domo(
            args.client_id,
            args.secret_key,
            api_host='api.domo.com'
        )
    except Exception as e:
        print(
            'The client_id or secret_key you provided were invalid. Please check for typos and try again.')
        print(e)
        sys.exit(errors.EXIT_CODE_INVALID_CREDENTIALS)

    # execute dataset refresh
    dataset_id = args.dataset_id
    stream_id = get_stream_from_dataset_id(dataset_id, domo)
    refresh_data = run_stream_refresh(stream_id, domo_instance=domo)
    execution_id = refresh_data['id']

    create_pickle_file(execution_id)  # needed for backwards compatibility
    if args.wait_for_completion == "TRUE":
        execution_data = get_execution_details(dataset_id, execution_id, domo)
        exit_code_status = determine_execution_status(execution_data)
        print("Waiting for Domo Refresh to complete")
        while exit_code_status == errors.EXIT_CODE_STATUS_INCOMPLETE:
            print("Waiting for 60 seconds before checking again")
            time.sleep(60)
            execution_data = get_execution_details(dataset_id, execution_id, domo)
            exit_code_status = determine_execution_status(execution_data)
        sys.exit(exit_code_status)


if __name__ == "__main__":
    main()

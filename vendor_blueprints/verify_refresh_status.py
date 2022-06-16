import argparse
import sys
import pydomo
import shipyard_utils as shipyard


EXIT_CODE_FINAL_STATUS_SUCCESS = 0
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_ACCOUNT = 201
EXIT_CODE_DATASET_NOT_FOUND = 202
EXIT_CODE_EXECUTION_ID_NOT_FOUND = 203
EXIT_CODE_FINAL_STATUS_INVALID = 204
EXIT_CODE_FINAL_STATUS_CANCELLED = 205
EXIT_CODE_STATUS_INCOMPLETE = 206


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client-id', dest='client_id', required=True)
    parser.add_argument('--secret-key', dest='secret_key', required=True)
    parser.add_argument('--dataset-id', dest='dataset_id', required=True)
    parser.add_argument('--execution-id', dest='execution_id', required=False)
    args = parser.parse_args()
    return args


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
                execution_data = streams.get_execution(stream['id'], execution_id)
                return execution_data
            except Exception as e:
                print(f"Error occured - {e}")
                sys.exit(EXIT_CODE_EXECUTION_ID_NOT_FOUND)
    else:
        print(f"stream with dataSet id:{dataset_id} not found!")
        sys.exit(EXIT_CODE_DATASET_NOT_FOUND)


def determine_execution_status(execution_data):
    # check if execution has finished first
    if execution_data['endedAt']:
        status = execution_data['currentState']
        if status == 'SUCCESS':
            exit_code = EXIT_CODE_FINAL_STATUS_SUCCESS
        elif status == 'INVALID':
            exit_code = EXIT_CODE_FINAL_STATUS_INVALID
        elif status == 'ABORTED':
            exit_code = EXIT_CODE_FINAL_STATUS_CANCELLED
        elif status == 'ACTIVE':
            exit_code = EXIT_CODE_STATUS_INCOMPLETE
    else:
        # execution has not finished running
        print(f"Execution {execution_data['id']} not yet completed")
        exit_code = EXIT_CODE_STATUS_INCOMPLETE
    # return exit code
    return exit_code
        

def main():
    args = get_args()
    # initialize domo with auth credentials
    domo = pydomo.Domo(
        args.client_id,
        args.secret_key,
        api_host='api.domo.com'
    )
    dataset_id = args.dataset_id
    # create artifacts folder to save variable
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'domo')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
    # get execution id from client and fallback to pickle file
    if args.execution_id:
        execution_id = args.execution_id
    else:
        execution_id = shipyard.logs.read_pickle_file(
            artifact_subfolder_paths, 'execution_id')
    # run check status
    execution_data = get_execution_details(dataset_id, execution_id, domo)
    exit_code_status = determine_execution_status(execution_data)
    sys.exit(exit_code_status)


if __name__ == "__main__":
    main()
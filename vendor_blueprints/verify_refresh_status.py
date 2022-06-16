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
    parser.add_argument('--execution-id', dest='execution_id', required=True)
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
            sys.exit(EXIT_CODE_FINAL_STATUS_SUCCESS)
        elif status == 'INVALID':
            sys.exit(EXIT_CODE_FINAL_STATUS_INVALID)
        elif status == 'ABORTED':
            sys.exit(EXIT_CODE_FINAL_STATUS_CANCELLED)
        elif status == 'ACTIVE':
            sys.exit(EXIT_CODE_STATUS_INCOMPLETE)
    else:
        # execution has not finished running
        print(f"Execution {execution_data['id']} not yet completed")
        sys.exit(EXIT_CODE_STATUS_INCOMPLETE)
        

def main():
    pass


if __name__ == "__main__":
    main()
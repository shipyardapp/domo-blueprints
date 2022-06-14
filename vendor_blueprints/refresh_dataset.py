import sys
import argparse
import pydomo


EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_DATASET_NOT_FOUND = 201



def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client-id', dest='client_id', required=True)
    parser.add_argument('--secret-key', dest='secret_key', required=True)
    parser.add_argument('--dataset-id', dest='dataset_id', required=True)
    args = parser.parse_args()
    return args


def get_stream_from_dataset_id(dataset_id, domo):
    """
    Gets the Stream ID of a particular stream using the dataSet id.
    
    Returns:
        stream_id (int): the Id of the found stream 
    """
    streams = domo.streams
    limit = 99
    offset = 0
    # get all streams
    stream_list = streams.list(limit, offset)
    # return stream with matching dataset id
    for stream in stream_list:
        if stream['dataSet']['id'] == dataset_id:
            return stream['id']
    else:
        print(f"stream with dataSet id:{dataset_id} not found!")
        sys.exit(EXIT_CODE_DATASET_NOT_FOUND)


def run_stream_refresh(stream_id, domo):
    """
    Executes/starts a stream 
    """
    streams = domo.streams
    limit = 99
    offset = 0
    # get list of all the stream executions
    execution_list = streams.list_executions(stream_id, limit, offset)
    # run commit to update all stream executions associated with a stream id
    for execution in execution_list:
        committed_execution = streams.commit_execution(stream_id,
                                                   execution['id'])


def main():
    args = get_args()
    # initialize domo with auth credentials
    domo = pydomo.Domo(
        args.client_id,
        args.secret_key,
        api_host='api.domo.com'
    )
    # execute dataset refresh
    dataset_id = args.dataset_id
    stream_id = get_stream_from_dataset_id(dataset_id, domo)
    run_stream_refresh(stream_id, domo)
    
if __name__ == "__main__":
    main()
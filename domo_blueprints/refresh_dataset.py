import sys
import argparse
import json
import pydomo
import requests
import shipyard_utils as shipyard


EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_ACCOUNT = 201
EXIT_CODE_BAD_REQUEST = 202
EXIT_CODE_DATASET_NOT_FOUND = 203
EXIT_CODE_REFRESH_ERROR = 204


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client-id', dest='client_id', required=True)
    parser.add_argument('--secret-key', dest='secret_key', required=True)
    parser.add_argument('--email', dest='email', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--domo-instance', dest='domo_instance', required=True)
    parser.add_argument('--dataset-id', dest='dataset_id', required=True)
    parser.add_argument(
        '--developer-token',
        dest='developer_token',
        required=False)
    args = parser.parse_args()
    return args


def get_access_token(email, password, domo_instance):
    """
    Generate Access Token for use with internal content APIs.

    email (str): email address of the user on domo.com
    password (str): login password of the user domo.com
    """
    auth_api = f"https://{domo_instance}.domo.com/api/content/v2/authentication"

    auth_body = json.dumps({
        "method": "password",
        "emailAddress": email,
        "password": password
    })

    auth_headers = {'Content-Type': 'application/json'}
    try:
        auth_response = requests.post(auth_api, data=auth_body,
                                      headers=auth_headers)
    except Exception as e:
        print(f"Request error: {e}")
        sys.exit(EXIT_CODE_BAD_REQUEST)

    auth_response_json = auth_response.json()
    try:
        if auth_response_json["success"] is False:  # Failed to login
            print(
                f"Authentication failed due to reason: {auth_response_json['reason']}")
            sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
    except Exception as e:
        if auth_response_json["status"] == 403:  # Failed to login
            print(
                f"Authentication failed due to domo instance {domo_instance} being invalid.")
            sys.exit(EXIT_CODE_INVALID_ACCOUNT)
        else:
            print(f"Request error: {e}")
            sys.exit(EXIT_CODE_BAD_REQUEST)

    # else if the authentication succeeded
    domo_token = auth_response_json['sessionToken']
    return domo_token


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
    # return stream with matching dataset id
    for stream in stream_list:
        if stream['dataSet']['id'] == dataset_id:
            return stream['id']
    else:
        print(f"stream with dataSet id:{dataset_id} not found!")
        sys.exit(EXIT_CODE_DATASET_NOT_FOUND)


def run_stream_refresh(stream_id, domo_instance, access_token):
    """
    Executes/starts a stream
    """
    stream_post_api = f"https://{domo_instance}.domo.com/api/data/v1/streams/{stream_id}/executions"
    card_headers = {
        'Content-Type': 'application/json',
        'x-domo-authentication': access_token
    }
    payload = {
        "runType": "MANUAL"
    }
    stream_refresh_response = requests.post(stream_post_api,
                                            json=payload,
                                            headers=card_headers)

    if stream_refresh_response.status_code == 201:
        print(f"stream refresh for stream:{stream_id} successful")
        return stream_refresh_response.json()
    else:
        print(
            f"Encountered an error with the code {stream_refresh_response.status_code}")
        sys.exit(EXIT_CODE_REFRESH_ERROR)


def run_stream_refresh_dev(stream_id, domo_instance, dev_token):
    """
    Executes/starts a stream
    """
    stream_post_api = f"https://{domo_instance}.domo.com/api/data/v1/streams/{stream_id}/executions"
    card_headers = {
        'Content-Type': 'application/json',
        'x-domo-developer-token': dev_token
    }
    payload = {
        "runType": "MANUAL"
    }
    print("Using developer token for stream refresh")
    stream_refresh_response = requests.post(stream_post_api,
                                            json=payload,
                                            headers=card_headers)
    if stream_refresh_response.status_code == 201:
        print(f"stream refresh for stream:{stream_id} successful")
        return stream_refresh_response.json()
    else:
        print(
            f"Encountered an error with the code {stream_refresh_response.status_code}")
        sys.exit(EXIT_CODE_REFRESH_ERROR)


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
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
    email = args.email
    password = args.password
    domo_instance = args.domo_instance
    access_token = get_access_token(email, password, domo_instance)

    developer_token = args.developer_token
    # execute dataset refresh
    dataset_id = args.dataset_id
    stream_id = get_stream_from_dataset_id(dataset_id, domo)

    # if developer token is enabled, try that instead
    if args.developer_token:
        refresh_data = run_stream_refresh_dev(
            stream_id, domo_instance, developer_token)
    else:
        refresh_data = run_stream_refresh(
            stream_id, domo_instance, access_token)
    execution_id = refresh_data['executionId']

    # create artifacts folder to save variable
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'domo')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # save execution id as variable
<<<<<<< HEAD
    shipyard.logs.create_pickle_file(artifact_subfolder_paths,
                                     'execution_id', execution_id)

=======
    shipyard.logs.create_pickle_file(artifact_subfolder_paths, 
                                'execution_id', execution_id)
>>>>>>> 47a59ae4ee2f8155c2f62f36270e1e00570e6db9

if __name__ == "__main__":
    main()

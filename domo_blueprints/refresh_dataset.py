import sys
import argparse
import json
import pydomo
import requests
import shipyard_utils as shipyard

try:
    import errors
except BaseException:
    from . import errors


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client-id', dest='client_id', required=True)
    parser.add_argument('--secret-key', dest='secret_key', required=True)
    parser.add_argument('--email', dest='email', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--domo-instance', dest='domo_instance', required=True)
    parser.add_argument('--dataset-id', dest='dataset_id', required=True)
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
        sys.exit(errors.EXIT_CODE_BAD_REQUEST)

    auth_response_json = auth_response.json()
    try:
        if auth_response_json["success"] is False:  # Failed to login
            print(
                f"Authentication failed due to reason: {auth_response_json['reason']}")
            sys.exit(errors.EXIT_CODE_INVALID_CREDENTIALS)
    except Exception as e:
        if auth_response_json["status"] == 403:  # Failed to login
            print(
                f"Authentication failed due to domo instance {domo_instance} being invalid.")
            sys.exit(errors.EXIT_CODE_INVALID_ACCOUNT)
        else:
            print(f"Request error: {e}")
            sys.exit(errors.EXIT_CODE_BAD_REQUEST)

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
        sys.exit(errors.EXIT_CODE_DATASET_NOT_FOUND)


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
        sys.exit(errors.EXIT_CODE_REFRESH_ERROR)


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
    email = args.email
    password = args.password
    domo_instance = args.domo_instance
    access_token = get_access_token(email, password, domo_instance)

    # execute dataset refresh
    dataset_id = args.dataset_id
    stream_id = get_stream_from_dataset_id(dataset_id, domo)
    refresh_data = run_stream_refresh(stream_id, domo_instance, access_token)
    execution_id = refresh_data['executionId']

    # create artifacts folder to save variable
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'domo')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # save execution id as variable
    shipyard.logs.create_pickle_file(artifact_subfolder_paths,
                                     'execution_id', execution_id)


if __name__ == "__main__":
    main()

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
    parser.add_argument('--email', dest='email', required=False)
    parser.add_argument('--password', dest='password', required=False)
    parser.add_argument('--domo-instance', dest='domo_instance', required=True)
    parser.add_argument('--dataset-id', dest='dataset_id', required=True)
    parser.add_argument('--developer-token',
                        dest='developer_token',
                        required=False)
    parser.add_argument("--wait-for-completion", dest="wait_for_completion")
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
    try:
        return auth_response_json['sessionToken']
    except BaseException as e:
        print(
            'Username/Password authentication is not accepted for your organization. Please use an access token instead.'
        )
        sys.exit(errors.EXIT_CODE_USERNAME_PASSWORD_NOT_ACCEPTED)


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
    # return stream with matching dataset id
    for stream in stream_list:
        if stream['dataSet']['id'] == dataset_id:
            return stream['id']
    print(f"stream with dataSet id:{dataset_id} not found!")
    sys.exit(errors.EXIT_CODE_DATASET_NOT_FOUND)


def run_stream_refresh(stream_id, domo_instance, auth_headers):
    """
    Executes/starts a stream
    """
    stream_post_api = f"https://{domo_instance}.domo.com/api/data/v1/streams/{stream_id}/executions"
    payload = {
        "runType": "MANUAL"
    }
    print("Using developer token for stream refresh")
    stream_refresh_response = requests.post(stream_post_api,
                                            json=payload,
                                            headers=auth_headers)

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
    # create auth headers for sending requests
    if args.developer_token:
        auth_headers = create_dev_token_header(args.developer_token)
    else:
        access_token = get_access_token(email, password, domo_instance)
        auth_headers = create_pass_token_header(access_token)

    # execute dataset refresh
    dataset_id = args.dataset_id
    stream_id = get_stream_from_dataset_id(dataset_id, domo)
    refresh_data = run_stream_refresh(stream_id, domo_instance, auth_headers)
    execution_id = refresh_data['executionId']

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

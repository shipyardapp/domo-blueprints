import json
import sys
import argparse
import requests
import shipyard_utils as shipyard


EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_ACCOUNT = 201
EXIT_CODE_BAD_REQUEST = 202
EXIT_CODE_INCORRECT_CARD_TYPE = 203


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--email', dest='email', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--domo-instance', dest='domo_instance', required=True)
    parser.add_argument('--card-id', dest='card_id', required=True)
    parser.add_argument('--dest-folder-path', 
                        dest='dest_folder_path', 
                        default='',
                        required=True)
    parser.add_argument('--developer-token',
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

    auth_headers = {'Content-Type' : 'application/json'}
    try:
        auth_response = requests.post(auth_api, data=auth_body, 
                                      headers=auth_headers)
    except Exception as e:
        print(f"Request error: {e}")
        sys.exit(EXIT_CODE_BAD_REQUEST)

    auth_response_json = auth_response.json()
    if auth_response_json["success"] is False: # Failed to login
        print(f"Authentication failed due to reason: {auth_response_json['reason']}")
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
        
    # else if the authentication succeeded
    domo_token = auth_response_json['sessionToken']
    return domo_token


def create_pass_token_header(access_token):
    """
    Generate Auth headers for DOMO private API using email/password
    authentication.

    Returns:
    auth_header -> dict with the authentication headers for use in
    domo api requests. 
    """
    auth_headers = {
        'Content-Type': 'application/json',
        'x-domo-authentication': access_token
    }
    return auth_headers


def create_dev_token_header(developer_token):
    """
    Generate Auth headers for DOMO private API using developer
    access tokens found at the following url:
    https://<domo-instance>.domo.com/admin/security/accesstokens

    Returns:
    auth_header -> dict with the authentication headers for use in
    domo api requests. 
    """
    auth_headers = {
        'Content-Type': 'application/json',
        'x-domo-developer-token': developer_token
    }
    return auth_headers


def get_card_data(card_id, auth_headers, domo_instance):
    """
    Get metadata and property information of a single card

    card_id (str): The unique ID of the card
    access_token: client access token. Use the get_access_token() function to retrieve the token.

    Returns:
    card_response -> dict with the metadata and details of the card.
    """
    card_info_api = f"https://{domo_instance}.domo.com/api/content/v1/cards"
    params = {
        'urns': card_id, 
        'parts': ['metadata', 'properties'],
        'includeFiltered': 'true'
        }
    card_response = requests.get(url=card_info_api, params=params, headers=auth_headers)
    return card_response.json()


def export_document_to_file(card_id, auth_headers, domo_instance, folder_path=''):
    # grab the document id from card metadata
    card = get_card_data(card_id, auth_headers, domo_instance)[0]
    document_id = card['metadata']['revisionId']
    document_name = card['title']
    file_download_api = f"https://{domo_instance}.domo.com/api/data/v1/data-files/{document_id}/revisions/{document_id}"
    params = {
        'fileName': document_name
        }
    file_response = requests.get(url=file_download_api, params=params,
                             headers=auth_headers, stream=True)
    if file_response.status_code == 200:
        destination_folder_name = shipyard.files.clean_folder_name(
            folder_path)
        destination_full_path = shipyard.files.combine_folder_and_file_name(
        folder_name=destination_folder_name, file_name=document_name)
        with open(destination_full_path, 'wb') as fd:
            # iterate through the blob 1MB at a time
            for chunk in file_response.iter_content(1024*1024):
                fd.write(chunk)
        print(f" file:{destination_full_path} saved successfully!")
    else:
        print(f"Request failed with status code {file_response.status_code}")
        sys.exit(EXIT_CODE_BAD_REQUEST)


def main():
    args = get_args()
    email = args.email
    password = args.password
    card_id = args.card_id
    folder_path = args.dest_folder_path
    domo_instance = args.domo_instance
    # create auth headers for sending requests
    if args.developer_token:
        auth_headers = create_dev_token_header(args.developer_token)
    else:
        access_token = get_access_token(email, password, domo_instance)
        auth_headers = create_pass_token_header(access_token)

    card = get_card_data(card_id, auth_headers, domo_instance)[0]
    # export if card type is 'document'
    if card['type'] == "document":
        export_document_to_file(card_id,
                             auth_headers, domo_instance, 
                             folder_path=folder_path)
    else:
        print(f"card type {card_id} not supported by function")
        sys.exit(EXIT_CODE_INCORRECT_CARD_TYPE)


if __name__ == '__main__':
    main()
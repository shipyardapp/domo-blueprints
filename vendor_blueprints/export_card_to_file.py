import json
import sys
import argparse
import requests
import urllib.parse
import shipyard_utils as shipyard


EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_ACCOUNT = 201
EXIT_CODE_BAD_REQUEST = 202
EXIT_CODE_INCORRECT_CARD_TYPE = 203


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--email', dest='client_id', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--domo-instance', dest='domo_instance', required=True)
    parser.add_argument('--card-id', dest='card_id', required=True)
    parser.add_argument('--dest-file-name', dest='dest_file_name', required=True)
    parser.add_argument('--dest-folder-path', 
                        dest='dest_folder_path', 
                        default='',
                        required=True)
    parser.add_argument('--file-type',
                        dest='file_type',
                        choices={'PPT', 'CSV', 'EXCEL'},
                        required=True)
    args = parser.parse_args()
    return args


def get_access_token(email, password, DOMO_INSTANCE):
    """
    Generate Access Token for use with internal content APIs.

    email (str): email address of the user on domo.com
    password (str): login password of the user domo.com
    """
    auth_api = f"https://{DOMO_INSTANCE}.domo.com/api/content/v2/authentication"

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


def get_card_data(card_id, access_token, DOMO_INSTANCE):
    """
    Get metadata and property information of a single card

    card_id (str): The unique ID of the card
    access_token: client access token. Use the get_access_token() function to retrieve the token.

    Returns:
    card_response -> dict with the metadata and details of the card.
    """
    card_info_api = f"https://{DOMO_INSTANCE}.domo.com/api/content/v1/cards?urns={card_id}&parts=metadata,properties&includeFiltered=true"
    card_headers = {
            'Content-Type' : 'application/json',
            'x-domo-authentication': access_token
        }
    card_response = requests.get(url=card_info_api, headers=card_headers)
    return card_response.json()


def export_graph_to_file(card_id, file_name, file_type, 
                         access_token, DOMO_INSTANCE, folder_path=""):
    """
    Exports a file to one of the given file types: csv, ppt, excel
    """
    export_api = f"https://{DOMO_INSTANCE}.domo.com/api/content/v1/cards/{card_id}/export"

    card_headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'accept': 'application/json, text/plain, */*',
            'x-domo-authentication': access_token
    }
    
    # make a dictionary to map user file_type with requested mimetype
    filetype_map = {
        "csv":"text/csv", 
        "excel":"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "ppt": "application/vnd.ms-powerpoint"
    }
    
    body = {
        "queryOverrides":{
            "filters":[],
            "dataControlContext":{
                "filterGroupIds":[]
            }
        },
        "watermark":'true',
        "mobile":'false',
        "showAnnotations":'true',
        "type":"file",
        "fileName": f"{file_name}",
        "accept": filetype_map[file_type]
    }
    # convert body to domo encoded payload
    encoded_body = urllib.parse.quote(f"{body}")
    encoded_body = encoded_body.replace("%27", "%22") # changing " to '
    payload = f"request={encoded_body}"

    export_response = requests.post(url=export_api, data=payload,
        headers=card_headers, stream=True)
    if export_response.status_code == 200:
        destination_folder_name = shipyard.files.clean_folder_name(
            folder_path)
        destination_full_path = shipyard.files.combine_folder_and_file_name(
        folder_name=destination_folder_name, file_name=file_name)
        with open(destination_full_path, 'wb') as fd:
            # iterate through the blob 1MB at a time
            for chunk in export_response.iter_content(1024*1024):
                fd.write(chunk)
        print(f"{file_type} file:{destination_full_path} saved successfully!")
    else:
        print(f"Request failed with status code {export_response.status_code}")
        sys.exit(EXIT_CODE_BAD_REQUEST)


def main():
    args = get_args()
    email = args.email
    password = args.password
    card_id = args.card_id
    file_name = args.dest_file_name
    folder_path = args.dest_folder_path
    file_type = args.file_type
    folder_path = args.folder_path
    DOMO_INSTANCE = args.domo_instance
    ACCESS_TOKEN = get_access_token(email, password, DOMO_INSTANCE)
    # check if the card is of the 'dataset/graph' type
    card = get_card_data(card_id, ACCESS_TOKEN, DOMO_INSTANCE)[0]
    # export if card type is 'graph'
    if card['type'] == "kpi":
        export_graph_to_file(card_id, file_name, file_type,
                             ACCESS_TOKEN, DOMO_INSTANCE, 
                             folder_path=folder_path)
    else:
        print(f"card type {card_id} not supported by system")
        sys.exit(EXIT_CODE_INCORRECT_CARD_TYPE)


if __name__ == '__main__':
    main()
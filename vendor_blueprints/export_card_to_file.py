import json
import re
import argparse
import requests
from pydomo import Domo
import shipyard_utils as shipyard


EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_ACCOUNT = 201
EXIT_CODE_BAD_REQUEST = 202


domo = Domo('client_id','secret_key',api_host='api.domo.com')
DOMO_INSTANCE = "instance_ID"


def get_access_token(email, password):
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
		auth_response = requests.post(auth_api, data=auth_body, headers=auth_headers)
	except Exception as e:
		print("Request error: {e}")
		sys.exit(EXIT_CODE_BAD_REQUEST)

	if auth_response.json()["success"] is False: # Failed to login
		print(f"Authentication failed due to reason: {auth_response['reason']}")
		sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
		
	# else if the authentication succeeded
	domo_token = auth_response.json()['sessionToken']
	return domo_token


def get_all_card_ids():
	"""
	Retrieve a list of all the card id's associated with a user's dashboard.

	Returns:
	card_id  -> list with all the card_ids
	"""
	pages = domo.pages
	card_ids = []
	for page in pages.list():
		# get page data
		page_data = pages.get(page['id'])
		cards = page_data['cardIds']
		card_ids += cards

	return card_ids


def get_card_data(card_id, access_token):
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


def get_card_id_from_name(name, access_token):
	"""
	Retrieve the particular Card Id of a card given the name
	"""
	cards = get_all_card_ids()
	for card_id in cards:
		card = get_card_data(card_id, access_token)
		card_name = card[0]['title']
		# check if name matches with card name (ignore all caps)
		if re.search(name, card_name, re.IGNORECASE):
			return card_id

			
def export_document_card(card_id, access_token, filename):
	# grab the document id from card metadata
	card = get_card_data(card_id, access_token)[0]
	document_id = card['metadata']['revisionId']
	file_type = card['mimeType']
	file_download_api = f"https://{DOMO_INSTANCE}.domo.com/api/data/v1/data-files/{document_id}/revisions/{document_id}?fileName=temp.jpg"
	if "image" in file_type:
		image_ext = filet_type.replace("image/", "")
		# download image file
	elif "csv" in file_type:
		# download csv
	
	elif "excel" in filet_type:
		# download excel

	else:
		# generic file download 

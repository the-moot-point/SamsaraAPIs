import requests
import os
import json

url = "https://api.samsara.com/fleet/drivers"

payload = {
    "externalIds": {"EncompassID": "16584"},
    "tagIds": ["2762148", "4134370", "2762147"],
    "password": "jeco2023",
    "name": "Trevion Barlow",
    "notes": "Created by API",
    "peerGroupTagId": "4134370",
    "licenseState": "TX",
    "eldExempt": True,
    "eldExemptReason": "Short Haul",
    "username": "tbarlow"
}

# Fetch the authorization key from environment variables
auth_key = os.getenv('SAMSARA_API_KEY')

if not auth_key:
    raise Exception("No authorization key found")

headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Bearer {auth_key}"
}

try:
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")
    # Handle error
else:
    print(f"Request was successful. Response: {json.dumps(response.json(), indent=4)}")

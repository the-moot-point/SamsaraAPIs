import csv
import logging
import requests
import time
import configparser
from collections import Counter
import os

logging.basicConfig(filename='errors.log', level=logging.ERROR, format='%(asctime)s %(levelname)s:%(message)s')

config = configparser.ConfigParser()
config.read('config.ini')

SAMSARA_API_ENDPOINT = config.get('API', 'samsara_endpoint')


def get_addresses_from_csv(filename):
    addresses = []
    with open(filename, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            addresses.append(row)
    return addresses


def add_address_to_samsara(name, address, api_call_outcomes):
    headers = {
        "Authorization": f"Bearer {os.getenv('SAMSARA_API_KEY')}",
        "Content-Type": "application/json"
    }
    body = {
        'tagIds': [address['Location']],
        'formattedAddress': address['Report Address'],
        'latitude': float(address['Latitude']),
        'longitude': float(address['Longitude']),
        'name': name,
    }

    for _ in range(3):  # retry up to 3 times
        response = None
        try:
            response = requests.post(SAMSARA_API_ENDPOINT, headers=headers, json=body, timeout=5)  # 5 seconds timeout
            response.raise_for_status()  # This will raise an HTTPError if the response status code indicates an error
            api_call_outcomes["successful"] += 1
            logging.info(f"Successfully added address for '{name}'.")
        except requests.exceptions.Timeout:
            logging.error(f"Request timed out for {name}.")
            api_call_outcomes["failed"] += 1
        except requests.exceptions.HTTPError as e:
            if response and response.status_code == 429:  # Too many requests
                logging.error(f"Too many requests. Sleeping for 60 seconds.")
                time.sleep(60)
                api_call_outcomes["failed"] += 1
            elif response and response.status_code >= 500:  # server error
                logging.error(f"Server error ({response.status_code}): {e}. Retrying in 5 seconds.")
                time.sleep(5)
            else:
                logging.error(f"Request failed: {e}")
                api_call_outcomes["failed"] += 1
                break
        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred while making the API request: {e}")
            api_call_outcomes["failed"] += 1
        else:
            break  # if no exception was raised, break the loop


def main():
    logging.info("Program started")
    api_call_outcomes = Counter()
    samsara_addresses = get_addresses_from_csv(config.get('FILES', 'samsara_csv'))
    encompass_addresses = get_addresses_from_csv(config.get('FILES', 'encompass_csv'))

    for address in encompass_addresses:
        name = address['Customer Name']
        if name not in [addr['Customer Name'] for addr in samsara_addresses]:
            add_address_to_samsara(name, address, api_call_outcomes)

    logging.info(f"Program ended. Total addresses processed: {len(encompass_addresses)}. "
                 f"Successful API calls: {api_call_outcomes['successful']}. "
                 f"Failed API calls: {api_call_outcomes['failed']}.")


if __name__ == '__main__':
    main()

import pandas as pd
import logging
import requests
import configparser
import os
from collections import Counter
import re

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
config = configparser.ConfigParser()
config.read('config.ini')

ENCOMPASS_CSV = config.get('FILES', 'encompass_csv')
SAMSARA_CSV = config.get('FILES', 'samsara_csv')
SAMSARA_API_ENDPOINT = config.get('API', 'samsara_endpoint')

headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "Authorization": f"Bearer {os.getenv('SAMSARA_API_KEY')}"
}


def deactivate_driver_samsara(driver_id):
    url = f"{SAMSARA_API_ENDPOINT}/fleet/drivers/{driver_id}"

    payload = {
        "usDriverRulesetOverride": {
            "cycle": "USA Property (8/70)",
            "restart": "34-hour Restart",
            "restbreak": "Property (off-duty/sleeper)",
            "usStateToOverride": ""
        },
        "deactivatedAtTime": "current time",
        "driverActivationStatus": "deactivated"
    }

    try:
        response = requests.patch(url, json=payload, headers=headers)
        response.raise_for_status()
        logging.info(f"Successfully deactivated driver with ID: {driver_id}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to deactivate driver with ID {driver_id}: {e}")


def main():
    logging.info("Program started")

    api_call_outcomes = Counter()

    try:
        encompass_df = pd.read_csv(ENCOMPASS_CSV)
        samsara_df = pd.read_csv(SAMSARA_CSV)
    except FileNotFoundError as e:
        logging.error(f"An error occurred while loading the CSV files: {e}")
        return
    except pd.errors.ParserError as e:
        logging.error(f"An error occurred while parsing the CSV files: {e}")
        return

    inactive_encompass_df = encompass_df[encompass_df['Active'] == 'Inactive']

    for _, row in inactive_encompass_df.iterrows():
        # Remove '_x' or '_X' from the name and username, ignoring case
        full_name = re.sub('_x', '', row['Full Name'], flags=re.IGNORECASE)
        user_name = re.sub('_x', '', row['User Name'], flags=re.IGNORECASE)

        # Find the respective driver in the Samsara CSV
        samsara_driver_row = samsara_df[(samsara_df['Full Name'] == full_name) & (samsara_df['User Name'] == user_name)]

        if not samsara_driver_row.empty:
            driver_id = samsara_driver_row.iloc[0]['Driver ID']
            deactivate_driver_samsara(driver_id)

    logging.info(f"Program ended. Total drivers processed: {len(inactive_encompass_df)}. "
                 f"Successful API calls: {api_call_outcomes['successful']}. "
                 f"Failed API calls: {api_call_outcomes['failed']}.")


if __name__ == "__main__":
    main()

import pandas as pd
import requests
import logging
from collections import Counter
import configparser
import os

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
config = configparser.ConfigParser()
config.read('config.ini')

ENCOMPASS_CSV = config.get('FILES', 'encompass_csv')
SAMSARA_CSV = config.get('FILES', 'samsara_csv')
SAMSARA_API_ENDPOINT = config.get('API', 'samsara_endpoint')


def add_driver_to_samsara(driver_data, api_call_outcomes):
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Bearer {os.getenv('SAMSARA_API_KEY')}"  # Assuming API key is set as an environment variable
    }

    try:
        response = requests.post("https://api.samsara.com/fleet/drivers", json=driver_data, headers=headers)
        response.raise_for_status()
        api_call_outcomes["successful"] += 1
        logging.info(f"Successfully added driver: {driver_data['name']}")
    except requests.exceptions.RequestException as e:
        api_call_outcomes["failed"] += 1
        logging.error(f"Failed to add driver {driver_data['name']}: {e}")


def main():
    logging.info("Program started")

    # Define the mapping from Companies to Locations
    company_mapping = {
        'Matador': ['Westpark', 'Lawrenceville', 'Cartersville', 'North Carolina', 'Conyers', 'Rossville'],
        'Chaparral': ['Austin', 'Austin - North'],
        'Viva': ['Amarillo', 'Abilene', 'Midland', 'El Paso', 'Lubbock', 'San Angelo']
    }
    # Define the mapping from Roles to third peer group tag
    peer_group = {
        'Manager': ['District Manager-2016', 'Market Manager-2016', 'Operations Level 3 2022', 'Sales Level 3 2022',
                    'Sales Level 4 2022', 'Warehouse Manager - 2016'],
        'Driver': ['Sales Level 2 2022', 'Sales Level 1 2022', 'Merchandiser - 2016'],
        'Warehouse': ['Operations Level 1 2022', 'Operations Level 2 2022', 'Warehouse Associate - 2016']
    }
    license_state_mapping = {'Viva': 'TX', 'Chaparral': 'TX', 'North Carolina': 'NC'}
    default_license_state = 'GA'

    api_call_outcomes = Counter()

    try:
        encompass_df = pd.read_csv(ENCOMPASS_CSV, usecols=['Full Name', 'Location', 'Active', 'User Name', 'Mobile'])
        samsara_df = pd.read_csv(SAMSARA_CSV)
    except FileNotFoundError as e:
        logging.error(f"An error occurred while loading the CSV files: {e}")
        return
    except pd.errors.ParserError as e:
        logging.error(f"An error occurred while parsing the CSV files: {e}")
        return

    active_encompass_df = encompass_df[encompass_df['Active'] == 'Active']
    samsara_drivers = set(samsara_df['Full Name'])
    drivers_processed = len(active_encompass_df)

    for _, row in active_encompass_df.iterrows():
        if row['Full Name'] not in samsara_drivers:
            company = next((comp for comp, locations in company_mapping.items() if row['Location'] in locations), None)

            if company is None:
                logging.warning(f'Unknown location: {row["Location"]}')
                continue

            licenseState = license_state_mapping.get(company, default_license_state)

            # Prepare the payload for the API call
            payload = {
                "tagIds": [company, row['Location'], peer_group.get("Driver")],
                "usDriverRulesetOverride": {  # Does this even need to be here?
                    "cycle": "USA Property (8/70)",  # I know this part isn't correct
                    "restart": "34-hour Restart",  # I know this part isn't correct
                    "restbreak": "Property (off-duty/sleeper)",  # I know this part isn't correct
                    "usStateToOverride": ""
                },
                "eldExempt": True,
                "eldExemptReason": "Short Haul",
                "licenseState": licenseState,
                "name": row['Full Name'],
                "peerGroupTagId": peer_group.get("Driver"),
                "username": row['User Name'],
                "phone": row['Mobile'],
                "password": "Jeco2023"
            }

            add_driver_to_samsara(payload, api_call_outcomes)

    logging.info(
        f"Program ended. Total drivers processed: {drivers_processed}. Successful "
        f"API calls: {api_call_outcomes['successful']}. Failed API calls: {api_call_outcomes['failed']}.")


if __name__ == "__main__":
    main()

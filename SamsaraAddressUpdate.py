import requests
import pandas as pd
import csv
from datetime import datetime


def get_addresses_from_html(url):
    tables = pd.read_html(url)
    return tables[0]


def map_location_to_tagid(location):
    location_tagid_mapping = {
        "Abilene": "2762144",
        "Amarillo": "2762143",
        "Austin": "2762148",
        "Austin - North": "2762149",
        "Byron": "2762160",
        "Cartersville": "2762162",
        "Conyers": "3516042",
        "Lawrenceville": "2762163",
        "Lubbock": "2762142",
        "Midland": "2568656",
        "North Carolina": "2762164",
        "Rossville": "2762165",
        "San Angelo": "2762145",
        "Westpark": "2762161"
    }
    return location_tagid_mapping.get(location, "")


def update_address_in_samsara(address, api_key):
    url = "https://api.samsara.com/addresses"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {api_key}"
    }
    payload = {
        "geofence": {"circle": {
            "radiusMeters": 100,
            "latitude": address["Latitude"] if pd.notnull(address["Latitude"]) else None,
            "longitude": address["Longitude"] if pd.notnull(address["Longitude"]) else None
        }},
        "tagIds": [map_location_to_tagid(address["Location"])],
        "formattedAddress": address["Report Address"],
        "latitude": address["Latitude"] if pd.notnull(address["Latitude"]) else None,
        "longitude": address["Longitude"] if pd.notnull(address["Longitude"]) else None,
        "name": address["Customer Name"],
        "notes": "Made by API"
    }
    response = requests.post(url, json=payload, headers=headers)
    return response.text


def log_to_csv(filename, address):
    with open(filename, 'a', newline='') as csvfile:
        fieldnames = ['Customer Name', 'Formatted Address', 'Latitude', 'Longitude', 'Date Entered']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow({
            'Customer Name': address['Customer Name'],
            'Formatted Address': address['Report Address'],
            'Latitude': address['Latitude'] if pd.notnull(address["Latitude"]) else '',
            'Longitude': address['Longitude'] if pd.notnull(address["Longitude"]) else '',
            'Date Entered': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })


def main():
    api_key = "samsara_api_DlROBXu3fMbwvO31NUJkDEaPxrEKrF"
    url = "https://jecdist.com/Home?DashboardID=100120&ReportID=15380765"
    log_filename = "C:\\Users\\sgtjo\\Downloads\\samsara_log_file.csv"
    addresses_table = get_addresses_from_html(url)

    # Load the log file into a DataFrame
    try:
        log_df = pd.read_csv(log_filename)
    except FileNotFoundError:
        log_df = pd.DataFrame(columns=['Customer Name', 'Formatted Address', 'Latitude', 'Longitude', 'Date Entered'])

    for _, address in addresses_table.iterrows():
        # If the customer name is already in the log, skip this address
        if address['Customer Name'] in log_df['Customer Name'].values:
            continue

        response_text = update_address_in_samsara(address, api_key)
        print(response_text)
        log_to_csv(log_filename, address)


if __name__ == "__main__":
    main()

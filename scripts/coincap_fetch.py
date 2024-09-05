import pandas as pd
import requests
import logging
import json
import s3fs

logging.basicConfig(
    filename='s3://coincap-airflow-etl-s3/error_logs.log',
    filemode='a',         # Append mode
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)


def fetch_coincap_data():
    # Function to get Crypto data
    try:
        url = 'http://api.coincap.io/v2/assets'
        headers = {
            "Content-Type": "application/json",
            "Accept-Encoding": "deflate"
        }
        response = requests.get(url, headers=headers)

        responseData = response.json()

        with open('crypto_raw_data.json', 'w') as json_file:
            json.dump(responseData, json_file)

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

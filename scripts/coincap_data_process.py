import pandas as pd
import json
import s3fs
import logging

logging.basicConfig(
    filename='s3://coincap-airflow-etl-s3/error_logs.log',
    filemode='a',         # Append mode
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)


def process_coincap_data():

    try:
        with open('crypto_raw_data.json', 'r') as json_file:
            responseData = json.load(json_file)

        df = pd.json_normalize(responseData, 'data')

        cols_to_remove = ['marketCapUsd', 'volumeUsd24Hr', 'vwap24Hr']
        df_filtered = df.drop(columns=cols_to_remove)

        cols_to_round = ['supply', 'maxSupply',
                         'priceUsd', 'changePercent24Hr']
        df_filtered[cols_to_round] = df_filtered[cols_to_round].astype(
            float).round(2)

        df_filtered.to_csv(
            's3://coincap-airflow-etl-s3/crypto_filtered_data.csv', index=False)

    except FileNotFoundError as e:
        logging.error(f"Error: {e} 'crypto_raw_data.json' not found.")

    except ValueError as e:
        print(f"Unable to process data: {e}")

    except Exception as e:
        print(f"An error occurred: {e}")

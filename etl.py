import re
import os
import logging
import shutil
import requests
import argparse
import pandas as pd

# region tools
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = None
parser = argparse.ArgumentParser("crunchbaseParser")
parser.add_argument("-t", "--token", help="Provide your Crunchbase API key")

# endregion

# region constants
CRUNCHBASE_API_KEY = os.environ['CRUNCHBASE_API_KEY']
CRUNCHBASE_URL = f'https://api.crunchbase.com/bulk/v4/bulk_export.tar.gz?user_key={CRUNCHBASE_API_KEY}'
CRUNCHBASE_ORG_URL = f'https://api.crunchbase.com/bulk/premium/v4/organizations.csv?user_key={CRUNCHBASE_API_KEY}'

HOME = os.environ['HOME']
TMP_DIR = os.path.join(HOME, 'crunchbase_temp')
FILE_NAME = os.path.join(TMP_DIR, 'crunchbase.tar.gz')
CSV_DIR = os.path.join(TMP_DIR, 'crunchbase_csvs')
# endregion


def download_crunchbase_file():
    os.mkdir(TMP_DIR)
    chunk_count: int = 0
    
    with requests.get(CRUNCHBASE_URL, stream=True) as r:
        r.raise_for_status()
        with open(FILE_NAME, 'wb') as f:
            for chunk in r.iter_content():
                chunk_count += 1
                logger.info("Working on chunk: {0}".format(i))
                f.write(chunk)

def custom_function(df: pd.DataFrame)
    """
    :param df: The dataframe created in the write_csvs_to_custom_dest
    :type pd.DataFrame

    :returns: based on the user decision, the original usage of it is to upload objects to GBQ
    :rtype: Union of all?"""
    pass
    

def write_csvs_to_custom_dest() -> Dataframe:
    shutil.unpack_archive(filename=FILE_NAME,
                          extract_dir=CSV_DIR)

    for csv in os.listdir(CSV_DIR):
        table_name = csv.split('.')[0]
        csv_path = os.path.join(CSV_DIR, csv)

        if table_name == 'organizations':
            csv_path = CRUNCHBASE_ORG_URL

        logging.info("Working on table name {0} with the CSV path of: {1}".format(table_name,csv_path))

        curr_df = pd.read_csv(csv_path)
        str_cols = curr_df.select_dtypes('object').columns
        curr_df[str_cols] = curr_df[str_cols].replace(to_replace=r'\n|\t|\r',
                                                      value='',
                                                      regex=True)

        etl_timestamp = pd.to_datetime('today').normalize()
        curr_df.insert(loc=0, column='timestamp', value=etl_timestamp)

        """
        Here the dataframe is being populated and getting bigger as we're CB data
        You can decide do whatever you want with this object once finished iterating the files
        My suggestion is to store in some data warehouse and upload it gradually (GBQ preferably)
        Can be done with importing pandas_gbq, google.oauth2 -> and use to_gbq.
        """


def delete_files():
    logger.debug("Starting to delete the created CSV files")
    for csv in os.listdir(CSV_DIR):
        csv_path = os.path.join(CSV_DIR, csv)
        os.remove(csv_path)
    os.rmdir(CSV_DIR)
    os.remove(FILE_NAME)
    os.rmdir(TMP_DIR)
    logger.debug("Finished deleting all CSV files")


def run_etl():
    global logger
    global CRUNCHBASE_API_KEY

    args = parser.parse_args()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if args.token:
        CRUNCHBASE_API_KEY=args.token.replace(" ","")
  
    logger.info("Starting the ETL process...")
    try:
        download_crunchbase_file()
        write_csvs_to_custom_dest()
        delete_files()

    except Exception as e:
        logger.error("Encountered an error when trying to execute the custom crunchbase ETL, Errors: {0}".format(str(e)))
        delete_files()


if __name__ == '__main__':
    run_etl()

"""
Main Script for LiveIQ Data Processing and Uploading

This script automates the process of downloading an Excel file from LiveIQ,
processing the data, and then uploading the processed data to Airtable and AWS S3.

The script uses AWS Secrets Manager to securely retrieve credentials for LiveIQ,
and environment variables for configuration settings for Airtable and AWS S3.

Requirements:
- AWS CLI configured with access to the required AWS Secrets Manager secret.
- Environment variables set for Airtable API key, base ID, table name, and S3 bucket name.
- Python packages: boto3, pandas, selenium, airtable-python-wrapper, python-dotenv.

Usage:
Run the script in a Python environment where all dependencies have been installed:
$ python main.py
"""

import logging # Import the logging module
import os # Import the os module
import tempfile
from crawler.crawler import download_excel # Import the download_excel function from the crawler module
from processor.processor import process_excel # Import the process_excel function from the processor module
from airtable_module.airtable import upload_to_airtable # Import the upload_to_airtable function from the airtable_module within the airtable package
from aws.s3 import upload_file_to_s3 # Import the upload_file_to_s3 function from the s3 module within the aws package
from jacal_secrets.secrets_manager import get_secret # Import the get_secret function from the secrets_manager module within the secrets package
from dotenv import load_dotenv # Import the load_dotenv function from the dotenv package
load_dotenv() # Load the .env file  (environment variables) into the script

def setup_logging():
    # Create a logger
    logger = logging.getLogger('my_app')
    logger.setLevel(logging.DEBUG)  # Set the logging level

    # Create a console handler that logs debug messages
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create a file handler that logs debug messages
    fh = logging.FileHandler('my_app.log')
    fh.setLevel(logging.DEBUG)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(ch)
    logger.addHandler(fh)

def main(): # Define the main function that will be executed
    setup_logging()
    logger = logging.getLogger('my_app')

    try:
        logger.debug("Starting main function")

        region_name = os.getenv('AWS_REGION') # Define the AWS region name for the Secrets Manager
        # liveiq_secret_name = os.getenv('LIVEIQ_SECRET_NAME', 'liveiq')
        liveiq_secret = get_secret('liveiq', region_name) # Retrieve the LiveIQ secret (username and password) from AWS Secrets Manager
        logger.debug("Retrieved LiveIQ secret")

        excel_file_path = os.getenv('EXCEL_FILE_PATH')
        if excel_file_path:
            excel_file = excel_file_path
        else:
            download_path = tempfile.mkdtemp() # Define the path where the downloaded Excel file will be stored
            max_attempts = 5
            for attempt in range(max_attempts):
                try:
                    excel_file = download_excel(download_path, liveiq_secret) # Call the download_excel function to download the Excel file using the LiveIQ credentials
                    break
                except Exception as e:
                    if attempt < max_attempts - 1:
                        logger.warning(f"Attempt {attempt + 1} failed: {e}")
                        continue
                    else:
                        logger.error(f"Failed to download Excel file: {e}")
                        raise

        logger.info("Processing Excel file")
        processed_data = process_excel(excel_file) # Process the downloaded Excel file and store the processed data
        processed_data = processed_data.fillna("")  # Replace NaN with empty strings

        # Define the API key, base ID, and table name for Airtable
        airtable_api_key = os.getenv('AIRTABLE_API_KEY')
        airtable_base_id = os.getenv('AIRTABLE_BASE_ID')
        airtable_table_name = os.getenv('AIRTABLE_TABLE_NAME')

        logger.debug("Uploading data to Airtable")
        try:
            upload_to_airtable(airtable_api_key, airtable_base_id, airtable_table_name, processed_data) # Upload the processed data to Airtable
        except Exception as e:
            logger.error(f"Failed to upload data to Airtable: {e}")
            raise

        

    except Exception as e:
        logger.exception("An error occurred")

    logger.info("Finished main function")

if __name__ == "__main__":
    main()
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import glob
import logging
import tempfile
from dotenv import load_dotenv

def get_latest_file(download_path, file_extension):
    list_of_files = glob.glob(os.path.join(download_path, f"*.{file_extension}")) 
    if not list_of_files:  
        return None
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file

def download_excel(download_path, secret):
    logger = logging.getLogger('my_app')
    logger.debug("Starting the download process.")
    downloaded_file_path = None

    options = Options()  
    options.add_experimental_option("prefs", {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    logger.debug("WebDriver initialized.")
    driver = webdriver.Chrome(options=options)  

    try:
        driver.get("https://liveiq.subway.com/")
        logger.debug("Navigated to the login page.")

        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "signInName")))
        logger.info("Logging in.")
        driver.find_element(By.ID, "signInName").send_keys(secret['username'])

        driver.find_element(By.ID, "password").send_keys(secret['password'])

        driver.find_element(By.ID, "next").click()

        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "page-title")))
        logger.info("Logged in successfully. Navigating to the download page.")

        driver.get("https://liveiq.subway.com/Labour/EmployeeExport")

        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "exportEmployees")))
        driver.find_element(By.ID, "exportEmployees").click()

        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="noPayrollNumbers"][@class="white-popup mfp-with-anim"]')))
            driver.find_element(By.ID, "validateOkBtn").click()
        except:
            print("No popup appeared.")

        logger.info("Waiting for the file to download.")
        time.sleep(10)  

        downloaded_file_path = get_latest_file(download_path, "xlsx")

    except Exception as e:
        logger.exception("An error occurred during the download process.")

    finally:
        driver.quit()
        logger.debug("WebDriver closed.")

    return downloaded_file_path

def main():
    load_dotenv()  

    username = os.environ.get('LIVEIQ_USERNAME')
    password = os.environ.get('LIVEIQ_PASSWORD')
    secret = {
        "username": username,
        "password": password
    }
    download_path = tempfile.mkdtemp()
    downloaded_file = download_excel(download_path, secret)
    if downloaded_file:
        print(f"Successfully downloaded the Employee Export file")
    else:
        print("No file was downloaded.")

if __name__ == "__main__":
    main()
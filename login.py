from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from azure.storage.blob import BlobServiceClient
import json
from io import BytesIO
import time
from selenium import webdriver
from datetime import datetime
from li_scraper_main import (LIScraper_SSL,
                             decisionMakers,
                             LIScraper_profile,
                             company_scraping,
                             LIScraper_profile_detail,
                             cookie_expiry,LIScraper_SSL_blob,
                             LIScraper_SSL_Azure)
from sales_navigator.getSavedSearches import getSavedSearches
from config import service_bus_str
from config import queue_name
from flask import Flask
from middleware import Middleware
from flask import request
from datetime import datetime
import time
import gc
import threading
from azure.servicebus import ServiceBusClient
import json
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
from flask import jsonify
from py_elastic_logs import send_log_to_elasticsearch
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from datetime import datetime
import gc

app = Flask(__name__)
app.wsgi_app = Middleware(app.wsgi_app)


def perform_login(username_int,password_int,login_type):

    try:

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        driver = webdriver.Chrome(options=chrome_options)

        if login_type == 'linkedin':
            driver.get("https://www.linkedin.com/login")
            time.sleep(3)
            # LinkedIn login code here
            wait = WebDriverWait(driver, 120)
            username = wait.until(EC.visibility_of_element_located((By.ID, "username")))
            username.send_keys(username_int)
            password = wait.until(EC.visibility_of_element_located((By.ID, "password")))
            password.send_keys(password_int)
            submit_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@type='submit']")))
            submit_button.click()
            wait.until(EC.url_matches("https://www.linkedin.com/feed/"))
        elif login_type == 'sales_navigator':
            driver.get("https://www.linkedin.com/sales?trk=d_flagship3_nav&")
            time.sleep(3)
            # Sales Navigator login code here
            wait = WebDriverWait(driver, 120)
            username = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#username")))
            username.send_keys(username_int)
            password = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#password")))
            password.send_keys(password_int)
            submit_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div.login__form_action_container > button.btn__primary--large")))
            submit_button.click()
            wait.until(EC.url_matches("https://www.linkedin.com/sales/home"))
        else:
            raise ValueError("Invalid login type")

        cookies = driver.get_cookies()

        # Print information about each cookie
        for cookie in cookies:
            name = cookie['name']
            value = cookie['value']
            expiry = cookie.get('expiry', None)

            if expiry is not None:
                # Convert the timestamp to a human-readable format
                expiry_time = datetime.fromtimestamp(expiry)
                print(f"Cookie '{name}' with value '{value}' will expire on {expiry_time}")
            else:
                print(f"Cookie '{name}' with value '{value}' does not have an expiration time")

        container_name = "cookiesli" if login_type == 'linkedin' else "cookiesli"
        account_name = "mbaimlaistoragedev"
        account_key = "55eW6JgNdsqUhbLrRDSeUo9jpys/Dh9srD8VZLgz0ltWhHJnvy0NufwOOg5J5f5UHFVxK9UeapFL+ASt1qjDVg=="
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)


        if len(username_int) > 0:
            if login_type == 'linkedin':
                prefix = 'LI_'
            elif login_type == 'sales_navigator':
                prefix = 'SN_'
            else:
                raise ValueError("Invalid login type")

            blob_name = f"{prefix}{username_int}.json"
        else:
            raise ValueError("Please provide the username")

        blob_client = container_client.get_blob_client(blob_name)
        cookies_json = json.dumps(cookies)
        blob_client.upload_blob(BytesIO(cookies_json.encode()), overwrite=True)
        time.sleep(3)
        driver.close()

        return "Storage cookies successfully created"


    except Exception as e:
        return e


@app.route('/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        username = data.get('userName')
        password = data.get('PassWord')
        login_type = data.get('loginType')

        if username and password and login_type:
            result = perform_login(username, password, login_type)
            return jsonify({'message': result}), 200
        else:
            return jsonify({'error': 'Invalid request. Please provide userName, PassWord, and loginType in JSON.'}), 400

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

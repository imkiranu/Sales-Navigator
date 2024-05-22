from flask import jsonify
import pandas as pd
from selenium.webdriver.support import expected_conditions as ec
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from datetime import datetime
from config import (az_account_key,
                    az_account_name,
                    li_cookies,
                    service_bus_str,lead_json,

                    queue_name)
from py_elastic_logs import send_log_to_elasticsearch
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from azure.storage.blob import BlobServiceClient
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import json
import io
import time

import time


class cookie_expiry:


    def login_with_blob_cookies(self, blob_name):

        blob_name = str(blob_name)

        if blob_name.startswith("LI_"):
            # Your Azure Blob Storage code to read the cookie from the blob
            account_name = az_account_name
            account_key = az_account_key
            connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
            container_name = li_cookies

            # Create a BlobServiceClient
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)

            # Create a ContainerClient
            container_client = blob_service_client.get_container_client(container_name)

            # Create a BlobClient
            blob_client = container_client.get_blob_client(blob_name)

            # Download the JSON file to a temporary object
            download_stream = blob_client.download_blob()
            content = download_stream.readall()

            # Create an in-memory buffer to store the content
            content_buffer = io.BytesIO(content)

            # Parse the JSON content to obtain the cookies
            try:
                cookies = json.loads(content_buffer.getvalue().decode('utf-8'))
            except Exception as e:
                return {
                    "blobName": blob_name,
                    "active": False,
                    "type": str("Linkedin")
                }

            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--incognito")
            chrome_options.add_argument("--window-size=1200x600")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--allow-insecure-localhost")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.page_load_strategy = 'eager'
            # Creating a webdriver instance
            driver = webdriver.Chrome(options=chrome_options)

            # Opening LinkedIn's login page
            driver.get("https://www.linkedin.com/login")

            # Load the obtained cookies into the driver
            for cookie in cookies:
                # You should set the 'domain' attribute for each cookie to ".linkedin.com" before adding it to the driver.
                cookie['domain'] = ".linkedin.com"
                try:
                    driver.add_cookie(cookie)
                except Exception as e:
                    return {
                        "blobName": blob_name,
                        "active": False,
                        "type": str("Linkedin")
                    }

            time.sleep(2)
            home_url = "https://www.linkedin.com/feed/"
            driver.get(home_url)  # Use 'driver.get' to navigate to the home page
            time.sleep(2)

            # Wait for the home page to load dynamically using XPath
            xpath_expression = "/html/body/div[5]/div[3]/div/div/div[2]"
            try:
                element_present = EC.presence_of_element_located((By.XPATH, xpath_expression))
                WebDriverWait(driver, 10).until(element_present)
                driver.quit()
                print("Linkedin cookie working fine")
                return {
                    "blobName": blob_name,
                    "active": True,
                    "type": str("Linkedin")
                }
            except TimeoutException as te:
                driver.quit()
                return {
                    "blobName": blob_name,
                    "active": False,
                    "type": str("Linkedin")
                }

        elif blob_name.startswith("SN_"):
            # Your Azure Blob Storage code to read the cookie from the blob
            account_name = az_account_name
            account_key = az_account_key
            connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
            container_name = li_cookies
            # Create a BlobServiceClient
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)

            # Create a ContainerClient
            container_client = blob_service_client.get_container_client(container_name)

            # Create a BlobClient
            blob_client = container_client.get_blob_client(blob_name)

            # Download the JSON file to a temporary object
            download_stream = blob_client.download_blob()
            content = download_stream.readall()

            # Create an in-memory buffer to store the content
            content_buffer = io.BytesIO(content)

            # Parse the JSON content to obtain the cookies
            try:
                cookies = json.loads(content_buffer.getvalue().decode('utf-8'))
            except Exception as e:
                return {
                    "blobName": blob_name,
                    "active": False,
                    "type": "salesNavigator"
                }

            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--incognito")
            chrome_options.add_argument("--window-size=1200x600")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--allow-insecure-localhost")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.page_load_strategy = 'eager'
            # Creating a webdriver instance
            driver = webdriver.Chrome(options=chrome_options)

            # Opening LinkedIn's login page
            driver.get("https://www.linkedin.com/sales?trk=d_flagship3_nav&")

            # Load the obtained cookies into the driver
            for cookie in cookies:
                # You should set the 'domain' attribute for each cookie to ".linkedin.com" before adding it to the driver.
                cookie['domain'] = ".linkedin.com"
                try:
                    driver.add_cookie(cookie)
                except Exception as e:
                    return {
                        "blobName": blob_name,
                        "active": False,
                        "type": "salesNavigator"
                    }

            time.sleep(2)
            home_url = "https://www.linkedin.com/sales/home"
            driver.get(home_url)  # Use 'driver.get' to navigate to the home page
            time.sleep(2)

            # Wait for the home page to load dynamically using XPath
            xpath_expression = "/html/body/main/div[2]"
            try:
                element_present = EC.presence_of_element_located((By.XPATH, xpath_expression))
                WebDriverWait(driver, 10).until(element_present)
                driver.quit()
                print("Sales Navigator cookie working fine")
                return {
                    "blobName": blob_name,
                    "active": True,
                    "type": "salesNavigator"
                }
            except TimeoutException as te:
                driver.quit()
                return {
                    "blobName": blob_name,
                    "active": False,
                    "type": "salesNavigator"
                }

        else:
            return {
                "blobName": blob_name,
                "active": False,
                "type": "Invalid login type"
            }


class LIScraper_SSL_blob:
    '''
    This class is about fething leads from sales navigator and storing it in streaming manner and sending a message
    '''

    def log_in_to_li_sales_nav(self, saved_search_url,blob_name_sn,campaign_query_id):
        '''

        :param saved_search_url:
        :param blob_name_sn:
        :param campaign_query_id:
        :return:
        '''
        # Your Azure Blob Storage code to read the cookie from the blob
        account_name = az_account_name
        account_key = az_account_key
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        container_name = li_cookies
        blob_name = blob_name_sn # Name of the JSON file to download

        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Create a ContainerClient
        container_client = blob_service_client.get_container_client(container_name)

        # Create a BlobClient
        blob_client = container_client.get_blob_client(blob_name)

        # Download the JSON file to a temporary object
        download_stream = blob_client.download_blob()
        content = download_stream.readall()

        # Create an in-memory buffer to store the content
        content_buffer = io.BytesIO(content)

        # Parse the JSON content to obtain the cookies
        try:
            cookies = json.loads(content_buffer.getvalue().decode('utf-8'))
        except Exception as e:
            # Example log message
            cookie_error = {'message': e, 'level': 'INFO',
                          'timestamp': datetime.utcnow()}
            send_log_to_elasticsearch(cookie_error)
            print("Error parsing cookies:", e)
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--window-size=1200x600")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.page_load_strategy = 'eager'

        # Creating a webdriver instance
        self.driver = webdriver.Chrome(options=chrome_options)

        # Opening LinkedIn's login page
        self.driver.get("https://www.linkedin.com/sales?trk=d_flagship3_nav&")
        time.sleep(2)

        # Load the obtained cookies into the driver
        for cookie in cookies:
            # You should set the 'domain' attribute for each cookie to ".linkedin.com" before adding it to the driver.
            cookie['domain'] = ".linkedin.com"
            try:
                self.driver.add_cookie(cookie)
            except Exception as e:
                # Example log message
                cookie_error = {'message': e, 'level': 'INFO',
                                'timestamp': datetime.utcnow()}
                send_log_to_elasticsearch(cookie_error)
                print("Error adding cookie:", e)


        time.sleep(3)
        home_url = "https://www.linkedin.com/sales/home"
        self.driver.get(home_url) # Use 'driver.get' to navigate to the home page
        # Example log message
        '''login_success = {'message': "Successfully Logged into Sales Navigator", 'level': 'INFO',
                        'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)'''
        print("Successfully Logged into Sales Navigator")

        # Retry loop with a maximum number of attempts - 3
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            self.driver.get(saved_search_url)
            try:
                print("Sales Navigator was successfully loaded using session cookies!!!!!")
                break
            except Exception as e:
                print(f"Attempt {attempt}/{max_attempts} failed. Error: {e}")

                if attempt < max_attempts:
                    print("Retrying Logging into Sales Navigator")
                    # Optionally, you can introduce a delay between retries
                    time.sleep(3)

        time.sleep(3)

        print("Login Successful.")
        # Example log message
        '''login_success = {'message': "Successfully Logged into Sales Navigator", 'level': 'INFO',
                         'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)'''
        time.sleep(5)
        start_scr = time.time()
        self.scroll_to_bottom()
        end_scr = time.time()
        print("Time taken to scroll page 1:", end_scr - start_scr)
        # Example log message
        '''login_success = {'message': "Time taken to scroll page 1:"+ str(end_scr - start_scr), 'level': 'INFO',
                         'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)'''
        num_results = self.get_number_of_results()
        print(f"{num_results} results found")
        number_records_collected = 0

        p = 1
        print("Gathering data for next page")
        print(f"Collecting page {p}")
        time.sleep(5)
        num_to_search = self.count_number_of_people_on_page()
        time.sleep(5)
        df = self.gather_data(num_to_search)
        df_json = df.to_json(orient='records')
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        blob_name = f"{campaign_query_id}-{p}.json"

        # Set Azure Storage Account details
        account_name = az_account_name
        account_key = az_account_key
        container_name = lead_json

        # Connect to Azure Storage Account
        azure_connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)

        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob=blob_name)
        blob_client.upload_blob(df_json, overwrite=True)
        print(f"Successfully uploaded the {p} data to blob")
        df["pageNumber"] = str(p)
        number_records_collected += num_to_search

        # Send the JSON data to Azure Service Bus
        servicebus_client = ServiceBusClient.from_connection_string(service_bus_str)

        queue_name = "leadresponse"

        message = {"campaignQueryId": campaign_query_id,
                   "fileName": blob_name,
                   "totalRecords": num_results,
                   "pageSize": int(len(df)),
                   "timeStamp": timestamp}

        with servicebus_client:
            sender = servicebus_client.get_queue_sender(queue_name)
            with sender:
                message = ServiceBusMessage(json.dumps(message))  # Ensure data is JSON serialized
                sender.send_messages(message)

        while (num_results - number_records_collected) > 0:
            try:
                self.paginate()
                p += 1
                print(f"Collecting page {p}")
                time.sleep(3)
                start_scrol = time.time()
                self.scroll_to_bottom()
                end_scrol = time.time()
                print("Total time taken for scrolling:", end_scrol - start_scrol)
                time.sleep(3)
                num_to_search = self.count_number_of_people_on_page()
                time.sleep(5)
                new_df = self.gather_data(num_to_search)
                newdf_json = new_df.to_json(orient='records')
                timestamp_new = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                blob_name = f"{campaign_query_id}-{p}.json"

                # Set Azure Storage Account details
                account_name = az_account_name
                account_key = az_account_key
                container_name = lead_json

                # Connect to Azure Storage Account
                azure_connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
                blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)

                container_client = blob_service_client.get_container_client(container_name)
                blob_client = container_client.get_blob_client(blob=blob_name)
                blob_client.upload_blob(newdf_json, overwrite=True)
                print(f"Successfully uploaded the {p} data to blob")

                # Send the JSON data to Azure Service Bus
                servicebus_client = ServiceBusClient.from_connection_string(service_bus_str)

                queue_name = "leadresponse"

                message = {"campaignQueryId": campaign_query_id,
                           "fileName": blob_name,
                           "totalRecords": num_results,
                           "pageSize": int(len(new_df)),
                           "timeStamp": timestamp_new}

                with servicebus_client:
                    sender = servicebus_client.get_queue_sender(queue_name)
                    with sender:
                        message = ServiceBusMessage(json.dumps(message))  # Ensure data is JSON serialized
                        sender.send_messages(message)

                time.sleep(3)
                new_df["pageNumber"] = str(p)

                df = pd.concat([df, new_df])
                print("Total records collected:", len(df))
                number_records_collected += num_to_search
            except NoSuchElementException:
                break
        return {
            'campaignQueryId': campaign_query_id,
            'totalRecords': num_results,
            'totalrecords': number_records_collected
        }

    def scroll_to_bottom(self):
        print("scrolling to bottom started")
        time.sleep(5)
        inner_window = self.driver.find_element(By.XPATH, "/html/body/main/div/div[2]/div[2]")
        scroll = 0
        while scroll < 10:
            self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].offsetHeight;", inner_window)
            scroll += 1
            time.sleep(4)
            print("scrolling through section:", scroll)

        try:
            # Locate the parent div after scrolling to the bottom using XPath
            parent_div_xpath = "/html/body/main/div[1]/div/div[2]/div/div[4]"

            parent_div = self.driver.find_element(By.XPATH, parent_div_xpath)

            # Locate the thumbs-up button and click it using XPath
            thumbs_up_button_xpath = "/html/body/main/div[1]/div/div[2]/div/div[4]/div[2]/button[1]"
            thumbs_up_button = parent_div.find_element(By.XPATH, thumbs_up_button_xpath)
            thumbs_up_button.click()

            time.sleep(2)

            # Locate the close button and click it using XPath
            close_pop_xpath = "/html/body/main/div[1]/div/div[2]/div/div[4]/button"
            close_pop = parent_div.find_element(By.XPATH, close_pop_xpath)
            close_pop.click()

        except NoSuchElementException:
            print("Pop up not found. Skipping operation.")

    def gather_data(self, num_results):
        '''

        :param num_results: No of results people found on page
        :return: dataframe of the all the results found on current page,
                pd.DataFrame({"name": names, "position": positions, "timeInRole": time_in_roles,
                             "location": locations, "salesNavigatorUrl": sn_urls,
                             "company": company, "snCompanyUrl": sn_companyid_urls})
        '''
        time.sleep(5)
        names = []
        positions = []
        time_in_roles = []
        locations = []
        sn_urls = []
        company = []
        sn_companyid_urls = []
        for i in range(1, num_results + 1):
            name = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/"
                f"div[1]/div[1]/div/div[2]/div[1]/div[1]/a/span",
            ).text
            names.append(name)

            position = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/"
                f"div[1]/div[1]/div/div[2]/div[2]/span[1]",
            ).text
            positions.append(position)

            time_in_role = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/"
                f"div[1]/div[1]/div/div[2]/div[4]",
            ).text
            time_in_roles.append(time_in_role)

            location = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[3]/span",
            ).text
            locations.append(location)

            sn_url = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[1]/div[1]/a",
            ).get_attribute('href')
            sn_urls.append(sn_url)

            try:
                sn_companyid_url = self.driver.find_element(
                    By.XPATH,
                    f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[2]/a",
                ).get_attribute('href')
                sn_companyid_urls.append(sn_companyid_url)
            except NoSuchElementException:
                sn_companyid_url = ""
                sn_companyid_urls.append(sn_companyid_url)



            try:
                companys = self.driver.find_element(
                    By.XPATH,
                    f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[2]/a",
                ).text
                company.append(companys)
            except NoSuchElementException:
                companys = ""
                company.append(companys)

        return pd.DataFrame({"name": names, "position": positions, "timeInRole": time_in_roles,
                             "location": locations, "salesNavigatorUrl": sn_urls,
                             "company": company, "snCompanyUrl": sn_companyid_urls})

    def paginate(self):
        time.sleep(3)
        print("Going to next page")

        try:
            # Wait for the 'Saved Search' button to be clickable
            paginate = WebDriverWait(self.driver, 10).until(
                ec.element_to_be_clickable(
                    (By.XPATH, "/html/body/main/div[1]/div[2]/div[2]/div/div[4]/div/button[2]")))
            paginate.click()
            print("Successfully going to next page")

        except Exception as EC:
            print("No next button, this is the last page", EC)
            pass



    def get_number_of_results(self):
        time.sleep(3)
        response = self.driver.find_element(
            By.XPATH, "//*[@id='content-main']/div[1]/div[2]/div[1]/div[2]/div/div[3]/span"
        ).text.split(" ")[0]
        print("Number of results:", response)
        if response[-1] == "+":
            return 1000  # upper cap on how many we'll collect
        else:
            return int(response)

    def count_number_of_people_on_page(self):
        print("Counting no of people on the page")
        time.sleep(3)
        a = self.driver.find_element(By.XPATH, "/html/body/main/div/div[2]/div[2]") \
            .get_attribute("innerHTML"). \
            count("circle-entity")
        print(a)

        return a

    def close_driver(self):
        self.driver.quit()

class LIScraper_SSL_Azure:
    def log_in_to_li_sales_nav(self, saved_search_url,blob_name_sn,campaign_query_id):
        # Your Azure Blob Storage code to read the cookie from the blob
        account_name = az_account_name
        account_key = az_account_key
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        container_name = li_cookies
        blob_name = blob_name_sn # Name of the JSON file to download

        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Create a ContainerClient
        container_client = blob_service_client.get_container_client(container_name)

        # Create a BlobClient
        blob_client = container_client.get_blob_client(blob_name)

        # Download the JSON file to a temporary object
        download_stream = blob_client.download_blob()
        content = download_stream.readall()

        # Create an in-memory buffer to store the content
        content_buffer = io.BytesIO(content)

        # Parse the JSON content to obtain the cookies
        try:
            cookies = json.loads(content_buffer.getvalue().decode('utf-8'))
        except Exception as e:
            # Example log message
            cookie_error = {'message': e, 'level': 'INFO',
                          'timestamp': datetime.utcnow()}
            send_log_to_elasticsearch(cookie_error)
            print("Error parsing cookies:", e)
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--window-size=1200x600")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.page_load_strategy = 'eager'

        # Creating a webdriver instance
        self.driver = webdriver.Chrome(options=chrome_options)

        # Opening LinkedIn's login page
        self.driver.get("https://www.linkedin.com/sales?trk=d_flagship3_nav&")
        time.sleep(2)

        # Load the obtained cookies into the driver
        for cookie in cookies:
            # You should set the 'domain' attribute for each cookie to ".linkedin.com" before adding it to the driver.
            cookie['domain'] = ".linkedin.com"
            try:
                self.driver.add_cookie(cookie)
            except Exception as e:
                # Example log message
                cookie_error = {'message': e, 'level': 'INFO',
                                'timestamp': datetime.utcnow()}
                send_log_to_elasticsearch(cookie_error)
                print("Error adding cookie:", e)


        time.sleep(3)
        home_url = "https://www.linkedin.com/sales/home"
        self.driver.get(home_url) # Use 'driver.get' to navigate to the home page
        # Example log message
        '''login_success = {'message': "Successfully Logged into Sales Navigator", 'level': 'INFO',
                        'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)'''
        print("Successfully Logged into Sales Navigator")

        # Retry loop with a maximum number of attempts - 3
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            self.driver.get(saved_search_url)
            try:
                print("Sales Navigator was successfully loaded using session cookies!!!!!")
                break
            except Exception as e:
                print(f"Attempt {attempt}/{max_attempts} failed. Error: {e}")

                if attempt < max_attempts:
                    print("Retrying Logging into Sales Navigator")
                    # Optionally, you can introduce a delay between retries
                    time.sleep(3)

        time.sleep(3)

        print("Login Successful.")
        # Example log message
        '''login_success = {'message': "Successfully Logged into Sales Navigator", 'level': 'INFO',
                         'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)'''
        time.sleep(5)
        start_scr = time.time()
        self.scroll_to_bottom()
        end_scr = time.time()
        print("Time taken to scroll page 1:", end_scr - start_scr)
        # Example log message
        '''login_success = {'message': "Time taken to scroll page 1:"+ str(end_scr - start_scr), 'level': 'INFO',
                         'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)'''
        num_results = self.get_number_of_results()
        print(f"{num_results} results found")
        number_records_collected = 0

        p = 1
        print("Gathering data for next page")
        print(f"Collecting page {p}")
        time.sleep(5)
        num_to_search = self.count_number_of_people_on_page()
        time.sleep(5)
        df = self.gather_data(num_to_search)
        df_as_dict = df.to_dict(orient='records')
        # Prepare the data for Azure Service Bus
        message_data = {
            'campaignQueryId': campaign_query_id,
            'records': df_as_dict,
            'pageNumber': str(p),
            'totalRecords': int(num_results),
            'pageSize': int(len(df))
        }

        # Convert the message data to JSON
        json_data = json.dumps(message_data)

        # Send the JSON data to Azure Service Bus
        servicebus_client = ServiceBusClient.from_connection_string(service_bus_str)
        with servicebus_client:
            sender = servicebus_client.get_queue_sender(queue_name)
            with sender:
                message = ServiceBusMessage(json_data)
                sender.send_messages(message)

        df["pageNumber"] = str(p)
        number_records_collected += num_to_search

        while (num_results - number_records_collected) > 0:
            try:
                self.paginate()
                p += 1
                print(f"Collecting page {p}")
                time.sleep(3)
                start_scrol = time.time()
                self.scroll_to_bottom()
                end_scrol = time.time()
                print("Total time taken for scrolling:", end_scrol - start_scrol)
                time.sleep(3)
                num_to_search = self.count_number_of_people_on_page()
                time.sleep(5)
                new_df = self.gather_data(num_to_search)
                newdf_as_dict = new_df.to_dict(orient='records')
                # Prepare the data for Azure Service Bus
                message_data_new = {
                    'campaignQueryId': campaign_query_id,
                    'records': newdf_as_dict,
                    'pageNumber': str(p),
                    'totalRecords': int(num_results),
                    'pageSize': int(len(new_df))
                }

                # Convert the message data to JSON
                json_data_new = json.dumps(message_data_new)

                # Send the JSON data to Azure Service Bus
                servicebus_client = ServiceBusClient.from_connection_string(service_bus_str)
                with servicebus_client:
                    sender = servicebus_client.get_queue_sender(queue_name)
                    with sender:
                        message = ServiceBusMessage(json_data_new)
                        sender.send_messages(message)

                time.sleep(3)
                new_df["pageNumber"] = str(p)

                df = pd.concat([df, new_df])
                print("Total records collected:", len(df))
                number_records_collected += num_to_search
            except NoSuchElementException:
                break

        return df

    def scroll_to_bottom(self):
        print("scrolling to bottom started")
        time.sleep(5)
        inner_window = self.driver.find_element(By.XPATH, "/html/body/main/div/div[2]/div[2]")
        scroll = 0
        while scroll < 10:
            self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].offsetHeight;", inner_window)
            scroll += 1
            time.sleep(4)
            print("scrolling through section:", scroll)

        try:
            # Locate the parent div after scrolling to the bottom using XPath
            parent_div_xpath = "/html/body/main/div[1]/div/div[2]/div/div[4]"

            parent_div = self.driver.find_element(By.XPATH, parent_div_xpath)

            # Locate the thumbs-up button and click it using XPath
            thumbs_up_button_xpath = "/html/body/main/div[1]/div/div[2]/div/div[4]/div[2]/button[1]"
            thumbs_up_button = parent_div.find_element(By.XPATH, thumbs_up_button_xpath)
            thumbs_up_button.click()

            time.sleep(2)

            # Locate the close button and click it using XPath
            close_pop_xpath = "/html/body/main/div[1]/div/div[2]/div/div[4]/button"
            close_pop = parent_div.find_element(By.XPATH, close_pop_xpath)
            close_pop.click()

        except NoSuchElementException:
            print("Pop up not found. Skipping operation.")

    def gather_data(self, num_results):
        time.sleep(5)
        names = []
        positions = []
        time_in_roles = []
        locations = []
        sn_urls = []
        company = []
        sn_companyid_urls = []
        for i in range(1, num_results + 1):
            name = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/"
                f"div[1]/div[1]/div/div[2]/div[1]/div[1]/a/span",
            ).text
            names.append(name)

            position = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/"
                f"div[1]/div[1]/div/div[2]/div[2]/span[1]",
            ).text
            positions.append(position)

            time_in_role = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/"
                f"div[1]/div[1]/div/div[2]/div[4]",
            ).text
            time_in_roles.append(time_in_role)

            location = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[3]/span",
            ).text
            locations.append(location)

            sn_url = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[1]/div[1]/a",
            ).get_attribute('href')
            sn_urls.append(sn_url)

            try:
                sn_companyid_url = self.driver.find_element(
                    By.XPATH,
                    f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[2]/a",
                ).get_attribute('href')
                sn_companyid_urls.append(sn_companyid_url)
            except NoSuchElementException:
                sn_companyid_url = ""
                sn_companyid_urls.append(sn_companyid_url)



            try:
                companys = self.driver.find_element(
                    By.XPATH,
                    f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[2]/a",
                ).text
                company.append(companys)
            except NoSuchElementException:
                companys = ""
                company.append(companys)

        return pd.DataFrame({"name": names, "position": positions, "timeInRole": time_in_roles,
                             "location": locations, "salesNavigatorUrl": sn_urls,
                             "company": company, "snCompanyUrl": sn_companyid_urls})

    def paginate(self):
        time.sleep(3)
        print("Going to next page")

        try:
            # Wait for the 'Saved Search' button to be clickable
            paginate = WebDriverWait(self.driver, 10).until(
                ec.element_to_be_clickable(
                    (By.XPATH, "/html/body/main/div[1]/div[2]/div[2]/div/div[4]/div/button[2]")))
            paginate.click()
            print("Successfully going to next page")

        except Exception as EC:
            print("No next button, this is the last page", EC)
            pass



    def get_number_of_results(self):
        time.sleep(3)
        response = self.driver.find_element(
            By.XPATH, "//*[@id='content-main']/div[1]/div[2]/div[1]/div[2]/div/div[3]/span"
        ).text.split(" ")[0]
        print("Number of results:", response)
        if response[-1] == "+":
            return 1000  # upper cap on how many we'll collect
        else:
            return int(response)

    def count_number_of_people_on_page(self):
        print("Counting no of people on the page")
        time.sleep(3)
        a = self.driver.find_element(By.XPATH, "/html/body/main/div/div[2]/div[2]") \
            .get_attribute("innerHTML"). \
            count("circle-entity")
        print(a)

        return a

    def close_driver(self):
        self.driver.quit()

class LIScraper_SSL:
    def log_in_to_li_sales_nav(self, saved_search_url,blob_name_sn):
        # Your Azure Blob Storage code to read the cookie from the blob
        account_name = az_account_name
        account_key = az_account_key
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        container_name = li_cookies
        blob_name = blob_name_sn # Name of the JSON file to download

        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Create a ContainerClient
        container_client = blob_service_client.get_container_client(container_name)

        # Create a BlobClient
        blob_client = container_client.get_blob_client(blob_name)

        # Download the JSON file to a temporary object
        download_stream = blob_client.download_blob()
        content = download_stream.readall()

        # Create an in-memory buffer to store the content
        content_buffer = io.BytesIO(content)

        # Parse the JSON content to obtain the cookies
        try:
            cookies = json.loads(content_buffer.getvalue().decode('utf-8'))
        except Exception as e:
            # Example log message
            cookie_error = {'message': e, 'level': 'INFO',
                          'timestamp': datetime.utcnow()}
            send_log_to_elasticsearch(cookie_error)
            print("Error parsing cookies:", e)
        chrome_options = webdriver.ChromeOptions()
        #chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--window-size=1200x600")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.page_load_strategy = 'eager'

        # Creating a webdriver instance
        self.driver = webdriver.Chrome(options=chrome_options)

        # Opening LinkedIn's login page
        self.driver.get("https://www.linkedin.com/sales?trk=d_flagship3_nav&")
        time.sleep(2)

        # Load the obtained cookies into the driver
        for cookie in cookies:
            # You should set the 'domain' attribute for each cookie to ".linkedin.com" before adding it to the driver.
            cookie['domain'] = ".linkedin.com"
            try:
                self.driver.add_cookie(cookie)
            except Exception as e:
                # Example log message
                cookie_error = {'message': e, 'level': 'INFO',
                                'timestamp': datetime.utcnow()}
                send_log_to_elasticsearch(cookie_error)
                print("Error adding cookie:", e)


        time.sleep(3)
        home_url = "https://www.linkedin.com/sales/home"
        self.driver.get(home_url) # Use 'driver.get' to navigate to the home page
        # Example log message
        '''login_success = {'message': "Successfully Logged into Sales Navigator", 'level': 'INFO',
                        'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)'''
        print("Successfully Logged into Sales Navigator")

        # Retry loop with a maximum number of attempts - 3
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            self.driver.get(saved_search_url)
            try:
                print("Sales Navigator was successfully loaded using session cookies!!!!!")
                break
            except Exception as e:
                print(f"Attempt {attempt}/{max_attempts} failed. Error: {e}")

                if attempt < max_attempts:
                    print("Retrying Logging into Sales Navigator")
                    # Optionally, you can introduce a delay between retries
                    time.sleep(3)

        time.sleep(3)

        print("Login Successful.")
        # Example log message
        '''login_success = {'message': "Successfully Logged into Sales Navigator", 'level': 'INFO',
                         'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)'''
        time.sleep(5)
        start_scr = time.time()
        self.scroll_to_bottom()
        end_scr = time.time()
        print("Time taken to scroll page 1:", end_scr - start_scr)
        # Example log message
        '''login_success = {'message': "Time taken to scroll page 1:"+ str(end_scr - start_scr), 'level': 'INFO',
                         'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)'''
        num_results = self.get_number_of_results()
        print(f"{num_results} results found")
        number_records_collected = 0

        p = 1
        print("Gathering data for next page")
        print(f"Collecting page {p}")
        time.sleep(5)
        num_to_search = self.count_number_of_people_on_page()
        time.sleep(5)
        df = self.gather_data(num_to_search)

        df["pageNumber"] = str(p)
        number_records_collected += num_to_search

        while (num_results - number_records_collected) > 0:
            try:
                self.paginate()
                p += 1
                print(f"Collecting page {p}")
                time.sleep(3)
                start_scrol = time.time()
                self.scroll_to_bottom()
                end_scrol = time.time()
                print("Total time taken for scrolling:", end_scrol - start_scrol)
                time.sleep(3)
                num_to_search = self.count_number_of_people_on_page()
                time.sleep(5)
                new_df = self.gather_data(num_to_search)
                time.sleep(3)
                new_df["pageNumber"] = str(p)

                df = pd.concat([df, new_df])
                print("Total records collected:", len(df))
                number_records_collected += num_to_search
            except NoSuchElementException:
                break

        df['linkedinUrl'] = "https://www.linkedin.com/in/" + df['salesNavigatorUrl'].str.extract(
            '/sales/lead/([^,]+)')

        df['userId'] = df['salesNavigatorUrl'].str.extract('/sales/lead/([^,]+)')

        role = (
            df["timeInRole"]
            .str.extract(r"(?:(?P<y>\d+) years?)?\s*(?:(?P<m>\d+) months?)? in role")
            .astype(float)
            .fillna(0)
        )

        companyy = (
            df["timeInRole"]
            .str.extract(r"(?:(?P<y>\d+) years?)?\s*(?:(?P<m>\d+) months?)? in company")
            .astype(float)
            .fillna(0)
        )

        df["timeRoleMonths"] = (role["y"] * 12 + role["m"]).astype(int)
        df["timeCompanyMonths"] = (companyy["y"] * 12 + companyy["m"]).astype(int)
        df["snCompanyId"] = df['snCompanyUrl'].apply(lambda url: url.split("/sales/company/")[-1].split("?")[0] if "/sales/company/" in url else "")

        return df

    def scroll_to_bottom(self):
        print("scrolling to bottom started")
        time.sleep(5)
        inner_window = self.driver.find_element(By.XPATH, "/html/body/main/div/div[2]/div[2]")
        scroll = 0
        while scroll < 10:
            self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].offsetHeight;", inner_window)
            scroll += 1
            time.sleep(4)
            print("scrolling through section:", scroll)

        try:
            # Locate the parent div after scrolling to the bottom using XPath
            parent_div_xpath = "/html/body/main/div[1]/div/div[2]/div/div[4]"

            parent_div = self.driver.find_element(By.XPATH, parent_div_xpath)

            # Locate the thumbs-up button and click it using XPath
            thumbs_up_button_xpath = "/html/body/main/div[1]/div/div[2]/div/div[4]/div[2]/button[1]"
            thumbs_up_button = parent_div.find_element(By.XPATH, thumbs_up_button_xpath)
            thumbs_up_button.click()

            time.sleep(2)

            # Locate the close button and click it using XPath
            close_pop_xpath = "/html/body/main/div[1]/div/div[2]/div/div[4]/button"
            close_pop = parent_div.find_element(By.XPATH, close_pop_xpath)
            close_pop.click()

        except NoSuchElementException:
            print("Pop up not found. Skipping operation.")

    def gather_data(self, num_results):
        time.sleep(5)
        names = []
        positions = []
        time_in_roles = []
        locations = []
        sn_urls = []
        company = []
        sn_companyid_urls = []
        for i in range(1, num_results + 1):
            name = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/"
                f"div[1]/div[1]/div/div[2]/div[1]/div[1]/a/span",
            ).text
            names.append(name)

            position = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/"
                f"div[1]/div[1]/div/div[2]/div[2]/span[1]",
            ).text
            positions.append(position)

            time_in_role = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/"
                f"div[1]/div[1]/div/div[2]/div[4]",
            ).text
            time_in_roles.append(time_in_role)

            location = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[3]/span",
            ).text
            locations.append(location)

            sn_url = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[1]/div[1]/a",
            ).get_attribute('href')
            sn_urls.append(sn_url)

            try:
                sn_companyid_url = self.driver.find_element(
                    By.XPATH,
                    f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[2]/a",
                ).get_attribute('href')
                sn_companyid_urls.append(sn_companyid_url)
            except NoSuchElementException:
                sn_companyid_url = ""
                sn_companyid_urls.append(sn_companyid_url)



            try:
                companys = self.driver.find_element(
                    By.XPATH,
                    f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[2]/a",
                ).text
                company.append(companys)
            except NoSuchElementException:
                companys = ""
                company.append(companys)

        return pd.DataFrame({"name": names, "position": positions, "timeInRole": time_in_roles,
                             "location": locations, "salesNavigatorUrl": sn_urls,
                             "company": company, "snCompanyUrl": sn_companyid_urls})

    def paginate(self):
        time.sleep(3)
        print("Going to next page")

        try:
            # Wait for the 'Saved Search' button to be clickable
            paginate = WebDriverWait(self.driver, 10).until(
                ec.element_to_be_clickable(
                    (By.XPATH, "/html/body/main/div[1]/div[2]/div[2]/div/div[4]/div/button[2]")))
            paginate.click()
            print("Successfully going to next page")

        except Exception as EC:
            print("No next button, this is the last page", EC)
            pass



    def get_number_of_results(self):
        time.sleep(3)
        response = self.driver.find_element(
            By.XPATH, "//*[@id='content-main']/div[1]/div[2]/div[1]/div[2]/div/div[3]/span"
        ).text.split(" ")[0]
        print("Number of results:", response)
        if response[-1] == "+":
            return 1000  # upper cap on how many we'll collect
        else:
            return int(response)

    def count_number_of_people_on_page(self):
        print("Counting no of people on the page")
        time.sleep(3)
        a = self.driver.find_element(By.XPATH, "/html/body/main/div/div[2]/div[2]") \
            .get_attribute("innerHTML"). \
            count("circle-entity")
        print(a)

        return a

    def close_driver(self):
        self.driver.quit()

class LIScraper_profile:
    def log_in_to_li(self, blob_name):
        # Your Azure Blob Storage code to read the cookie from the blob
        account_name = az_account_name
        account_key = az_account_key
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        container_name = li_cookies

        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Create a ContainerClient
        container_client = blob_service_client.get_container_client(container_name)

        # Create a BlobClient
        blob_client = container_client.get_blob_client(blob_name)

        # Download the JSON file to a temporary object
        download_stream = blob_client.download_blob()
        content = download_stream.readall()

        # Create an in-memory buffer to store the content
        content_buffer = io.BytesIO(content)

        # Parse the JSON content to obtain the cookies
        try:
            cookies = json.loads(content_buffer.getvalue().decode('utf-8'))
        except Exception as e:
            print("Error parsing cookies:", e)

        chrome_options = webdriver.ChromeOptions()

        #chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--window-size=1200x600")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.page_load_strategy = 'eager'


        # Configure Chrome options
        '''chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.page_load_strategy = 'eager'
        chrome_options.add_argument("--window-size=1280,800")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--no-sandbox")'''

        # Creating a webdriver instance
        self.driver = webdriver.Chrome(options=chrome_options)

        # Opening LinkedIn's login page
        self.driver.get("https://www.linkedin.com/login")

        # Load the obtained cookies into the driver
        for cookie in cookies:
            # You should set the 'domain' attribute for each cookie to ".linkedin.com" before adding it to the driver.
            cookie['domain'] = ".linkedin.com"
            try:
                self.driver.add_cookie(cookie)
            except Exception as e:
                print("Error adding cookie:", e)

        time.sleep(2)
        home_url = "https://www.linkedin.com/feed/"
        self.driver.get(home_url)  # Use 'driver.get' to navigate to the home page
        time.sleep(2)
        print("Successfully Logged into Linkedin")
        print("Login Successful.")
        # Example log message
        login_success = {'message': "Successfully Logged into Sales Navigator", 'level': 'INFO',
                         'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)



    def linkedin_scraper(self,profile_url):

        def extract_about_profile_info(about_section):
            result = {
                "joined": ""
            }

            # Extract Joined Date
            joined_match = re.search(r"Joined\n(\w+ \d{4})", about_section)
            result["joined"] = joined_match.group(1) if joined_match else ""

            return result

        def extract_contact_info(contact_section):
            result = {
                "linkedinUrl": "",
                "email": "",
                "phone": "",
                "skype": "",
                "website": "",
                "twitter": "",
                "birthday": ""
            }

            # Extract LinkedIn URL
            linkedin_url_match = re.search(r"linkedin\.com/in/([\w-]+)", contact_section)
            result[
                "linkedinUrl"] = f"https://www.linkedin.com/in/{linkedin_url_match.group(1)}" if linkedin_url_match else ""

            # Extract Email
            email_match = re.search(r"Email\s*([\w.-]+@[a-zA-Z\d.-]+\.[a-zA-Z]{2,})", contact_section, re.IGNORECASE)
            result["email"] = email_match.group(1) if email_match else ""

            # Extract Phone
            phone_match = re.search(r"Phone\s*([\d\s()-]+)", contact_section, re.IGNORECASE)
            result["phone"] = phone_match.group(1) if phone_match else ""

            # Extract Skype
            skype_match = re.search(r"Skype\s*([\w.-]+)", contact_section, re.IGNORECASE)
            result["skype"] = skype_match.group(1) if skype_match else ""

            # Extract Company/Personal Website
            website_match = re.search(r"Website\s*([\w./:-]+)", contact_section, re.IGNORECASE)
            result["website"] = website_match.group(1) if website_match else ""

            # Extract Twitter
            twitter_match = re.search(r"Twitter\s*([\w.@]+)", contact_section, re.IGNORECASE)
            result["twitter"] = twitter_match.group(1) if twitter_match else ""

            # Extract Birthday
            birthday_match = re.search(r"Birthday\s*([\w\s]+)", contact_section, re.IGNORECASE)
            result["birthday"] = birthday_match.group(1) if birthday_match else ""

            return result


        url = str(profile_url)

        # Create a list to store profile information
        profiles_data = []

        self.driver.get(url)
        time.sleep(5)



        # Get the page source using Selenium
        src = self.driver.page_source

        # Parse the page using Beautiful Soup
        soup = BeautifulSoup(src, 'lxml')

        # Extract the profile name
        name_element = soup.find('h1', {'class': 'text-heading-xlarge inline t-24 v-align-middle break-words'})
        if name_element:
            profile_name = name_element.get_text().strip()
        else:
            profile_name = ""

        # Extract the profile position
        position_element = soup.find('div', {'class': 'text-body-medium break-words'})
        if position_element:
            profile_position = position_element.get_text().strip()
        else:
            profile_position = ""

        # Extract the profile location
        location_element = soup.find('span', {'class': 'text-body-small inline t-black--light break-words'})
        if location_element:
            profile_location = location_element.get_text().strip()
        else:
            profile_location = ""

        # Extract the about section
        about_element = soup.find('div', {
            'class': 'pv-shared-text-with-see-more full-width t-14 t-normal t-black display-flex align-items-center'})
        if about_element:
            about_text = about_element.get_text().strip()
        else:
            about_text = ""


        # Extract all li elements as text
        li_elements = soup.find('ul', {'class': 'EnflmBjARAphClpfMfbKmWkVPAJcWMJjRw'}).find_all('li')

        # Initialize variables
        follower_text = ""
        connection_text = ""

        # Iterate through li elements to extract text
        for li in li_elements:
            if li.find('span', {'class': 't-bold'}):  # Check if the li contains t-bold span
                if 'followers' in li.get_text():
                    follower_text = li.get_text().strip()
                    follower_text = follower_text.replace(',', '')  # Remove commas from the text
                elif 'connections' in li.get_text():
                    connection_text = li.get_text().strip()

        # If follower or connection text is empty, assign blank
        follower_text = follower_text if follower_text else ""
        connection_text = connection_text if connection_text else ""

        # Extract the "Contact info" link URL
        contact_info_link = soup.find('a', {'id': 'top-card-text-details-contact-info'})
        if contact_info_link:
            contact_info_url = contact_info_link.get('href')
            full_contact_info_url = "https://www.linkedin.com" + contact_info_url
            linkedin_abs_url = self.driver.current_url
        else:
            full_contact_info_url = ""
            linkedin_abs_url = ""

        # Find the span element with the specified class
        verification_icon = soup.find('span', class_='pv-member-badge--for-top-card')
        # Extract content if the block is present, else print ""
        if verification_icon:
            content_inside_block = True
        else:
            content_inside_block = False

        # Checking for verification icon
        verification_icon = soup.find('svg', {'data-test-icon': 'verified-medium'})

        # Create a dictionary for the current profile
        profile_data = {
            "profileUrl": url,
            "profileName": profile_name,
            "profilePosition": profile_position,
            "profileLocation": profile_location,
            "about": about_text,
            "noFollowers": follower_text,
            "noConnections": connection_text,
            "contactInfoLinks": full_contact_info_url,
            "linkedinAbsUrl": linkedin_abs_url,
            "premiumUser": content_inside_block
        }

        # Extract SNContactId from profileUrl
        sn_contact_id_match = re.search(r"linkedin\.com/in/([\w-]+)", profile_data["profileUrl"], re.IGNORECASE)
        profile_data["snContactId"] = sn_contact_id_match.group(1) if sn_contact_id_match else ""

        # Extract liContactId from linkedinAbsUrl
        li_contact_id_match = re.search(r"linkedin\.com/in/([\w-]+)", profile_data["linkedinAbsUrl"], re.IGNORECASE)
        profile_data["liContactId"] = li_contact_id_match.group(1) if li_contact_id_match else ""

        # Add the dictionary to the list
        profiles_data.append(profile_data)
        profile_data['isVerified'] = verification_icon is not False

        # Extracting profile details
        profile_data['name'] = soup.find('h1', class_='text-heading-xlarge').get_text(strip=True)

        # Extracting profile image URL
        # img_url = soup.find('img', class_='pv-top-card-profile-picture__image')['src']
        # profile_data['imgUrl'] = img_url

        profile_data['imgUrl'] = soup.find('img', class_='pv-top-card-profile-picture__image')['src'] if soup.find(
            'img',
            class_='pv-top-card-profile-picture__image') else ""

        # Extracting other details
        presence_indicator = soup.find('div', class_='presence-entity__indicator')
        status = presence_indicator.find('span', class_='visually-hidden').get_text(
            strip=True) if presence_indicator else ""
        profile_data['status'] = status

        # Extracting the alt attribute from the image
        alt_text = soup.find('img', class_='pv-top-card-profile-picture__image')['alt']
        profile_data['hiringCard'] = alt_text

        # Checking if the alt attribute contains the "#HIRING" text
        isHiring = "#HIRING" in alt_text
        isOenToWork = "#OPEN_TO_WORK" in alt_text
        profile_data['isHiring'] = isHiring
        profile_data['isOpenToWork'] = isOenToWork

        contact_section = linkedin_abs_url + "overlay/contact-info/"

        self.driver.get(str(contact_section))
        time.sleep(1)

        try:
            time.sleep(3)
            contact = WebDriverWait(self.driver, 120).until(
                EC.presence_of_element_located((By.XPATH, "/html/body/div[3]/div/div/div[2]"))).text
        except NoSuchElementException:
            contact = ""
        except TimeoutException:
            contact = ""

        profile_data['contactSection'] = contact
        time.sleep(3)

        about_this_profile_section = linkedin_abs_url + "overlay/about-this-profile/"

        self.driver.get(str(about_this_profile_section))

        try:
            time.sleep(3)
            about_this_profile = WebDriverWait(self.driver, 120).until(
                EC.presence_of_element_located((By.XPATH, "/html/body/div[3]/div/div/div[2]"))).text
        except NoSuchElementException:
            about_this_profile = ""
        except TimeoutException:
            about_this_profile = ""

        profile_data['aboutThisProfileSection'] = about_this_profile
        time.sleep(3)
        profile_data['structuredContact'] = extract_contact_info(profile_data['contactSection'])
        profile_data['structuredAbout'] = extract_about_profile_info(profile_data['aboutThisProfileSection'])

        experience_section = linkedin_abs_url + "details/experience/"
        self.driver.get(str(experience_section))
        # Get the page source using Selenium
        time.sleep(3)

        src = self.driver.page_source

        # Parse the page using Beautiful Soup
        soup = BeautifulSoup(src, 'lxml')

        experiences = []

        # Extracting details for each 'li' element
        for li in soup.select('.pvs-list__paged-list-item'):
            position = li.select_one('.t-bold').get_text(strip=True) if li.select_one('.t-bold') else ""
            tenure_element = li.select_one('.pvs-entity__caption-wrapper')
            tenure = tenure_element.get_text(strip=True) if tenure_element else ""
            company_url_element = li.select_one('.optional-action-target-wrapper')
            company_url = company_url_element['href'] if company_url_element else ""

            # Extracting skills if available
            skills_element = li.select_one('.display-flex.align-items-center.t-14.t-normal.t-black')
            desp = skills_element.get_text(strip=True) if skills_element else ""

            # Check if company_url is not None and contains the expected pattern
            if company_url and '/company/' in company_url:
                match = re.search(r'/company/(\d+)/', company_url)
                # Check if the search result is not None
                sn_company_id = match.group(1) if match else ""
            else:
                sn_company_id = ""

            experience_details = {
                'position': position,
                'tenureSection': tenure,
                'companyUrl': company_url,
                'snCompanyId': sn_company_id,
                'jobDescription': desp
            }

            experiences.append(experience_details)

        profile_data['experiences'] = experiences

        # Convert the list of dictionaries to a JSON string
        json_data = json.dumps(profile_data, default=str ,indent=2)


        return json_data
    def close_driver(self):
        self.driver.quit()

class decisionMakers:
    def decisionmakers(self, blob_name, profile_url):
        # Azure Blob Storage Configuration
        account_name = az_account_name
        account_key = az_account_key
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        container_name = li_cookies

        # Set up Azure Blob Storage client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)

        # Download the JSON file to a temporary object
        download_stream = blob_client.download_blob()
        content = download_stream.readall()

        # Create an in-memory buffer to store the content
        content_buffer = io.BytesIO(content)

        # Parse the JSON content to obtain the cookies
        try:
            cookies = json.loads(content_buffer.getvalue().decode('utf-8'))
        except Exception as e:
            return {"error": f"Error parsing cookies: {e}"}

        # Configure Chrome options
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--window-size=1200x600")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.page_load_strategy = 'eager'
        # Creating a webdriver instance
        self.driver = webdriver.Chrome(options=chrome_options)

        # Open LinkedIn's login page
        self.driver.get("https://www.linkedin.com/login")

        # Load the obtained cookies into the driver
        for cookie in cookies:
            # Set the 'domain' attribute for each cookie to ".linkedin.com"
            cookie['domain'] = ".linkedin.com"
            try:
                self.driver.add_cookie(cookie)
            except Exception as e:
                return {"error": f"Error adding cookie: {e}"}

        time.sleep(2)
        home_url = "https://www.linkedin.com/feed/"
        self.driver.get(home_url)
        time.sleep(2)

        url = profile_url + "people/"

        self.driver.get(url)
        time.sleep(5)

        # Get the page source using Selenium
        src = self.driver.page_source

        # Parse the page using Beautiful Soup
        soup = BeautifulSoup(src, 'lxml')

        a = soup.select_one("a[href*=decision-makers]")

        if a:
            dm = a['href']
            return {"decisionMakerUrl": dm}
        else:
            return {"decisionMakerUrl": "No element with 'href' containing 'decision-makers' found."}

    def close_driver(self):
        self.driver.quit()


class company_scraping:



    def log_in_to_li(self, blob_name):


        # Your Azure Blob Storage code to read the cookie from the blob
        account_name = az_account_name
        account_key = az_account_key
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        container_name = li_cookies

        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Create a ContainerClient
        container_client = blob_service_client.get_container_client(container_name)

        # Create a BlobClient
        blob_client = container_client.get_blob_client(blob_name)

        # Download the JSON file to a temporary object
        download_stream = blob_client.download_blob()
        content = download_stream.readall()

        # Create an in-memory buffer to store the content
        content_buffer = io.BytesIO(content)

        # Parse the JSON content to obtain the cookies
        try:
            cookies = json.loads(content_buffer.getvalue().decode('utf-8'))
        except Exception as e:
            print("Error parsing cookies:", e)

        chrome_options = webdriver.ChromeOptions()

        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--window-size=1200x600")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.page_load_strategy = 'eager'

        # Creating a webdriver instance
        self.driver = webdriver.Chrome(options=chrome_options)

        # Opening LinkedIn's login page
        self.driver.get("https://www.linkedin.com/login")

        # Load the obtained cookies into the driver
        for cookie in cookies:
            # You should set the 'domain' attribute for each cookie to ".linkedin.com" before adding it to the driver.
            cookie['domain'] = ".linkedin.com"
            try:
                self.driver.add_cookie(cookie)
            except Exception as e:
                print("Error adding cookie:", e)

        time.sleep(2)
        home_url = "https://www.linkedin.com/feed/"
        self.driver.get(home_url)  # Use 'driver.get' to navigate to the home page
        time.sleep(2)
        print("Successfully Logged into Linkedin")
        print("Login Successful.")
        # Example log message
        login_success = {'message': "Successfully Logged into Sales Navigator", 'level': 'INFO',
                         'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)

    def company_scraping(self, sn_id):
        time.sleep(2)

        def format_stock(stock):
            stock_symbol = stock[1]

            # Check if the stock symbol is valid (less than or equal to 7 letters)
            if len(stock_symbol) <= 7:
                stock_dict = {
                    "stockSymbol": stock_symbol,
                    "stockExchange": stock[2],
                    "stockPrice": stock[3],
                    "change": stock[4],
                    "open": stock[5],
                    "high": stock[6],
                    "low": stock[7],
                    "dataSource": stock[8]
                }
            else:
                # Invalid stock symbol, return a dictionary with empty values
                stock_dict = {
                    "stockSymbol": "",
                    "stockExchange": "",
                    "stockPrice": "",
                    "change": "",
                    "open": "",
                    "high": "",
                    "low": "",
                    "dataSource": ""
                }

            return stock_dict



        str_id = str(sn_id)

        ######################################################################

        profile_url = f"https://www.linkedin.com/company/{str_id}/"

        self.driver.get(profile_url)
        time.sleep(5)
        # Get the current URL from the WebDriver
        current_url = self.driver.current_url

        src = self.driver.page_source

        # Parse the page using Beautiful Soup
        soup = BeautifulSoup(src, 'lxml')

        # Find the logo container and extract the URL
        logo_container = soup.find('div', class_='org-top-card-primary-content__logo-container')
        if logo_container:
            logo_url = logo_container.find('img')['src']
        else:
            logo_url = ""

        name_element = soup.find('span', {'dir': 'ltr'})
        if name_element:
            profile_name = name_element.get_text().strip()
        else:
            profile_name = ""



            # Extract the top card section
        position_element1 = soup.find('p', {'class': 'org-top-card-summary__tagline'})
        if position_element1:
            profile_topcard = position_element1.get_text().strip()
        else:
            profile_topcard = ""

        # Find all headquarters element elements within position_element3 and extract their text
        position_element3 = soup.find_all('div', {'class': 'org-top-card-summary-info-list__info-item'})

        if position_element3:
            # Find all child elements within position_element3
            child_elements = position_element3

            # Initialize an empty list to store the extracted content from the elements
            headquarters_info = []

            # Extract text from each child element and append it to the headquarters_info list
            for child in child_elements:
                text = child.get_text(strip=True)
                headquarters_info.append(text)
        else:
            headquarters_info = [""]

        time.sleep(2)

        about_url = profile_url + "about/"

        self.driver.get(about_url)
        time.sleep(5)

        try:
            overview = self.driver.find_element(By.XPATH,
                                           "/html/body/div[4]/div[3]/div/div[2]/div/div[2]/main/div[2]/div/div/div[1]/section/p").text
        except NoSuchElementException:
            overview = ""

        try:
            dt = self.driver.find_element(By.XPATH,
                                     "/html/body/div[4]/div[3]/div/div[2]/div/div[2]/main/div[2]/div/div/div[1]/section/dl").text
        except NoSuchElementException:
            dt = ""

        try:
            noLocations = self.driver.find_element(By.XPATH,
                                              "/html/body/div[4]/div[3]/div/div[2]/div/div[2]/main/div[2]/div/div/div[3]/div[1]/h3").text
        except NoSuchElementException:
            noLocations = ""

        try:
            loc = self.driver.find_element(By.XPATH,
                                      "/html/body/div[4]/div[3]/div/div[2]/div/div[2]/main/div[2]/div/div/div[3]/div[2]/div").text
        except NoSuchElementException:
            loc = ""

        try:
            stock = self.driver.find_element(By.XPATH,
                                        "/html/body/div[4]/div[3]/div/div[2]/div/div[2]/aside/div[1]/div/div[1]").text
        except NoSuchElementException:
            stock = ""

        # Get the current date and time
        current_datetime = datetime.now()

        # Format the date and time as a string
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

        # Add the formatted datetime to the company_info dictionary
        company_info = {
            "snCompanyId": sn_id,
            "profileUrl": profile_url,
            "absLinkedinUrl": current_url,
            "name": profile_name,
            "logoUrl": logo_url,
            "topcard": profile_topcard,
            "hqInfo": ', '.join(headquarters_info),
            "overview": overview,
            "details": dt.split("\n"),
            "noLocations": noLocations.split("\n"),
            "loc": loc,
            "stock": stock.split("\n"),
            "dateTime:": formatted_datetime  # Split the details string into a list
        }

        # Extract LinkedIn ID from absLinkedinUrl
        parsed_url = urlparse(company_info["absLinkedinUrl"])
        linkedin_id = parsed_url.path.split("/")[2]

        company_info["linkedinId"] = linkedin_id

        # Format details and stock
        company_info["structuredStock"] = format_stock(company_info["stock"])

        #############################################################################
        # Extract the details section from the company_info dictionary
        details_section = company_info["details"]

        # Initialize variables to store extracted information
        website = phone = industry = company_size = headquarters = founded = specialties = ""
        associated_members = ""

        # Iterate through the details section and extract information based on keywords
        for i in range(len(details_section)):
            detail = details_section[i].strip()

            if detail.lower() == "website":
                website = details_section[i + 1].strip()
            elif detail.lower() == "phone":
                phone = details_section[i + 1].strip()
            elif detail.lower() == "industry":
                industry = details_section[i + 1].strip()
            elif detail.lower() == "company size":
                company_size = details_section[i + 1].strip()
            elif detail.lower() == "headquarters":
                headquarters = details_section[i + 1].strip()
            elif detail.lower() == "founded":
                founded = details_section[i + 1].strip()
            elif detail.lower() == "specialties":
                specialties = details_section[i + 1].strip()
            elif "associated members" in detail.lower():
                # Extract the associated members count using regex
                match = re.search(r'\b(\d+)\b', detail)
                if match:
                    associated_members = match.group(1)

        # Add the extracted information to the company_info dictionary
        company_info["website"] = website
        company_info["phone"] = phone
        company_info["industry"] = industry
        company_info["companySize"] = company_size
        company_info["headquarters"] = headquarters
        company_info["founded"] = founded
        company_info["specialties"] = specialties
        company_info["associatedMembersCount"] = associated_members  # Add this line

        ###################################################################

        # Convert the dictionary to a JSON-formatted string
        json_data = json.dumps(company_info, indent=2)

        # Print or save the JSON-formatted string
        return json_data

    def close_driver(self):
        self.driver.quit()

class LIScraper_profile_detail:
    def log_in_to_li(self, blob_name):
        # Your Azure Blob Storage code to read the cookie from the blob
        account_name = az_account_name
        account_key = az_account_key
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        container_name = li_cookies

        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Create a ContainerClient
        container_client = blob_service_client.get_container_client(container_name)

        # Create a BlobClient
        blob_client = container_client.get_blob_client(blob_name)

        # Download the JSON file to a temporary object
        download_stream = blob_client.download_blob()
        content = download_stream.readall()

        # Create an in-memory buffer to store the content
        content_buffer = io.BytesIO(content)

        # Parse the JSON content to obtain the cookies
        try:
            cookies = json.loads(content_buffer.getvalue().decode('utf-8'))
        except Exception as e:
            print("Error parsing cookies:", e)

        chrome_options = webdriver.ChromeOptions()

        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--window-size=1200x600")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.page_load_strategy = 'eager'


        # Configure Chrome options
        '''chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.page_load_strategy = 'eager'
        chrome_options.add_argument("--window-size=1280,800")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--no-sandbox")'''

        # Creating a webdriver instance
        self.driver = webdriver.Chrome(options=chrome_options)

        # Opening LinkedIn's login page
        self.driver.get("https://www.linkedin.com/login")

        # Load the obtained cookies into the driver
        for cookie in cookies:
            # You should set the 'domain' attribute for each cookie to ".linkedin.com" before adding it to the driver.
            cookie['domain'] = ".linkedin.com"
            try:
                self.driver.add_cookie(cookie)
            except Exception as e:
                print("Error adding cookie:", e)

        time.sleep(2)
        home_url = "https://www.linkedin.com/feed/"
        self.driver.get(home_url)  # Use 'driver.get' to navigate to the home page
        time.sleep(2)
        print("Successfully Logged into Linkedin")
        print("Login Successful.")
        # Example log message
        login_success = {'message': "Successfully Logged into Sales Navigator", 'level': 'INFO',
                         'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)

    def linkedin_detail_scraper(self, profile_url):

        # Create a list of dictionaries to store the profile information
        data = {'education': []}

        education_section = str(profile_url) + "details/education/"
        skills_section = str(profile_url) + "details/skills/"
        language_section = str(profile_url) + "details/languages/"

        self.driver.get(str(education_section))
        # Get the page source using Selenium
        time.sleep(3)

        # Get the page source after waiting
        page_source = self.driver.page_source

        # Use Beautiful Soup to parse the HTML
        soup = BeautifulSoup(page_source, 'html.parser')

        # Close the WebDriver

        # Extract data from the parsed HTML
        for item in soup.find_all('li', class_='pvs-list__paged-list-item'):
            degree_elem = item.find('span', class_='t-14 t-normal')
            degree = degree_elem.text.strip() if degree_elem else ""

            year_elem = item.find('span', class_='t-14 t-normal t-black--light')
            year = year_elem.text.strip() if year_elem else ""

            url_elem = item.find('a', class_='optional-action-target-wrapper')
            url = url_elem['href'] if url_elem else ""

            # Use a regular expression to extract the company ID from the URL
            match = re.search(r'/company/(\d+)/', url)
            snCompanyId = match.group(1) if match else ""

            data['education'].append({
                'degree': degree,
                'year': year,
                'universityUrl': url,
                'snCompanyId': snCompanyId
            })

        ########################################################################################
        self.driver.get(str(skills_section))
        # Get the page source using Selenium
        time.sleep(10)

        # Get the page source after waiting
        page_source = self.driver.page_source

        # Use Beautiful Soup to parse the HTML
        soup = BeautifulSoup(page_source, 'html.parser')

        # Find all <li> elements within the specified class
        skill_list_items = soup.find_all('li', class_='pvs-list__paged-list-item')
        # Initialize a string to store concatenated skill names
        concatenated_skills = ""

        for skill_item in skill_list_items:
            # Extract skill name
            skill_name_element = skill_item.find('div', class_='t-bold')
            skill_name = skill_name_element.text.strip() if skill_name_element else ""

            if skill_name:
                # Concatenate skill names with a new line
                concatenated_skills += skill_name + "|"

        # Add the concatenated skill names to the 'skills' key in the data dictionary
        data['allSkills'] = concatenated_skills.strip()

        ##########################################################################################
        ##########################################################################################
        self.driver.get(str(language_section))
        # Get the page source using Selenium
        time.sleep(3)

        # Get the page source after waiting
        page_source = self.driver.page_source

        # Use Beautiful Soup to parse the HTML
        soup = BeautifulSoup(page_source, 'html.parser')

        # Find all language elements
        language_list_items = soup.find_all('li', class_='pvs-list__paged-list-item')

        concatenated_languages = ""

        for item in language_list_items:
            # Extract language names
            div_t_bold = item.find('div', class_='t-bold')

            if div_t_bold:
                languages_item = div_t_bold.text.strip()
                languages = languages_item if languages_item else ""

                if languages:
                    # Concatenate skill names with a new line
                    concatenated_languages += languages + "|"

        # Add the concatenated skill names to the 'allLanguages' key in the data dictionary
        data['allLanguages'] = concatenated_languages.strip()

        # Clean the positions in the experiences
        # Convert the list of dictionaries to a JSON string
        json_data = json.dumps(data, default=str, indent=2)

        return json_data

    def close_driver(self):
        self.driver.quit()


class LIScraper_profile12:
    def log_in_to_li1(self, blob_name):
        # Your Azure Blob Storage code to read the cookie from the blob
        account_name = az_account_name
        account_key = az_account_key
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        container_name = li_cookies

        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Create a ContainerClient
        container_client = blob_service_client.get_container_client(container_name)

        # Create a BlobClient
        blob_client = container_client.get_blob_client(blob_name)

        # Download the JSON file to a temporary object
        download_stream = blob_client.download_blob()
        content = download_stream.readall()

        # Create an in-memory buffer to store the content
        content_buffer = io.BytesIO(content)

        # Parse the JSON content to obtain the cookies
        try:
            cookies = json.loads(content_buffer.getvalue().decode('utf-8'))
        except Exception as e:
            print("Error parsing cookies:", e)

        chrome_options = webdriver.ChromeOptions()

        #chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--window-size=1200x600")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.page_load_strategy = 'eager'


        # Configure Chrome options
        '''chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.page_load_strategy = 'eager'
        chrome_options.add_argument("--window-size=1280,800")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--no-sandbox")'''

        # Creating a webdriver instance
        self.driver = webdriver.Chrome(options=chrome_options)

        # Opening LinkedIn's login page
        self.driver.get("https://www.linkedin.com/login")

        # Load the obtained cookies into the driver
        for cookie in cookies:
            # You should set the 'domain' attribute for each cookie to ".linkedin.com" before adding it to the driver.
            cookie['domain'] = ".linkedin.com"
            try:
                self.driver.add_cookie(cookie)
            except Exception as e:
                print("Error adding cookie:", e)

        time.sleep(2)
        home_url = "https://www.linkedin.com/feed/"
        self.driver.get(home_url)  # Use 'driver.get' to navigate to the home page
        time.sleep(2)
        print("Successfully Logged into Linkedin")
        print("Login Successful.")
        # Example log message
        login_success = {'message': "Successfully Logged into Sales Navigator", 'level': 'INFO',
                         'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)



    def linkedin_scraper(self,profile_url):

        def extract_about_profile_info(about_section):
            result = {
                "joined": ""
            }

            # Extract Joined Date
            joined_match = re.search(r"Joined\n(\w+ \d{4})", about_section)
            result["joined"] = joined_match.group(1) if joined_match else ""

            return result

        def extract_contact_info(contact_section):
            result = {
                "linkedinUrl": "",
                "email": "",
                "phone": "",
                "skype": "",
                "website": "",
                "twitter": "",
                "birthday": ""
            }

            # Extract LinkedIn URL
            linkedin_url_match = re.search(r"linkedin\.com/in/([\w-]+)", contact_section)
            result[
                "linkedinUrl"] = f"https://www.linkedin.com/in/{linkedin_url_match.group(1)}" if linkedin_url_match else ""

            # Extract Email
            email_match = re.search(r"Email\s*([\w.-]+@[a-zA-Z\d.-]+\.[a-zA-Z]{2,})", contact_section, re.IGNORECASE)
            result["email"] = email_match.group(1) if email_match else ""

            # Extract Phone
            phone_match = re.search(r"Phone\s*([\d\s()-]+)", contact_section, re.IGNORECASE)
            result["phone"] = phone_match.group(1) if phone_match else ""

            # Extract Skype
            skype_match = re.search(r"Skype\s*([\w.-]+)", contact_section, re.IGNORECASE)
            result["skype"] = skype_match.group(1) if skype_match else ""

            # Extract Company/Personal Website
            website_match = re.search(r"Website\s*([\w./:-]+)", contact_section, re.IGNORECASE)
            result["website"] = website_match.group(1) if website_match else ""

            # Extract Twitter
            twitter_match = re.search(r"Twitter\s*([\w.@]+)", contact_section, re.IGNORECASE)
            result["twitter"] = twitter_match.group(1) if twitter_match else ""

            # Extract Birthday
            birthday_match = re.search(r"Birthday\s*([\w\s]+)", contact_section, re.IGNORECASE)
            result["birthday"] = birthday_match.group(1) if birthday_match else ""

            return result


        url = str(profile_url)

        # Create a list to store profile information
        profiles_data = []

        self.driver.get(url)
        time.sleep(5)



        # Get the page source using Selenium
        src = self.driver.page_source

        # Parse the page using Beautiful Soup
        soup = BeautifulSoup(src, 'lxml')

        # Extract the profile name
        name_element = soup.find('h1', {'class': 'text-heading-xlarge inline t-24 v-align-middle break-words'})
        if name_element:
            profile_name = name_element.get_text().strip()
        else:
            profile_name = ""

        # Extract the profile position
        position_element = soup.find('div', {'class': 'text-body-medium break-words'})
        if position_element:
            profile_position = position_element.get_text().strip()
        else:
            profile_position = ""

        # Extract the profile location
        location_element = soup.find('span', {'class': 'text-body-small inline t-black--light break-words'})
        if location_element:
            profile_location = location_element.get_text().strip()
        else:
            profile_location = ""

        # Extract the about section
        about_element = soup.find('div', {
            'class': 'pv-shared-text-with-see-more full-width t-14 t-normal t-black display-flex align-items-center'})
        if about_element:
            about_text = about_element.get_text().strip()
        else:
            about_text = ""

        '''# Extract all li elements as text
        ul_element = soup.find('ul', {'class': 'MwLAKCzgWMFdUDUmSEWNqsmIEVmouZOeY pv-top-card--list-bullet'})
        if ul_element:
            li_elements = ul_element.find_all('li')

            # Initialize variables
            follower_text = ""
            connection_text = ""

            # Iterate through li elements to extract text
            for li in li_elements:
                if li.find('span', {'class': 't-bold'}):  # Check if the li contains t-bold span
                    if 'followers' in li.get_text():
                        follower_text = li.get_text().strip()
                        follower_text = follower_text.replace(',', '')  # Remove commas from the text
                    elif 'connections' in li.get_text():
                        connection_text = li.get_text().strip()

                # If follower or connection text is empty, assign blank
            follower_text = follower_text if follower_text else ""
            connection_text = connection_text if connection_text else ""
        else:
            print("Ul element not found or empty")'''

        # Update the class pattern to match the dynamic part of the class name
        class_pattern = re.compile(r"iLfqLhswEsbNpkcmbZBAadTRgDObcbGbisWs TgufCrTbwvyrKVlGEGxVnYYQTvZteoJeobPg")

        # Extract all li elements as text
        ul_element = soup.find('ul', {'class': class_pattern})
        if ul_element:
            li_elements = ul_element.find_all('li')

            # Initialize variables
            follower_text = ""
            connection_text = ""

            # Iterate through li elements to extract text
            for li in li_elements:
                if li.find('span', {'class': 't-bold'}):  # Check if the li contains t-bold span
                    if 'followers' in li.get_text():
                        follower_text = li.get_text().strip()
                        follower_text = follower_text.replace(',', '')  # Remove commas from the text
                    elif 'connections' in li.get_text():
                        connection_text = li.get_text().strip()

            # If follower or connection text is empty, assign blank
            follower_text = follower_text if follower_text else ""
            connection_text = connection_text if connection_text else ""
        else:
            print("Ul element not found or empty")

        # Extract the "Contact info" link URL
        contact_info_link = soup.find('a', {'id': 'top-card-text-details-contact-info'})
        if contact_info_link:
            contact_info_url = contact_info_link.get('href')
            full_contact_info_url = "https://www.linkedin.com" + contact_info_url
            linkedin_abs_url = self.driver.current_url
        else:
            full_contact_info_url = ""
            linkedin_abs_url = ""

        # Find the span element with the specified class
        verification_icon = soup.find('span', class_='pv-member-badge--for-top-card')
        # Extract content if the block is present, else print ""
        if verification_icon:
            content_inside_block = True
        else:
            content_inside_block = False

        # Checking for verification icon
        verification_icon = soup.find('svg', {'data-test-icon': 'verified-medium'})

        # Create a dictionary for the current profile
        profile_data = {
            "profileUrl": url,
            "profileName": profile_name,
            "profilePosition": profile_position,
            "profileLocation": profile_location,
            "about": about_text,
            "noFollowers": follower_text,
            "noConnections": connection_text,
            "contactInfoLinks": full_contact_info_url,
            "linkedinAbsUrl": linkedin_abs_url,
            "premiumUser": content_inside_block
        }

        # Extract SNContactId from profileUrl
        sn_contact_id_match = re.search(r"linkedin\.com/in/([\w-]+)", profile_data["profileUrl"], re.IGNORECASE)
        profile_data["snContactId"] = sn_contact_id_match.group(1) if sn_contact_id_match else ""

        # Extract liContactId from linkedinAbsUrl
        li_contact_id_match = re.search(r"linkedin\.com/in/([\w-]+)", profile_data["linkedinAbsUrl"], re.IGNORECASE)
        profile_data["liContactId"] = li_contact_id_match.group(1) if li_contact_id_match else ""

        # Add the dictionary to the list
        profiles_data.append(profile_data)
        profile_data['isVerified'] = verification_icon is not False

        # Extracting profile details
        profile_data['name'] = soup.find('h1', class_='text-heading-xlarge').get_text(strip=True)

        # Extracting profile image URL
        # img_url = soup.find('img', class_='pv-top-card-profile-picture__image')['src']
        # profile_data['imgUrl'] = img_url

        profile_data['imgUrl'] = soup.find('img', class_='pv-top-card-profile-picture__image')['src'] if soup.find(
            'img',
            class_='pv-top-card-profile-picture__image') else ""

        # Extracting other details
        presence_indicator = soup.find('div', class_='presence-entity__indicator')
        status = presence_indicator.find('span', class_='visually-hidden').get_text(
            strip=True) if presence_indicator else ""
        profile_data['status'] = status

        # Extracting the alt attribute from the image
        alt_text = soup.find('img', class_='pv-top-card-profile-picture__image')['alt']
        profile_data['hiringCard'] = alt_text

        # Checking if the alt attribute contains the "#HIRING" text
        isHiring = "#HIRING" in alt_text
        isOenToWork = "#OPEN_TO_WORK" in alt_text
        profile_data['isHiring'] = isHiring
        profile_data['isOpenToWork'] = isOenToWork

        contact_section = linkedin_abs_url + "overlay/contact-info/"

        self.driver.get(str(contact_section))
        time.sleep(1)

        try:
            time.sleep(3)
            contact = WebDriverWait(self.driver, 120).until(
                EC.presence_of_element_located((By.XPATH, "/html/body/div[3]/div/div/div[2]"))).text
        except NoSuchElementException:
            contact = ""
        except TimeoutException:
            contact = ""

        profile_data['contactSection'] = contact
        time.sleep(3)

        about_this_profile_section = linkedin_abs_url + "overlay/about-this-profile/"

        self.driver.get(str(about_this_profile_section))

        try:
            time.sleep(3)
            about_this_profile = WebDriverWait(self.driver, 120).until(
                EC.presence_of_element_located((By.XPATH, "/html/body/div[3]/div/div/div[2]"))).text
        except NoSuchElementException:
            about_this_profile = ""
        except TimeoutException:
            about_this_profile = ""

        profile_data['aboutThisProfileSection'] = about_this_profile
        time.sleep(3)
        profile_data['structuredContact'] = extract_contact_info(profile_data['contactSection'])
        profile_data['structuredAbout'] = extract_about_profile_info(profile_data['aboutThisProfileSection'])

        experience_section = linkedin_abs_url + "details/experience/"
        self.driver.get(str(experience_section))
        # Get the page source using Selenium
        time.sleep(3)

        src = self.driver.page_source

        # Parse the page using Beautiful Soup
        soup = BeautifulSoup(src, 'lxml')

        experiences = []

        # Extracting details for each 'li' element
        for li in soup.select('.pvs-list__paged-list-item'):
            position = li.select_one('.t-bold').get_text(strip=True) if li.select_one('.t-bold') else ""
            tenure_element = li.select_one('.pvs-entity__caption-wrapper')
            tenure = tenure_element.get_text(strip=True) if tenure_element else ""
            company_url_element = li.select_one('.optional-action-target-wrapper')
            company_url = company_url_element['href'] if company_url_element else ""

            # Extracting skills if available
            skills_element = li.select_one('.display-flex.align-items-center.t-14.t-normal.t-black')
            desp = skills_element.get_text(strip=True) if skills_element else ""

            # Check if company_url is not None and contains the expected pattern
            if company_url and '/company/' in company_url:
                match = re.search(r'/company/(\d+)/', company_url)
                # Check if the search result is not None
                sn_company_id = match.group(1) if match else ""
            else:
                sn_company_id = ""

            experience_details = {
                'position': position,
                'tenureSection': tenure,
                'companyUrl': company_url,
                'snCompanyId': sn_company_id,
                'jobDescription': desp
            }

            experiences.append(experience_details)

        profile_data['experiences'] = experiences

        # Convert the list of dictionaries to a JSON string
        json_data = json.dumps(profile_data, default=str ,indent=2)


        return json_data
    def close_driver(self):
        self.driver.quit()

class LIScraper_SSL_keywords:
    def log_in_to_li_sales_nav(self, saved_search_url,blob_name_sn,keyword):
        # Your Azure Blob Storage code to read the cookie from the blob
        account_name = az_account_name
        account_key = az_account_key
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        container_name = li_cookies
        blob_name = blob_name_sn # Name of the JSON file to download

        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Create a ContainerClient
        container_client = blob_service_client.get_container_client(container_name)

        # Create a BlobClient
        blob_client = container_client.get_blob_client(blob_name)

        # Download the JSON file to a temporary object
        download_stream = blob_client.download_blob()
        content = download_stream.readall()

        # Create an in-memory buffer to store the content
        content_buffer = io.BytesIO(content)

        # Parse the JSON content to obtain the cookies
        try:
            cookies = json.loads(content_buffer.getvalue().decode('utf-8'))
        except Exception as e:
            # Example log message
            cookie_error = {'message': e, 'level': 'INFO',
                          'timestamp': datetime.utcnow()}
            send_log_to_elasticsearch(cookie_error)
            print("Error parsing cookies:", e)
        chrome_options = webdriver.ChromeOptions()
        #chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--window-size=1200x600")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--allow-insecure-localhost")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.page_load_strategy = 'eager'

        # Creating a webdriver instance
        self.driver = webdriver.Chrome(options=chrome_options)

        # Opening LinkedIn's login page
        self.driver.get("https://www.linkedin.com/sales?trk=d_flagship3_nav&")
        time.sleep(2)

        # Load the obtained cookies into the driver
        for cookie in cookies:
            # You should set the 'domain' attribute for each cookie to ".linkedin.com" before adding it to the driver.
            cookie['domain'] = ".linkedin.com"
            try:
                self.driver.add_cookie(cookie)
            except Exception as e:
                # Example log message
                cookie_error = {'message': e, 'level': 'INFO',
                                'timestamp': datetime.utcnow()}
                send_log_to_elasticsearch(cookie_error)
                print("Error adding cookie:", e)


        time.sleep(3)
        home_url = "https://www.linkedin.com/sales/home"
        self.driver.get(home_url) # Use 'driver.get' to navigate to the home page
        # Example log message
        '''login_success = {'message': "Successfully Logged into Sales Navigator", 'level': 'INFO',
                        'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)'''
        print("Successfully Logged into Sales Navigator")

        # Retry loop with a maximum number of attempts - 3
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            self.driver.get(saved_search_url)


            try:
                time.sleep(2)
                # Find the input box using XPath
                input_box = self.driver.find_element(By.XPATH,
                                                "/html/body/main/div[1]/div/div[1]/div[1]/div[2]/div/div/label/div/div/input")
                print("Keyword element found")
                # Input text into the input box
                input_box.send_keys(str(keyword))
                print("Successfully added keyword to the mix")
                # Press Enter key
                input_box.send_keys(Keys.RETURN)

                # Continue with your code after the wait

            except NoSuchElementException:
                print("Input box not found!")

            try:
                time.sleep(2)
                print("Sales Navigator was successfully loaded using session cookies!!!!!")
                break
            except Exception as e:
                print(f"Attempt {attempt}/{max_attempts} failed. Error: {e}")

                if attempt < max_attempts:
                    print("Retrying Logging into Sales Navigator")
                    # Optionally, you can introduce a delay between retries
                    time.sleep(3)

        time.sleep(3)

        print("Login Successful.")
        # Example log message
        '''login_success = {'message': "Successfully Logged into Sales Navigator", 'level': 'INFO',
                         'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)'''
        time.sleep(5)
        start_scr = time.time()
        self.scroll_to_bottom()
        end_scr = time.time()
        print("Time taken to scroll page 1:", end_scr - start_scr)
        # Example log message
        '''login_success = {'message': "Time taken to scroll page 1:"+ str(end_scr - start_scr), 'level': 'INFO',
                         'timestamp': datetime.utcnow()}
        send_log_to_elasticsearch(login_success)'''
        num_results = self.get_number_of_results()
        print(f"{num_results} results found")
        number_records_collected = 0

        p = 1
        print("Gathering data for next page")
        print(f"Collecting page {p}")
        time.sleep(5)
        num_to_search = self.count_number_of_people_on_page()
        time.sleep(5)
        df = self.gather_data(num_to_search)

        df["pageNumber"] = str(p)
        number_records_collected += num_to_search

        while (num_results - number_records_collected) > 0:
            try:
                self.paginate()
                p += 1
                print(f"Collecting page {p}")
                time.sleep(3)
                start_scrol = time.time()
                self.scroll_to_bottom()
                end_scrol = time.time()
                print("Total time taken for scrolling:", end_scrol - start_scrol)
                time.sleep(3)
                num_to_search = self.count_number_of_people_on_page()
                time.sleep(5)
                new_df = self.gather_data(num_to_search)
                time.sleep(3)
                new_df["pageNumber"] = str(p)

                df = pd.concat([df, new_df])
                print("Total records collected:", len(df))
                number_records_collected += num_to_search
            except NoSuchElementException:
                break

        df['linkedinUrl'] = "https://www.linkedin.com/in/" + df['salesNavigatorUrl'].str.extract(
            '/sales/lead/([^,]+)')

        df['userId'] = df['salesNavigatorUrl'].str.extract('/sales/lead/([^,]+)')

        role = (
            df["timeInRole"]
            .str.extract(r"(?:(?P<y>\d+) years?)?\s*(?:(?P<m>\d+) months?)? in role")
            .astype(float)
            .fillna(0)
        )

        companyy = (
            df["timeInRole"]
            .str.extract(r"(?:(?P<y>\d+) years?)?\s*(?:(?P<m>\d+) months?)? in company")
            .astype(float)
            .fillna(0)
        )

        df["timeRoleMonths"] = (role["y"] * 12 + role["m"]).astype(int)
        df["timeCompanyMonths"] = (companyy["y"] * 12 + companyy["m"]).astype(int)
        df["snCompanyId"] = df['snCompanyUrl'].apply(lambda url: url.split("/sales/company/")[-1].split("?")[0] if "/sales/company/" in url else "")

        return df

    def scroll_to_bottom(self):
        print("scrolling to bottom started")
        time.sleep(5)
        inner_window = self.driver.find_element(By.XPATH, "/html/body/main/div/div[2]/div[2]")
        scroll = 0
        while scroll < 10:
            self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].offsetHeight;", inner_window)
            scroll += 1
            time.sleep(4)
            print("scrolling through section:", scroll)

        try:
            # Locate the parent div after scrolling to the bottom using XPath
            parent_div_xpath = "/html/body/main/div[1]/div/div[2]/div/div[4]"

            parent_div = self.driver.find_element(By.XPATH, parent_div_xpath)

            # Locate the thumbs-up button and click it using XPath
            thumbs_up_button_xpath = "/html/body/main/div[1]/div/div[2]/div/div[4]/div[2]/button[1]"
            thumbs_up_button = parent_div.find_element(By.XPATH, thumbs_up_button_xpath)
            thumbs_up_button.click()

            time.sleep(2)

            # Locate the close button and click it using XPath
            close_pop_xpath = "/html/body/main/div[1]/div/div[2]/div/div[4]/button"
            close_pop = parent_div.find_element(By.XPATH, close_pop_xpath)
            close_pop.click()

        except NoSuchElementException:
            print("Pop up not found. Skipping operation.")

    def gather_data(self, num_results):
        time.sleep(5)
        names = []
        positions = []
        time_in_roles = []
        locations = []
        sn_urls = []
        company = []
        sn_companyid_urls = []
        for i in range(1, num_results + 1):
            name = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/"
                f"div[1]/div[1]/div/div[2]/div[1]/div[1]/a/span",
            ).text
            names.append(name)

            position = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/"
                f"div[1]/div[1]/div/div[2]/div[2]/span[1]",
            ).text
            positions.append(position)

            time_in_role = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/"
                f"div[1]/div[1]/div/div[2]/div[4]",
            ).text
            time_in_roles.append(time_in_role)

            location = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[3]/span",
            ).text
            locations.append(location)

            sn_url = self.driver.find_element(
                By.XPATH,
                f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[1]/div[1]/a",
            ).get_attribute('href')
            sn_urls.append(sn_url)

            try:
                sn_companyid_url = self.driver.find_element(
                    By.XPATH,
                    f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[2]/a",
                ).get_attribute('href')
                sn_companyid_urls.append(sn_companyid_url)
            except NoSuchElementException:
                sn_companyid_url = ""
                sn_companyid_urls.append(sn_companyid_url)



            try:
                companys = self.driver.find_element(
                    By.XPATH,
                    f"/html/body/main/div[1]/div[2]/div[2]/div/ol/li[{i}]/div/div/div[2]/div[1]/div[1]/div/div[2]/div[2]/a",
                ).text
                company.append(companys)
            except NoSuchElementException:
                companys = ""
                company.append(companys)

        return pd.DataFrame({"name": names, "position": positions, "timeInRole": time_in_roles,
                             "location": locations, "salesNavigatorUrl": sn_urls,
                             "company": company, "snCompanyUrl": sn_companyid_urls})

    def paginate(self):
        time.sleep(3)
        print("Going to next page")

        try:
            # Wait for the 'Saved Search' button to be clickable
            paginate = WebDriverWait(self.driver, 10).until(
                ec.element_to_be_clickable(
                    (By.XPATH, "/html/body/main/div[1]/div[2]/div[2]/div/div[4]/div/button[2]")))
            paginate.click()
            print("Successfully going to next page")

        except Exception as EC:
            print("No next button, this is the last page", EC)
            pass



    def get_number_of_results(self):
        time.sleep(3)
        response = self.driver.find_element(
            By.XPATH, "//*[@id='content-main']/div[1]/div[2]/div[1]/div[2]/div/div[3]/span"
        ).text.split(" ")[0]
        print("Number of results:", response)
        if response[-1] == "+":
            return 1000  # upper cap on how many we'll collect
        else:
            return int(response)

    def count_number_of_people_on_page(self):
        print("Counting no of people on the page")
        time.sleep(3)
        a = self.driver.find_element(By.XPATH, "/html/body/main/div/div[2]/div[2]") \
            .get_attribute("innerHTML"). \
            count("circle-entity")
        print(a)

        return a

    def close_driver(self):
        self.driver.quit()

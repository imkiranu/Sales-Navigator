from flask import Flask, request, jsonify
import json
import os
import requests
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime

app = Flask(__name__)

def perform_login(username_int, password_int, login_type):
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

        if len(username_int) > 0:
            if login_type == 'linkedin':
                prefix = 'LI_'
            elif login_type == 'sales_navigator':
                prefix = 'SN_'
            else:
                raise ValueError("Invalid login type")

            file_name = f"{prefix}{username_int}.json"
            file_path = os.path.join(os.getcwd(), file_name)

            with open(file_path, 'w') as file:
                json.dump(cookies, file)

            return file_path  # Return the file path of the saved cookies

        else:
            raise ValueError("Please provide the username")

    except Exception as e:
        return str(e)

@app.route('/login', methods=['POST'])
def login_and_upload():
    try:
        data = request.get_json()
        username = data.get('userName')
        password = data.get('passWord')
        login_type = data.get('loginType')

        print("Login process started")

        if not (username and password and login_type):
            return jsonify({'error': 'Invalid request. Please provide userName, PassWord, and loginType in JSON.'}), 400

        file_path = perform_login(username, password, login_type)
        if not file_path:
            return jsonify({'error': 'Failed to save cookies locally.'}), 500

        url = 'https://ipp-api-dev.my.matchbookservices.com/api/cookie/upload'

        with open(file_path, 'rb') as file:
            files = {'fileData': file}

            response = requests.post(url, files=files)

        if response.status_code == 200:
            os.remove(file_path)
            return jsonify({'message': 'Login successful and cookies uploaded successfully!'}), 200
        else:
            return jsonify({'error': f'Error uploading file. Status code: {response.status_code}', 'response': response.text}), 500

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
import os
from lxml import html
import time

SCRAPINGBEE_KEY = os.getenv("SCRAPINGBEE_KEY")
ERRORS = {"NOT_REACHABLE": "Either URL is incorrect or it is not reachable", "XPATH_NOT_FOUND": "Could not reach to this location", "NO_DATA": "Blank [No Data Available]"}

def scrape_url(url, xpaths):
    params = {
        'api_key': SCRAPINGBEE_KEY,
        'url': url,
        'render_js': 1,  # JS
        'country_code': 'in'  # India
    }
    try:
        r = requests.get('https://app.scrapingbee.com/api/v1/', params=params, timeout=60)
        if r.status_code == 200:
            tree = html.fromstring(r.text)
            data = []
            for xpath in xpaths:
                vals = tree.xpath(xpath)
                value = " ".join([v.text_content().strip() if v.text_content() else '' for v in vals]) or ERRORS["NO_DATA"]
                data.append(value)
            return data
        else:
            print(f"Error: {r.text}")
            return [ERRORS["NOT_REACHABLE"]] * len(xpaths)
    except Exception as e:
        print(f"Failed: {e}")
        return [ERRORS["NOT_REACHABLE"]] * len(xpaths)

def connect_to_sheet():
    key = json.loads(os.getenv("GOOGLE_CREDS"))
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(key, scopes=scope)
    client = gspread.authorize(creds)
    return client.open("output-m1-amz-nwlst-general").sheet1

if __name__ == "__main__":
    df = pd.read_csv("input-m1-amz-nwlst-general.csv")
    urls = df["Current URL"].dropna().tolist()
    xpaths = df.iloc[1, 1:].dropna().tolist()
    sheet = connect_to_sheet()
    for i, url in enumerate(urls):
        data = scrape_url(url, xpaths)
        sheet.append_row([url] + data)
        print(f"Processed {i+1}/{len(urls)}")
        time.sleep(0.5)  # Delay
    print("Done!")

import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
import os
from lxml import html
import time

SCRAPINGBEE_KEY = os.getenv("SCRAPINGBEE_KEY")

ERRORS = {
    "NOT_REACHABLE": "Either URL is incorrect or it is not reachable",
    "XPATH_NOT_FOUND": "Could not reach to this location",
    "NO_DATA": "Blank [No Data Available]"
}

def scrape_url(url, xpaths):
    print(f"Scraping: {url}")
    params = {
        'api_key': SCRAPINGBEE_KEY,
        'url': url,
        'render_js': 'true',
        'premium_proxy': 'true',
        'country_code': 'in',
        'wait': '8000',
        'block_resources': 'true,image,font'  # Faster, less detection
    }
    try:
        r = requests.get('https://app.scrapingbee.com/api/v1/', params=params, timeout=120)
        print(f"Status code: {r.status_code}")
        if r.status_code == 200:
            tree = html.fromstring(r.content)
            data = []
            for xpath in xpaths:
                try:
                    vals = tree.xpath(xpath)
                    value = " ".join([v.text_content().strip() for v in vals if v.text_content() and v.text_content().strip()])
                    data.append(value or ERRORS["NO_DATA"])
                except:
                    data.append(ERRORS["XPATH_NOT_FOUND"])
            return data
        else:
            print(f"API error {r.status_code}: {r.text[:300]}")
            return [ERRORS["NOT_REACHABLE"]] * len(xpaths)
    except Exception as e:
        print(f"Request failed: {e}")
        return [ERRORS["NOT_REACHABLE"]] * len(xpaths)

def connect_to_sheet():
    key = json.loads(os.getenv("GOOGLE_CREDS"))
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(key, scopes=scope)
    client = gspread.authorize(creds)
    return client.open("output-m1-amz-nwlst-general").sheet1

if __name__ == "__main__":
    print("Starting scraper...")
    df = pd.read_csv("input-m1-amz-nwlst-general.csv")
    urls = df["Current URL"].dropna().tolist()
    print(f"Found {len(urls)} URLs")
    xpaths = df.iloc[1, 1:].dropna().tolist()
    print(f"Found {len(xpaths)} XPaths")
    sheet = connect_to_sheet()
    for i, url in enumerate(urls):
        data = scrape_url(url, xpaths)
        sheet.append_row([url] + data)
        print(f"Processed {i+1}/{len(urls)}")
        time.sleep(1)  # Delay for stability
    print("All finished! Check your Google Sheet.")

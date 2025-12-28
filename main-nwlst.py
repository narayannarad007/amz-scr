import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
import os
from lxml import html
import time

SCRAPINGBEE_KEY = os.getenv("ZAHII03MJJKNAMPMVKW2H6ER87XV99OYQ2T4GVSRJYQQEC62EIXZNANGAWOCXZSL2JDSXHHLQIJ2VBKR")  # Your API key secret

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
        'render_js': 'true',  # Enable JS for dynamic content
        'premium_proxy': 'true',  # Use premium proxies to avoid blocks
        'country_code': 'in',  # India-specific data
        'wait': '5000'  # Wait 5 seconds for page load
    }
    try:
        r = requests.get('https://app.scrapingbee.com/api/v1/', params=params, timeout=90)
        if r.status_code == 200:
            tree = html.fromstring(r.text)
            data = []
            for xpath in xpaths:
                vals = tree.xpath(xpath)
                value = " ".join([v.text_content().strip() if hasattr(v, 'text_content') else '' for v in vals])
                data.append(value or ERRORS["NO_DATA"])
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
    xpaths = df.iloc[1, 1:].dropna().tolist()
    sheet = connect_to_sheet()
    for i, url in enumerate(urls):
        data = scrape_url(url, xpaths)
        sheet.append_row([url] + data)
        print(f"Processed {i+1}/{len(urls)}")
        time.sleep(0.5)  # Polite delay to avoid rate limits
    print("All done! Check Google Sheet for results.")

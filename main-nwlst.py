import asyncio
import json
import os
import random
import pandas as pd
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials

NUM_AT_ONCE = 20  # Test small
WAIT_TIME = 0.5
ERRORS = {"NOT_REACHABLE": "Either URL is incorrect or it is not reachable", "XPATH_NOT_FOUND": "Could not reach to this location", "NO_DATA": "Blank [No Data Available]"}
FAKE_BROWSERS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"]

async def get_stuff_from_page(page, xpaths):
    results = []
    for xpath in xpaths:
        try:
            if "/@" in xpath:
                part1, part2 = xpath.split("/@")
                part2 = "innerHTML" if part2 == "html" else part2
                elem = await page.wait_for_selector(part1, timeout=5000)
                value = await elem.get_attribute(part2) if elem else ERRORS["NO_DATA"]
            else:
                elem = await page.wait_for_selector(xpath, timeout=1000)
                if elem:
                    value = await elem.inner_text()
                    if not value:
                        value = await elem.get_attribute("innerText")
                    if not value:
                        html = await elem.inner_html()
                        value = html.replace('<script>', '').replace('</script>', '').strip()
                else:
                    value = ERRORS["XPATH_NOT_FOUND"]
            results.append(value if value else ERRORS["NO_DATA"])
        except:
            results.append(ERRORS["XPATH_NOT_FOUND"])
    return results

async def scrape_one_url(page, url, xpaths, tries=0):
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_load_state("networkidle")
        data = await get_stuff_from_page(page, xpaths)
        return {"url": url, "data": data, "ok": True}
    except:
        if tries < 3:
            await asyncio.sleep(5)
            return await scrape_one_url(page, url, xpaths, tries + 1)
        return {"url": url, "data": [ERRORS["NOT_REACHABLE"]] * len(xpaths), "ok": False}

async def do_the_scraping(urls, xpaths, sheet):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        done = 0
        for start in range(0, len(urls), NUM_AT_ONCE):
            group = urls[start:start + NUM_AT_ONCE]
            fake_profiles = [await browser.new_context(user_agent=random.choice(FAKE_BROWSERS)) for _ in group]
            pages = [await profile.new_page() for profile in fake_profiles]
            jobs = [scrape_one_url(pages[i], group[i], xpaths) for i in range(len(group))]
            results = await asyncio.gather(*jobs)
            for result in results:
                if result["ok"]:
                    row = [result["url"]] + result["data"]
                    sheet.append_row(row)
                    done += 1
                    print(f"✅ {result['url'][:50]}...")
                else:
                    print(f"❌ {result['url']}")
            for profile, page in zip(fake_profiles, pages):
                await profile.close()
            await asyncio.sleep(WAIT_TIME * NUM_AT_ONCE)
            print(f"Batch: {start + len(group)} / {len(urls)}")
        await browser.close()
    return done

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
    total_done = asyncio.run(do_the_scraping(urls, xpaths, sheet))
    print(f"Done! {total_done}/{len(urls)} in sheet.")

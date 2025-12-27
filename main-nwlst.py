import asyncio
import json
import os
import random
import pandas as pd
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials

NUM_AT_ONCE = 15
WAIT_TIME = 1.5

ERRORS = {
    "NOT_REACHABLE": "Either URL is incorrect or it is not reachable",
    "XPATH_NOT_FOUND": "Could not reach to this location",
    "NO_DATA": "Blank [No Data Available]"
}

FAKE_BROWSERS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
]

async def get_stuff_from_page(page, xpaths):
    results = []
    for idx, xpath in enumerate(xpaths):
        try:
            print(f"  [INFO] Trying field {idx+1}: {xpath[:70]}...")
            if "/@" in xpath:
                part1, part2 = xpath.split("/@")
                part2 = "innerHTML" if part2 == "html" else part2
                elem = await page.wait_for_selector(part1, timeout=20000)
                value = await elem.get_attribute(part2) if elem else None
            else:
                elem = await page.wait_for_selector(xpath, timeout=20000, state="visible")
                if elem:
                    await elem.scroll_into_view_if_needed()
                    value = await elem.inner_text(timeout=10000)
                    if not value.strip():
                        value = await elem.get_attribute("innerText")
                    if not value.strip():
                        html = await elem.inner_html()
                        value = html.strip()
                else:
                    value = None
            final = value.strip() if value else ERRORS["NO_DATA"]
            results.append(final if final else ERRORS["XPATH_NOT_FOUND"])
            print(f"  [SUCCESS] Got: {final[:60] if final else 'Empty'}")
        except Exception as e:
            print(f"  [ERROR] Field {idx+1} failed: {str(e)[:100]}")
            results.append(ERRORS["XPATH_NOT_FOUND"])
    return results

async def scrape_one_url(page, url, xpaths):
    try:
        print(f"[INFO] Loading: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=30000)
        
        await page.evaluate("""async () => {
            await new Promise((resolve) => {
                let totalHeight = 0;
                const distance = 100;
                const timer = setInterval(() => {
                    const scrollHeight = document.body.scrollHeight;
                    window.scrollBy(0, distance);
                    totalHeight += distance;
                    if (totalHeight >= scrollHeight - window.innerHeight) {
                        clearInterval(timer);
                        resolve();
                    }
                }, 100);
            });
        }""")
        await asyncio.sleep(5)
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(2)
        data = await get_stuff_from_page(page, xpaths)
        return {"url": url, "data": data, "ok": True}
    except Exception as e:
        print(f"[ERROR] URL failed: {url} | {str(e)[:150]}")
        return {"url": url, "data": [ERRORS["NOT_REACHABLE"]] * len(xpaths), "ok": False}

async def do_the_scraping(urls, xpaths, sheet):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        done = 0
        for start in range(0, len(urls), NUM_AT_ONCE):
            group = urls[start:start + NUM_AT_ONCE]
            contexts = [await browser.new_context(user_agent=random.choice(FAKE_BROWSERS)) for _ in group]
            for ctx in contexts:
                await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => false});")
            pages = [await ctx.new_page() for ctx in contexts]
            jobs = [scrape_one_url(pages[i], group[i], xpaths) for i in range(len(group))]
            results = await asyncio.gather(*jobs)  # results defined here
            for result in results:  # Proper indentation
                row = [result["url"]] + result["data"]
                sheet.append_row(row)  # Always append
                if result["ok"]:
                    done += 1
            for ctx in contexts:
                await ctx.close()
            await asyncio.sleep(WAIT_TIME * NUM_AT_ONCE)
            print(f"Batch done: {start + len(group)} / {len(urls)}")
        await browser.close()
    return done

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
    if len(urls) == 0:
        print("No URLs found – check CSV!")
    xpaths = df.iloc[1, 1:].dropna().tolist()
    print(f"Found {len(xpaths)} XPaths")
    sheet = connect_to_sheet()
    total_done = asyncio.run(do_the_scraping(urls, xpaths, sheet))
    print(f"All finished! {total_done}/{len(urls)} URLs processed.")

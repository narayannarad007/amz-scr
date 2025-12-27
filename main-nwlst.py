import asyncio
import json
import os
import random
import pandas as pd
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials

# Changed: Lower for stability, increase later
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
            print(f"  [INFO] Trying field {idx+1}: {xpath[:70]}...")  # NEW: Log each XPath
            if "/@" in xpath:
                part1, part2 = xpath.split("/@")
                part2 = "innerHTML" if part2 == "html" else part2
                elem = await page.wait_for_selector(part1, timeout=20000)  # Increased timeout
                value = await elem.get_attribute(part2) if elem else None
            else:
                # Increased timeout + visible state
                elem = await page.wait_for_selector(xpath, timeout=20000, state="visible")
                if elem:
                    await elem.scroll_into_view_if_needed()  # Scroll to load
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
            print(f"  [SUCCESS] Got: {final[:60] if final else 'Empty'}")  # Log result
        except Exception as e:
            print(f"  [ERROR] Field {idx+1} failed: {str(e)[:100]}")  # Log error
            results.append(ERRORS["XPATH_NOT_FOUND"])
    return results

async def scrape_one_url(page, url, xpaths):
    try:
        print(f"[INFO] Loading: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=30000)
        
        # NEW: Full scroll to load all lazy elements (like images/prices)
        await page.evaluate("""async () => {
            await new Promise((resolve, reject) => {
                var totalHeight = 0;
                var distance = 100;
                var timer = setInterval(() => {
                    var scrollHeight = document.body.scrollHeight;
                    window.scrollBy(0, distance);
                    totalHeight += distance;
                    if(totalHeight >= scrollHeight - window.innerHeight){
                        clearInterval(timer);
                        resolve();
                    }
                }, 100);
            });
        }""")
        await asyncio.sleep(5)  # Extra time for JS
        
        data = await get_stuff_from_page(page, xpaths)
        return {"url": url, "data": data, "ok": True}
    except Exception as e:
        print(f"[ERROR] URL failed: {url} | {str(e)[:150]}")
        return {"url": url, "data": [ERRORS["NOT_REACHABLE"]] * len(xpaths), "ok": False}

# Rest unchanged (do_the_scraping, connect_to_sheet, main)
# ... (same as your last version, but append row ALWAYS so sheet gets data/errors)

# In do_the_scraping: Change to append row even on failure
for result in results:
    row = [result["url"]] + result["data"]
    sheet.append_row(row)  # Always append → sheet shows errors
    if result["ok"]:
        done += 1

# ... rest same

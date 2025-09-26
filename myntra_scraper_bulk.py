# myntra_scraper_bulk.py
import time
import math
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, ElementClickInterceptedException, WebDriverException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import sys

INPUT_CSV = "urls.csv"            # expects a column "URL"
OUTPUT_XLSX = "myntra_bulk_output.xlsx"
PINCODE = "400706"
DELAY_BETWEEN = 2.2                   # seconds between pages (tweakable)
RETRY_LIMIT = 3
SAVE_EVERY = 20                       # save to file every N results
RESTART_EVERY = 200                   # restart browser every N pages to avoid leaks
HEADLESS = False                      # set True to run headless

# --- driver helpers (same ideas as single script) ---
def start_driver(headless=False):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1400,1000")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    # Optional user-agent rotation could be added here later
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.maximize_window()
    return driver

def safe_find(driver, by, value, timeout=6):
    try:
        return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))
    except TimeoutException:
        return None

def enter_pincode(driver, pincode):
    attempts = [
        (By.CSS_SELECTOR, "input[placeholder*='pincode']"),
        (By.CSS_SELECTOR, "input[placeholder*='PIN']"),
        (By.CSS_SELECTOR, "input[name*='pincode']"),
        (By.CSS_SELECTOR, "input[id*='pincode']"),
        (By.CSS_SELECTOR, "input[aria-label*='pincode']"),
    ]
    for by, sel in attempts:
        el = safe_find(driver, by, sel, timeout=2)
        if el:
            try:
                el.clear()
                el.send_keys(pincode)
                btn_selectors = [
                    (By.XPATH, "//button[contains(., 'Check')]"),
                    (By.XPATH, "//button[contains(., 'Apply')]"),
                ]
                for bby, bsel in btn_selectors:
                    trybtn = safe_find(driver, bby, bsel, timeout=1)
                    if trybtn:
                        trybtn.click()
                        time.sleep(0.5)
                        return True
                el.send_keys("\n")
                time.sleep(0.5)
                return True
            except Exception:
                continue
    return False

def click_first_size(driver):
    try:
        candidates = [
            "div.pdp-size-layout .size-buttons button",
            "ul.size-list li button",
            "button.size-buttons",
            "div.size-list button"
        ]
        for sel in candidates:
            elems = driver.find_elements(By.CSS_SELECTOR, sel)
            if elems:
                for e in elems:
                    try:
                        if e.is_displayed() and e.is_enabled():
                            e.click()
                            time.sleep(0.6)
                            return True
                    except ElementClickInterceptedException:
                        driver.execute_script("arguments[0].scrollIntoView(true);", e)
                        time.sleep(0.3)
                        try:
                            e.click()
                            time.sleep(0.6)
                            return True
                        except Exception:
                            continue
        buttons = driver.find_elements(By.TAG_NAME, "button")
        for b in buttons:
            txt = b.text.strip().upper()
            if txt in ("S", "M", "L", "XL", "XS"):
                try:
                    b.click()
                    time.sleep(0.6)
                    return True
                except Exception:
                    continue
    except Exception:
        pass
    return False

def find_seller_name(driver):
    try:
        # Try direct text containing 'Sold by' or 'Seller'
        try:
            sold_by_elem = driver.find_element(By.XPATH, "//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sold by') or contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'seller')]")
        except Exception:
            sold_by_elem = None
        if sold_by_elem:
            text = sold_by_elem.text.strip()
            if text:
                return text

        candidate_selectors = [
            "div.sellerBlock",
            "div.seller-name",
            "a.seller-link",
            "span.seller-name",
            "div[itemprop='seller']",
            "div.pdp-seller-info"
        ]
        for sel in candidate_selectors:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                if el and el.text.strip():
                    return el.text.strip()
            except NoSuchElementException:
                continue

        elems = driver.find_elements(By.XPATH, "//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'sold by') or contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'seller:')]")
        for e in elems:
            txt = e.text.strip()
            if txt and len(txt) < 300:
                return txt
    except Exception:
        pass
    return None

# --- single-page scraped wrapped with retries ---
def scrape_single_with_retries(driver, url):
    attempt = 0
    last_exc = None
    while attempt < RETRY_LIMIT:
        try:
            driver.get(url)
            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(1.0)
            enter_pincode(driver, PINCODE)
            click_first_size(driver)
            seller = find_seller_name(driver)
            return seller
        except WebDriverException as e:
            last_exc = e
            attempt += 1
            wait = 1.5 * (2 ** attempt)
            print(f"Webdriver error on attempt {attempt} for {url}. Waiting {wait:.1f}s then retrying...")
            time.sleep(wait)
            # try to recover by restarting the driver externally (handled by caller)
    print(f"Failed to scrape {url} after {RETRY_LIMIT} attempts. Last exception: {last_exc}")
    return None

def run_bulk():
    df_input = pd.read_csv(INPUT_CSV)
    if "URL" not in df_input.columns:
        print("Input CSV must contain column named 'URL'.")
        return

    results = []
    driver = start_driver(headless=HEADLESS)
    processed = 0
    restart_count = 0

    try:
        for idx, row in df_input.iterrows():
            url = str(row["URL"]).strip()
            if not url:
                results.append({"URL": url, "Seller Name": ""})
                continue

            # Optionally restart driver every RESTART_EVERY pages
            if processed and processed % RESTART_EVERY == 0:
                print("Restarting browser to avoid session problems...")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = start_driver(headless=HEADLESS)
                restart_count += 1
                time.sleep(1)

            seller = scrape_single_with_retries(driver, url)
            results.append({"URL": url, "Seller Name": seller or ""})
            processed += 1
            print(f"[{processed}/{len(df_input)}] {url} -> {seller}")
            # save periodically
            if processed % SAVE_EVERY == 0:
                pd.DataFrame(results).to_excel(OUTPUT_XLSX, index=False)
                print(f"Saved intermediate results to {OUTPUT_XLSX}")

            time.sleep(DELAY_BETWEEN)  # polite pause

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        # final save
        pd.DataFrame(results).to_excel(OUTPUT_XLSX, index=False)
        print(f"Finished. Final results saved to {OUTPUT_XLSX}. Browser restarted {restart_count} times.")

if __name__ == "__main__":
    run_bulk()
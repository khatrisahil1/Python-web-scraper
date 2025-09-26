# myntra_scraper_bulk.py
import time
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

INPUT_CSV = "urls.csv"          # update as needed
OUTPUT_XLSX = "myntra_bulk_output_v2.xlsx"
PINCODE = "400706"
DELAY_BETWEEN = 2.2
RETRY_LIMIT = 3
SAVE_EVERY = 20
RESTART_EVERY = 200
HEADLESS = False

def start_driver(headless=False):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1400,1000")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
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
    """
    Returns:
      True -> clicked a size (assume desired size available)
      False -> no size clicked
      'no_desired' -> size options exist but not desired size (useful if you want to check specific size text)
    """
    try:
        candidates = [
            "div.pdp-size-layout .size-buttons button",
            "ul.size-list li button",
            "button.size-buttons",
            "div.size-list button"
        ]
        found_any = False
        for sel in candidates:
            elems = driver.find_elements(By.CSS_SELECTOR, sel)
            if elems:
                found_any = True
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
        # fallback: check for size abbreviations but do not click if unavailable
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
        if found_any:
            return 'no_desired'
    except Exception:
        pass
    return False

def find_seller_name(driver):
    try:
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

def scrape_single_with_retries(driver, url):
    attempt = 0
    last_exc = None
    while attempt < RETRY_LIMIT:
        try:
            driver.get(url)
            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(1.0)
            enter_pincode(driver, PINCODE)
            size_click_res = click_first_size(driver)
            seller = find_seller_name(driver)
            return size_click_res, seller, None  # None for no exception
        except WebDriverException as e:
            last_exc = e
            attempt += 1
            wait = 1.5 * (2 ** attempt)
            print(f"Webdriver error on attempt {attempt} for {url}. Waiting {wait:.1f}s then retrying...")
            time.sleep(wait)
    return None, None, last_exc

def run_bulk():
    df_input = pd.read_csv(INPUT_CSV)
    results = []
    driver = start_driver(headless=HEADLESS)
    processed = 0
    restart_count = 0

    try:
        for idx, row in df_input.iterrows():
            url = str(row["URL"]).strip()
            if not url:
                results.append({"URL": url, "Seller Name": "", "Status": "404", "Notes": "Empty URL"})
                continue

            if processed and processed % RESTART_EVERY == 0:
                driver.quit()
                driver = start_driver(headless=HEADLESS)
                restart_count += 1
                time.sleep(1)

            size_click_res, seller, exc = scrape_single_with_retries(driver, url)
            status = ""
            notes = ""
            if exc:
                status = "500"
                notes = f"webdriver_error: {type(exc).__name__}"
                seller = seller or ""
            else:
                # interpret size result
                if size_click_res is True:
                    # clicked a size OK
                    if seller:
                        status = "200"
                        notes = "success"
                    else:
                        status = "404"
                        notes = "seller_not_found"
                elif size_click_res == 'no_desired':
                    status = "401"
                    notes = "size_options_present_but_no_clickable"
                elif size_click_res is False:
                    # no size options found; maybe one-size or page layout different
                    if seller:
                        status = "200"
                        notes = "success_no_size_needed"
                    else:
                        status = "404"
                        notes = "seller_not_found_no_size"
                else:
                    status = "500"
                    notes = "unknown_flow"

            results.append({
                "URL": url,
                "Seller Name": seller or "",
                "Status": status,
                "Notes": notes
            })
            processed += 1
            print(f"[{processed}/{len(df_input)}] {url} -> {seller} (status {status})")

            if processed % SAVE_EVERY == 0:
                pd.DataFrame(results).to_excel(OUTPUT_XLSX, index=False)
                print(f"Saved intermediate results to {OUTPUT_XLSX}")

            time.sleep(DELAY_BETWEEN)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        pd.DataFrame(results).to_excel(OUTPUT_XLSX, index=False)
        print(f"Finished. Final results saved to {OUTPUT_XLSX}. Browser restarted {restart_count} times.")

if __name__ == "__main__":
    run_bulk()
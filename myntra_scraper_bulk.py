# myntra_scraper_bulk_sequential.py - Sequential single-driver processing


import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys  # For pressing Enter and Escape
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, ElementClickInterceptedException, WebDriverException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os

# --- Configuration Parameters ---
CONFIG = {
    "PINCODE": "560037",
    "INPUT_XLSX": "dataset_20k.xlsx",
    "OUTPUT_XLSX": "myntra_output_two.xlsx",
    "NUM_WORKERS": 1,            # kept for compatibility / informational only
    "URL_LIMIT": 50,
    "INCREMENTAL_SAVE_INTERVAL": 10,
    "HEADLESS": False,  # Set to False for GUI
}

# --- Selenium Setup ---
def start_driver(headless=False):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1400,1000")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Anti-detection measures
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_argument("--disable-infobars")

    try:
        service = Service(ChromeDriverManager().install())
    except Exception as e:
        print(f"Error installing ChromeDriver: {e}. Please ensure you have a stable internet connection.")
        raise

    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.maximize_window()
    return driver

# --- Helper Functions  ---
def safe_find(driver, by, value, timeout=5):
    try:
        return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))
    except TimeoutException:
        return None


def safe_click(driver, element, timeout=5):
    try:
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(element)).click()
        return True
    except (TimeoutException, ElementClickInterceptedException):
        try:
            driver.execute_script("arguments[0].scrollIntoView(true);", element)  # Scroll into view
            time.sleep(0.5)
            WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(element)).click()
            return True
        except Exception:
            return False
    except Exception:
        return False


def handle_popups(driver):
    """Attempts to close common Myntra pop-ups."""
    try:
        close_button_app = safe_find(driver, By.CSS_SELECTOR, "div.app-container span.css-xyxdrg", timeout=2)
        if close_button_app and safe_click(driver, close_button_app):
            time.sleep(1)
            return True
    except Exception:
        pass

    try:
        close_icon_generic = safe_find(driver, By.XPATH, "//div[@class='desktop-previos-btn']/span", timeout=2)
        if close_icon_generic and safe_click(driver, close_icon_generic):
            time.sleep(1)
            return True
    except Exception:
        pass

    try:
        x_button_modal = safe_find(driver, By.XPATH, "//div[contains(@class, 'modal-content')]//button[contains(text(), 'X')]", timeout=2)
        if x_button_modal and safe_click(driver, x_button_modal):
            time.sleep(1)
            return True
    except Exception:
        pass

    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        time.sleep(1)
    except Exception:
        pass

    return False


def enter_pincode(driver, pincode):
    attempts = [
        (By.CSS_SELECTOR, "input[placeholder*='Pincode']"),
        (By.CSS_SELECTOR, "input[placeholder*='PIN']"),
        (By.CSS_SELECTOR, "input[name*='pincode']"),
        (By.CSS_SELECTOR, "input[id*='pincode']"),
        (By.CSS_SELECTOR, "input[aria-label*='pincode']"),
        (By.XPATH, "//label[contains(text(), 'Pincode')]/following-sibling::input")
    ]
    for by, sel in attempts:
        el = safe_find(driver, by, sel, timeout=5)
        if el:
            try:
                el.clear()
                el.send_keys(pincode)

                btn_selectors = [
                    (By.XPATH, "//button[contains(., 'Check')]),"),
                    (By.XPATH, "//button[contains(., 'Apply')]),"),
                    (By.XPATH, "//button[contains(., 'CHECK')]),"),
                    (By.XPATH, "//button[contains(., 'Apply Pincode')]),"),
                    (By.CSS_SELECTOR, "div.pincode-check-container button"),
                ]
                for bby, bsel in btn_selectors:
                    trybtn = safe_find(driver, bby, bsel, timeout=1)
                    if trybtn and safe_click(driver, trybtn, timeout=2):
                        time.sleep(1.5)
                        return True

                el.send_keys(Keys.ENTER)
                time.sleep(1.5)
                return True
            except Exception:
                continue
    return False


def click_first_size(driver):
    candidates = [
        "div.size-buttons-details button",
        "div.pdp-size-layout button",
        "ul.size-list li button",
        "button.size-buttons",
        "button.size-option",
        "div.size-list button"
    ]
    for sel in candidates:
        elems = driver.find_elements(By.CSS_SELECTOR, sel)
        if elems:
            for e in elems:
                try:
                    if e.is_displayed() and e.is_enabled() and 'selected' not in (e.get_attribute('class') or ''):
                        if safe_click(driver, e):
                            time.sleep(1.2)
                            return True
                except Exception:
                    continue
    buttons = driver.find_elements(By.TAG_NAME, "button")
    for b in buttons:
        txt = b.text.strip().upper()
        if txt in ("S", "M", "L", "XL", "XS", "XXL", "XXXL"):
            try:
                if b.is_displayed() and b.is_enabled() and 'selected' not in (b.get_attribute('class') or ''):
                    if safe_click(driver, b):
                        time.sleep(1.2)
                        return True
            except Exception:
                continue
    return False


def find_seller_name(driver):
    try:
        seller_element = safe_find(driver, By.CSS_SELECTOR, "div.supplier-supplier span.supplier-productSellerName", timeout=5)
        if seller_element and seller_element.text.strip():
            return seller_element.text.strip()

        candidate_selectors = [
            "span.seller-name",
            "div.pdp-seller-info span.supplier-name",
            "div.pdp-seller-info a.seller-link",
            "div.item-seller-details span",
        ]
        for sel in candidate_selectors:
            el = safe_find(driver, By.CSS_SELECTOR, sel, timeout=2)
            if el and el.text.strip():
                return el.text.strip()

        xpath_selectors = [
            "//span[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sold by')]/following-sibling::*",
            "//div[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sold by')]/span",
            "//div[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sold by')]",
            "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'seller:')]",
        ]
        for xpath_sel in xpath_selectors:
            elems = driver.find_elements(By.XPATH, xpath_sel)
            for el in elems:
                text = el.text.strip()
                if text:
                    lower_text = text.lower()
                    if "sold by" in lower_text:
                        parts = lower_text.split("sold by", 1)
                        if len(parts) > 1 and parts[1].strip():
                            name = parts[1].strip()
                            if "manufacturer" in name:
                                name = name.split("manufacturer")[0].strip()
                            return name
                    elif "seller:" in lower_text:
                        parts = lower_text.split("seller:", 1)
                        if len(parts) > 1 and parts[1].strip():
                            return parts[1].strip()
                    elif 2 < len(text) < 100:
                        return text
    except Exception:
        pass
    return None


def get_delivery_info(driver):
    try:
        delivery_message_selectors = [
            "div.pincode-serviceability-message",
            "div.pdp-pincode-info div.pincode-message span.pincode-details",
            "div.pdp-pincode-info span.pincode-message",
            "span.pincode-details",
            "div.delivery-message span",
            "li.delivery-option h4",
        ]
        for sel in delivery_message_selectors:
            elem = safe_find(driver, By.CSS_SELECTOR, sel, timeout=5)
            if elem and elem.text.strip():
                text = elem.text.strip()
                lower_text = text.lower()
                if "get it by" in lower_text:
                    text = text[lower_text.find("get it by") + len("get it by"):].strip()
                if " - " in text:
                    text = text.split(" - ")[0].strip()
                return text
    except Exception:
        pass

    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
        for line in body_text.splitlines():
            lower_line = line.strip().lower()
            if "get it by" in lower_line and len(line) < 150:
                text = line.strip()
                if text.lower().startswith("get it by"):
                    text = text[lower_line.find("get it by") + len("get it by"):].strip()
                if " - " in text:
                    text = text.split(" - ")[0].strip()
                return text
    except Exception:
        pass

    return None

# --- Main Scraper Function (for a single URL) ---
def scrape_url(url_no, url, driver):
    data = {
        "URL_No": url_no,
        "URL": url,
        "SellerNames": "",
        "SellerIDs": "",
        "Delivery": "",
        "Status": "404",
        "Notes": ""
    }

    initial_url = url

    try:
        print(f"\n--- Starting scrape #{url_no}: {url} ---")
        driver.get(url)

        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(3)  # Extra buffer for heavy pages

        if "404" in driver.current_url or "error" in driver.current_url:
            data["Status"] = "404"
            data["Notes"] = "Page redirected to a 404 or error page."
            print(f"#{url_no} redirected to {driver.current_url}. Skipping further processing.")
            return data

        handle_popups(driver)
        time.sleep(1)

        pincode_entered = enter_pincode(driver, CONFIG["PINCODE"])
        if not pincode_entered:
            data["Notes"] += "Pincode input not found or not interactable; "

        size_clicked = click_first_size(driver)
        if not size_clicked:
            data["Notes"] += "No size clicked (may be single size, all sizes unavailable, or selectors need tuning); "

        seller = find_seller_name(driver)
        if seller:
            data["SellerNames"] = seller
        else:
            data["Notes"] += "Seller not found; "

        delivery = get_delivery_info(driver)
        if delivery:
            data["Delivery"] = delivery
        else:
            data["Notes"] += "Delivery info not found; "

        if seller or delivery:
            data["Status"] = "200"
        else:
            if not data["Notes"]:
                data["Notes"] = "Page loaded, but no relevant data (seller/delivery) found; "
            if data["Status"] == "404" and "404" not in data["Notes"]:
                data["Notes"] = "No seller or delivery data and no explicit redirect/error; " + data["Notes"]

    except TimeoutException:
        data["Status"] = "408"
        data["Notes"] = "Page load timeout (took too long to load body element); "
    except NoSuchElementException as e:
        data["Status"] = "404"
        data["Notes"] = f"Element not found error during scraping: {e}; "
    except ElementClickInterceptedException as e:
        data["Status"] = "400"
        data["Notes"] = f"Click intercepted, likely by an overlay: {e}; "
    except WebDriverException as e:
        data["Status"] = "500"
        data["Notes"] = f"WebDriver communication error or browser crash: {e}; "
    except Exception as e:
        data["Status"] = "500"
        data["Notes"] = f"Unexpected error during scraping: {e}; "
    finally:
        current_url = driver.current_url if driver else "N/A"
        print(f"Finished #{url_no} | {initial_url} | Current URL: {current_url} | Status: {data['Status']} | Seller: {data['SellerNames']} | Delivery: {data['Delivery']} | Notes: {data['Notes']}")
    return data

# --- Sequential Orchestration (single instance at a time) ---

def main():
    print("Starting Myntra Scraper (sequential single-instance mode)...")
    print(f"Configuration: {CONFIG}")

    try:
        df_input = pd.read_excel(CONFIG["INPUT_XLSX"])
        # keep original indices to preserve URL_No
        indexed_urls = list(df_input["URL"].items())  # returns list of (index, url)
        # Convert to 1-based numbering matching original file
        indexed_urls = [(idx + 1, url) for idx, url in enumerate(df_input["URL"].tolist())]

        if CONFIG["URL_LIMIT"] is not None:
            indexed_urls = indexed_urls[:CONFIG["URL_LIMIT"]]

        print(f"Loaded {len(indexed_urls)} URLs from {CONFIG['INPUT_XLSX']}. (Using original file order and numbering)")
    except FileNotFoundError:
        print(f"Error: Input file '{CONFIG['INPUT_XLSX']}' not found.")
        return
    except KeyError:
        print(f"Error: 'URL' column not found in '{CONFIG['INPUT_XLSX']}'.")
        return

    all_results = []
    processed_set = set()
    if os.path.exists(CONFIG["OUTPUT_XLSX"]):
        try:
            existing_df = pd.read_excel(CONFIG["OUTPUT_XLSX"])
            all_results.extend(existing_df.to_dict('records'))
            print(f"Loaded {len(existing_df)} existing records from {CONFIG['OUTPUT_XLSX']}.")
            processed_set = set(existing_df["URL"].tolist())
            # Filter out already processed URLs while preserving original numbering
            indexed_urls = [(i, u) for (i, u) in indexed_urls if u not in processed_set]
            print(f"Remaining {len(indexed_urls)} URLs to scrape (excluding already processed).")
        except Exception as e:
            print(f"Warning: Could not load existing '{CONFIG['OUTPUT_XLSX']}' for appending: {e}")

    processed_count = 0

    for url_no, url in indexed_urls:
        driver = None
        try:
            # Start one driver instance for this URL
            driver = start_driver(headless=CONFIG["HEADLESS"])
        except Exception as e:
            print(f"Could not start driver for URL #{url_no} ({url}): {e}")
            # record a failed row and continue
            failed_row = {
                "URL_No": url_no,
                "URL": url,
                "SellerNames": "",
                "SellerIDs": "",
                "Delivery": "",
                "Status": "500",
                "Notes": f"Failed to start webdriver: {e};"
            }
            all_results.append(failed_row)
            processed_count += 1
            if processed_count % CONFIG["INCREMENTAL_SAVE_INTERVAL"] == 0:
                pd.DataFrame(all_results).to_excel(CONFIG["OUTPUT_XLSX"], index=False)
            continue

        try:
            result = scrape_url(url_no, url, driver)
            all_results.append(result)
            processed_count += 1

            if processed_count % CONFIG["INCREMENTAL_SAVE_INTERVAL"] == 0:
                print(f"\n--- Saving {processed_count} results incrementally to {CONFIG['OUTPUT_XLSX']} ---")
                pd.DataFrame(all_results).to_excel(CONFIG["OUTPUT_XLSX"], index=False)
                print("--- Incremental save complete ---")

        except Exception as e:
            print(f"Unexpected error scraping URL #{url_no} ({url}): {e}")
            all_results.append({
                "URL_No": url_no,
                "URL": url,
                "SellerNames": "",
                "SellerIDs": "",
                "Delivery": "",
                "Status": "500",
                "Notes": f"Unhandled exception during scrape: {e};"
            })
            processed_count += 1
        finally:
            # Ensure driver is closed before moving to next URL
            try:
                if driver:
                    driver.quit()
            except Exception:
                pass

    print(f"\nScraping complete. Saving all {len(all_results)} results to {CONFIG['OUTPUT_XLSX']}")
    pd.DataFrame(all_results).to_excel(CONFIG["OUTPUT_XLSX"], index=False)
    print("All results saved.")


if __name__ == "__main__":
    main()

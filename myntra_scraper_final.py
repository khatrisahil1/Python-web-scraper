# myntra_scraper_bulk.py - FINAL WITH SELLER NAME FIX AND ROBUSTNESS
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys # For pressing Enter and Escape
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, ElementClickInterceptedException, WebDriverException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# --- Configuration Parameters ---
CONFIG = {
    "PINCODE": "560037",
    "INPUT_XLSX": "dataset_20k.xlsx",
    "OUTPUT_XLSX": "myntra_output_two.xlsx",
    "NUM_WORKERS": 6,
    "URL_LIMIT": 11,
    "INCREMENTAL_SAVE_INTERVAL": 10,
    "HEADLESS": False, # Set to False for debugging, True for production
    
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
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--disable-infobars")

    try:
        service = Service(ChromeDriverManager().install())
    except Exception as e:
        print(f"Error installing ChromeDriver: {e}. Please ensure you have a stable internet connection.")
        raise

    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.maximize_window()
    return driver

# --- Helper Functions (Updated for robustness) ---
def safe_find(driver, by, value, timeout=10): # Increased default timeout
    try:
        return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))
    except TimeoutException:
        # print(f"Debug: Element not found within {timeout}s: {by}={value}")
        return None

def safe_click(driver, element, timeout=5):
    try:
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(element)).click()
        return True
    except (TimeoutException, ElementClickInterceptedException):
        # print(f"Debug: Could not click element {element.tag_name} with text '{element.text}'")
        driver.execute_script("arguments[0].scrollIntoView(true);", element) # Scroll into view
        time.sleep(0.5)
        try:
            WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(element)).click() # Try again after scroll
            return True
        except Exception:
            return False
    except Exception:
        return False

def handle_popups(driver):
    """Attempts to close common Myntra pop-ups."""
    # Try to close "Download App" banner (common Myntra popup)
    try:
        close_button_app = safe_find(driver, By.CSS_SELECTOR, "div.app-container span.css-xyxdrg", timeout=2)
        if close_button_app and safe_click(driver, close_button_app):
            print("Debug: Clicked app download popup close button.")
            time.sleep(1)
            return True
    except Exception:
        pass
    
    # Another common close icon pattern for modals/popups
    try:
        close_icon_generic = safe_find(driver, By.XPATH, "//div[@class='desktop-previos-btn']/span", timeout=2)
        if close_icon_generic and safe_click(driver, close_icon_generic):
            print("Debug: Clicked generic popup close icon.")
            time.sleep(1)
            return True
    except Exception:
        pass

    # Generic "x" button in a modal
    try:
        x_button_modal = safe_find(driver, By.XPATH, "//div[contains(@class, 'modal-content')]//button[contains(text(), 'X')]", timeout=2)
        if x_button_modal and safe_click(driver, x_button_modal):
            print("Debug: Clicked generic 'X' modal close button.")
            time.sleep(1)
            return True
    except Exception:
        pass

    # Attempt to dismiss with ESC key (for browser-level prompts or other modals)
    try:
        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
        print("Debug: Sent ESC key to dismiss a potential popup.")
        time.sleep(1)
    except Exception:
        pass
    
    return False

def enter_pincode(driver, pincode):
    # print("Debug: Attempting to enter pincode...")
    attempts = [
        (By.CSS_SELECTOR, "input[placeholder*='Pincode']"), # Myntra often uses capitalized Pincode
        (By.CSS_SELECTOR, "input[placeholder*='PIN']"),
        (By.CSS_SELECTOR, "input[name*='pincode']"),
        (By.CSS_SELECTOR, "input[id*='pincode']"),
        (By.CSS_SELECTOR, "input[aria-label*='pincode']"),
        (By.XPATH, "//label[contains(text(), 'Pincode')]/following-sibling::input") # Find label then input
    ]
    for by, sel in attempts:
        el = safe_find(driver, by, sel, timeout=5)
        if el:
            # print(f"Debug: Pincode input found using {by}={sel}. Interacting...")
            try:
                el.clear()
                el.send_keys(pincode)
                
                btn_selectors = [
                    (By.XPATH, "//button[contains(., 'Check')]"),
                    (By.XPATH, "//button[contains(., 'Apply')]"),
                    (By.XPATH, "//button[contains(., 'CHECK')]"),
                    (By.XPATH, "//button[contains(., 'Apply Pincode')]"),
                    (By.CSS_SELECTOR, "div.pincode-check-container button"), # Generic button in pincode section
                ]
                for bby, bsel in btn_selectors:
                    trybtn = safe_find(driver, bby, bsel, timeout=1)
                    if trybtn and safe_click(driver, trybtn, timeout=2):
                        # print(f"Debug: Pincode check/apply button clicked using {bby}={bsel}.")
                        time.sleep(1.5) # Give time for delivery info to update
                        return True
                
                el.send_keys(Keys.ENTER)
                # print("Debug: Sent ENTER key after pincode.")
                time.sleep(1.5)
                return True
            except Exception as e:
                # print(f"Debug: Pincode interaction error for {by}={sel}: {e}")
                continue
    # print("Debug: Failed to find or interact with pincode input after all attempts.")
    return False

def click_first_size(driver):
    # print("Debug: Attempting to click a size button...")
    candidates = [
        "div.size-buttons-details button", # Most common Myntra pattern for active sizes
        "div.pdp-size-layout button",
        "ul.size-list li button",
        "button.size-buttons",
        "button.size-option",
        "div.size-list button"
    ]
    for sel in candidates:
        elems = driver.find_elements(By.CSS_SELECTOR, sel)
        if elems:
            # print(f"Debug: Found {len(elems)} potential size buttons using {sel}.")
            for e in elems:
                try:
                    # Check if it's visible, enabled, and NOT already selected
                    # Myntra often applies 'selected' class to the pre-chosen size
                    if e.is_displayed() and e.is_enabled() and 'selected' not in e.get_attribute('class'):
                        if safe_click(driver, e):
                            # print(f"Debug: Clicked size button: {e.text}")
                            time.sleep(1.2)
                            return True
                except Exception as ex:
                    # print(f"Debug: Error clicking size button {e.text}: {ex}")
                    continue
    # Fallback: click any <button> with text like 'S', 'M', 'L' visible and not selected
    buttons = driver.find_elements(By.TAG_NAME, "button")
    for b in buttons:
        txt = b.text.strip().upper()
        if txt in ("S", "M", "L", "XL", "XS", "XXL", "XXXL"):
            try:
                if b.is_displayed() and b.is_enabled() and 'selected' not in b.get_attribute('class'):
                    if safe_click(driver, b):
                        # print(f"Debug: Fallback clicked size button: {b.text}")
                        time.sleep(1.2)
                        return True
            except Exception:
                continue
    # print("Debug: Failed to click any size button after all attempts.")
    return False

# --- UPDATED find_seller_name based on screenshot ---
def find_seller_name(driver):
    # print("Debug: Attempting to find seller name...")
    try:
        # **PRIMARY STRATEGY: Using the precise selector from the screenshot**
        # The seller name is in a span with class 'supplier-productSellerName'
        # which is nested under 'supplier-supplier' div/span.
        seller_element = safe_find(driver, By.CSS_SELECTOR, "div.supplier-supplier span.supplier-productSellerName", timeout=5)
        if seller_element and seller_element.text.strip():
            print(f"Debug: Seller found with precise CSS selector: {seller_element.text.strip()}")
            return seller_element.text.strip()
        
        # Secondary Strategy: Look for other common Myntra patterns
        candidate_selectors = [
            "span.seller-name",
            "div.pdp-seller-info span.supplier-name",
            "div.pdp-seller-info a.seller-link",
            "div.item-seller-details span",
        ]
        for sel in candidate_selectors:
            el = safe_find(driver, By.CSS_SELECTOR, sel, timeout=2)
            if el and el.text.strip():
                print(f"Debug: Seller found via secondary CSS '{sel}': {el.text.strip()}")
                return el.text.strip()
        
        # Fallback Strategy: Generic "Sold by" text parsing
        xpath_selectors = [
            "//span[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sold by')]/following-sibling::*",
            "//div[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sold by')]/span",
            "//div[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sold by')]",
            "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'seller:')]"
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
                            print(f"Debug: Seller found via fallback XPath '{xpath_sel}': {name}")
                            return name
                    elif "seller:" in lower_text:
                        parts = lower_text.split("seller:", 1)
                        if len(parts) > 1 and parts[1].strip():
                            print(f"Debug: Seller found via fallback XPath '{xpath_sel}': {parts[1].strip()}")
                            return parts[1].strip()
                    elif len(text) > 2 and len(text) < 100:
                         print(f"Debug: Potentially found seller name via fallback XPath '{xpath_sel}': {text}")
                         return text

    except Exception as e:
        print(f"Debug: Error in find_seller_name: {e}")
    # print("Debug: Seller name not found after all attempts.")
    return None

def get_delivery_info(driver):
    # print("Debug: Attempting to get delivery info...")
    try:
        # Priority 1: Specific elements that appear after pincode entry
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
                if text:
                    lower_text = text.lower()
                    if "get it by" in lower_text:
                        text = text[lower_text.find("get it by") + len("get it by"):].strip()
                    if " - " in text:
                        text = text.split(" - ")[0].strip()
                    print(f"Debug: Delivery info found via CSS '{sel}': {text}")
                    return text
    except Exception as e:
        print(f"Debug: Error in get_delivery_info (CSS): {e}")

    # Priority 2: Fallback to scanning body text
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
                print(f"Debug: Delivery info found via text scan: {text}")
                return text
    except Exception as e:
        print(f"Debug: Error in get_delivery_info (text scan): {e}")
    
    # print("Debug: Delivery info not found after all attempts.")
    return None

# --- Main Scraper Function (for a single URL) ---
def scrape_url(url, driver):
    data = {
        "URL": url,
        "SellerNames": "",
        "SellerIDs": "",
        "Delivery": "",
        "Status": "404",
        "Notes": ""
    }
    
    initial_url = url 

    try:
        print(f"\n--- Starting scrape for: {url} ---")
        driver.get(url)
        
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(3) # Extra buffer for heavy pages

        # Check for URL redirection (e.g., to a 404 page)
        if "404" in driver.current_url or "error" in driver.current_url:
            data["Status"] = "404"
            data["Notes"] = "Page redirected to a 404 or error page."
            print(f"Debug: {url} redirected to {driver.current_url}. Skipping further processing.")
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
        print(f"Finished {initial_url} | Current URL: {current_url} | Status: {data['Status']} | Seller: {data['SellerNames']} | Delivery: {data['Delivery']} | Notes: {data['Notes']}")
        
        if (CONFIG["DEBUG_SCREENSHOTS"] and not CONFIG["HEADLESS"] and data["Status"] != "200" and driver):
            screenshot_path = f"debug_error_{initial_url.split('/')[-2]}_{int(time.time())}.png"
            try:
                driver.save_screenshot(screenshot_path)
                print(f"DEBUG: Saved screenshot to {screenshot_path}")
            except Exception as se:
                print(f"DEBUG: Failed to save screenshot: {se}")
    return data

# --- Multi-threaded Orchestration ---
def worker_initializer(headless_mode):
    return start_driver(headless=headless_mode)

def worker_shutdown(driver):
    if driver:
        driver.quit()

def main():
    print("Starting Myntra Scraper...")
    print(f"Configuration: {CONFIG}")

    try:
        df_input = pd.read_excel(CONFIG["INPUT_XLSX"])
        urls_to_scrape = df_input["URL"].tolist()
        if CONFIG["URL_LIMIT"] is not None:
            urls_to_scrape = urls_to_scrape[:CONFIG["URL_LIMIT"]]
        print(f"Loaded {len(urls_to_scrape)} URLs from {CONFIG['INPUT_XLSX']}.")
    except FileNotFoundError:
        print(f"Error: Input file '{CONFIG['INPUT_XLSX']}' not found.")
        return
    except KeyError:
        print(f"Error: 'URL' column not found in '{CONFIG['INPUT_XLSX']}'.")
        return

    all_results = []
    if os.path.exists(CONFIG["OUTPUT_XLSX"]):
        try:
            existing_df = pd.read_excel(CONFIG["OUTPUT_XLSX"])
            all_results.extend(existing_df.to_dict('records'))
            print(f"Loaded {len(existing_df)} existing records from {CONFIG['OUTPUT_XLSX']}.")
            processed_urls = set(existing_df["URL"].tolist())
            urls_to_scrape = [url for url in urls_to_scrape if url not in processed_urls]
            print(f"Remaining {len(urls_to_scrape)} URLs to scrape (excluding already processed).")
        except Exception as e:
            print(f"Warning: Could not load existing '{CONFIG['OUTPUT_XLSX']}' for appending: {e}")

    with ThreadPoolExecutor(max_workers=CONFIG["NUM_WORKERS"]) as executor:
        futures = {executor.submit(scrape_url, url, worker_initializer(CONFIG["HEADLESS"])): url for url in urls_to_scrape}
        
        processed_count = 0
        for future in as_completed(futures):
            url = futures[future]
            try:
                result = future.result()
                all_results.append(result)
                processed_count += 1

                if processed_count % CONFIG["INCREMENTAL_SAVE_INTERVAL"] == 0:
                    print(f"\n--- Saving {processed_count} results incrementally to {CONFIG['OUTPUT_XLSX']} ---")
                    pd.DataFrame(all_results).to_excel(CONFIG["OUTPUT_XLSX"], index=False)
                    print("--- Incremental save complete ---")

            except Exception as e:
                print(f"Error processing URL {url}: {e}")
            finally:
                pass
                
    print(f"\nScraping complete. Saving all {len(all_results)} results to {CONFIG['OUTPUT_XLSX']}")
    pd.DataFrame(all_results).to_excel(CONFIG["OUTPUT_XLSX"], index=False)
    print("All results saved.")
    
    print("Here's a visualization of a successful scraping operation:")
    

if __name__ == "__main__":
    main()
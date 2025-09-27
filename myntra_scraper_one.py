# myntra_scraper_one.py
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, ElementClickInterceptedException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

URL = "https://www.myntra.com/track-pants/cult/cult-men-train-in-train-out-moisture-wicking-premium-trackpants/29417146/buy"
PINCODE = "560037"
OUTPUT_XLSX = "myntra_output_one.xlsx"

def start_driver(headless=False):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
    # Helpful flags for automation stability
    chrome_options.add_argument("--window-size=1400,1000")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.maximize_window()
    return driver

def safe_find(driver, by, value, timeout=8):
    try:
        return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))
    except TimeoutException:
        return None

def enter_pincode(driver, pincode):
    """
    Tries a few different approaches to enter the pincode on Myntra pages.
    Adjust selectors here if Myntra changes their DOM.
    """
    # Try common patterns
    attempts = [
        (By.CSS_SELECTOR, "input[placeholder*='pincode']"),
        (By.CSS_SELECTOR, "input[placeholder*='PIN']"),
        (By.CSS_SELECTOR, "input[name*='pincode']"),
        (By.CSS_SELECTOR, "input[id*='pincode']"),
        (By.CSS_SELECTOR, "input[aria-label*='pincode']"),
    ]
    for by, sel in attempts:
        el = safe_find(driver, by, sel, timeout=3)
        if el:
            try:
                el.clear()
                el.send_keys(pincode)
                # Try to find a nearby button to apply/check
                parent = el.find_element(By.XPATH, "./ancestor::form") if True else None
                # Try typical buttons
                btn_selectors = [
                    (By.XPATH, "//button[contains(., 'Check')]"),
                    (By.XPATH, "//button[contains(., 'Apply')]"),
                    (By.XPATH, "//button[contains(., 'CHECK')]"),
                    (By.XPATH, "//button[contains(., 'Apply Pincode')]"),
                ]
                for bby, bsel in btn_selectors:
                    trybtn = safe_find(driver, bby, bsel, timeout=1)
                    if trybtn:
                        trybtn.click()
                        time.sleep(1)
                        return True
                # If no explicit button found, press Enter in the input
                el.send_keys("\n")
                time.sleep(1)
                return True
            except Exception:
                continue
    # If not found, return False (we'll continue anyway; pincode sometimes not required)
    return False

def click_first_size(driver):
    """
    Clicks the first visible size element (small/medium/etc).
    Adjust the CSS if Myntra uses different classes.
    """
    try:
        # general pattern: size options are buttons or labels; try a few selectors
        candidates = [
            "div.pdp-size-layout .size-buttons button",    # example pattern
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
                        if e.is_displayed() and e.is_enabled():
                            e.click()
                            time.sleep(0.8)
                            return True
                    except ElementClickInterceptedException:
                        driver.execute_script("arguments[0].scrollIntoView(true);", e)
                        time.sleep(0.3)
                        try:
                            e.click()
                            time.sleep(0.8)
                            return True
                        except Exception:
                            continue
        # fallback: click any <button> with text like 'S', 'M', 'L' visible
        buttons = driver.find_elements(By.TAG_NAME, "button")
        for b in buttons:
            txt = b.text.strip().upper()
            if txt in ("S", "M", "L", "XL", "XS"):
                try:
                    b.click()
                    time.sleep(0.8)
                    return True
                except Exception:
                    continue
    except Exception:
        pass
    return False

def find_seller_name(driver):
    """
    Try multiple ways to find the seller. Myntra often shows 'Sold by' or 'Seller' label.
    Update selectors after inspecting the product page if needed.
    """
    # Try patterns that include text like "Sold by" or "Seller"
    try:
        # 1) a label containing 'Sold by' and next sibling text
        sold_by_elem = None
        try:
            sold_by_elem = driver.find_element(By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'sold by') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'seller')]")
        except Exception:
            sold_by_elem = None

        if sold_by_elem:
            # If this element contains both label and seller name, return trimmed text
            text = sold_by_elem.text.strip()
            if text:
                return text

        # 2) look for elements with seller-related attributes or class names
        candidate_selectors = [
            "div.sellerBlock",            # hypothetical
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

        # 3) last resort: look for elements that say 'Sold by' then get sibling text node
        elems = driver.find_elements(By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'sold by') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'seller:')]")
        for e in elems:
            txt = e.text.strip()
            if txt and len(txt) < 200:
                return txt

    except Exception as e:
        print("Error while finding seller:", e)
    return None

def get_delivery_info(driver):
    selectors = [
        "#mountRoot > div > div:nth-child(1) > main > div.pdp-details.common-clearfix > div.pdp-description-container > div:nth-child(2) > div:nth-child(4) > div > div > ul > li:nth-child(1) > h4",
        "div.delivery-info h4",          # fallback selector 1
        "li.delivery-option h4"          # fallback selector 2
    ]
    for sel in selectors:
        try:
            elem = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
            if elem.text.strip():
                text = elem.text.strip()
                # Remove "Get it by" prefix and any trailing pincode info
                if text.lower().startswith("get it by"):
                    text = text[len("get it by"):].strip()
                if " - " in text:
                    text = text.split(" - ")[0].strip()
                return text
        except Exception:
            continue
    # fallback: scan page text for a "get it by" line
    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
        for line in body_text.splitlines():
            if "get it by" in line.lower():
                text = line.strip()
                if text.lower().startswith("get it by"):
                    text = text[len("get it by"):].strip()
                if " - " in text:
                    text = text.split(" - ")[0].strip()
                return text
    except Exception:
        pass
    return None

def scrape_one(url, headless=False):
    driver = start_driver(headless=headless)
    # Initialize result dictionary with required columns
    data = {
        "URL": url,
        "SellerNames": "",
        "SellerIDs": "",
        "Delivery": "",
        "Status": "404",
        "Notes": ""
    }
    try:
        driver.get(url)
        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(1.2)  # let dynamic parts load
        pincode_entered = enter_pincode(driver, PINCODE)
        if pincode_entered:
            print("Pincode entered.")
            time.sleep(1)
        else:
            print("Pincode input not detected or not entered (okay if irrelevant).")

        size_clicked = click_first_size(driver)
        if size_clicked:
            print("Clicked a size.")
        else:
            print("No size clicked — maybe only one size or selectors need tuning.")

        seller = find_seller_name(driver)
        if seller:
            print("Found seller:", seller)
            data["SellerNames"] = seller
        else:
            print("Seller not found with current selectors. Inspect the page and update selectors.")
            data["Notes"] += "Seller not found; "

        delivery = get_delivery_info(driver)
        if delivery:
            print("Found delivery:", delivery)
            data["Delivery"] = delivery
        else:
            print("Delivery not found using the specified selector.")
            data["Notes"] += "Delivery not found; "

        if seller or delivery:
            data["Status"] = "200"
        else:
            data["Status"] = "404"
        # SellerIDs remains empty as no extraction logic provided
        return data
    finally:
        driver.quit()

def main():
    result = scrape_one(URL, headless=False)  # set headless=True if desired
    # Build DataFrame with required columns
    import pandas as pd
    df = pd.DataFrame([result])  # columns: URL, SellerNames, SellerIDs, Delivery, Status, Notes
    df.to_excel(OUTPUT_XLSX, index=False)
    print(f"Saved results to {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
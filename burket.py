import re
import time
import json
import os
import csv
import logging
import requests
import random
from random import randint, uniform
import mysql.connector
from datetime import datetime
from bs4 import BeautifulSoup
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import FirefoxOptions
from datetime import datetime, timedelta, date
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from random import randint
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.common.exceptions import NoSuchElementException
from concurrent.futures import ThreadPoolExecutor
from undetected_chromedriver import ChromeOptions
from selenium.common.exceptions import ElementClickInterceptedException, JavascriptException
from selenium.webdriver.common.action_chains import ActionChains
from decimal import Decimal, InvalidOperation
import undetected_chromedriver as uc
from modules.runTimeSecrets import HOST, DB, USER, PASS, HOST2, DB2, USER2, PASS2, HOST3, DB3, USER3, PASS3
from modules.saveRanks import commence as evalRanking

def loggerInit(logFileName):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
    file_handler = logging.FileHandler(f'logs/{logFileName}')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
    return logger
logger = loggerInit(logFileName="app.log")
# ---------------------------------------------------------------

# ----------------------------------------------------------------------

# ---------------------- WEBDRIVER SETUP ----------------------
def triggerSelenium(useVPN=True, checkIP=False):
    logger.debug(f"Selenium triggered")
    
    with open("/root/public/Directscraper/HahnAppliance/vpn.config.json") as json_data_file:
        configs = json.load(json_data_file)

    attempts = 0
    while attempts < 3:
        try:
            VPN_IP_PORT = configs['VPN_IP_PORT'][random.randint(0, len(configs['VPN_IP_PORT']) - 1)]

            seleniumwire_options = {
                'proxy': {
                    "http": f"http://{VPN_IP_PORT}",
                    "https": f"http://{VPN_IP_PORT}",
                    'no_proxy': 'localhost,127.0.0.1'
                }
            }

            options = FirefoxOptions()
            # options.add_argument('-headless')
            # options.add_argument('-private')
            if useVPN:
                driver = webdriver.Firefox(options=options,seleniumwire_options=seleniumwire_options)
            else:
                driver = webdriver.Firefox(options=options)
            try:
                if checkIP:
                    time.sleep(2)
                    driver.get("https://api.ipify.org?format=json")
                    time.sleep(2)
                    for request in driver.requests:
                        if request.response:
                            if 'ipify' in request.url:
                                ip_info = json.loads(request.response.body.decode('utf-8'))
                                ip_address = ip_info.get('ip', 'IP not found')
                                logger.debug(f"Current IP: {ip_address}")
                                break
                time.sleep(2)
                try:
                    close_btn = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[aria-label="Close"]'))
                    )
                    close_btn.click()
                except:
                    pass  # No modal appeared
                return driver
            except:
                driver.get_screenshot_as_file(f"logs/IpException({VPN_IP_PORT}).png")
                driver.quit()
                raise Exception("BadSession")
        except Exception as e:
            logger.debug(f"Attempt {attempts+1}/10 failed with error: {e}")
            print(f"Attempt {attempts+1}/10 failed with error: {e}")
            attempts += 1
            if attempts == 3:
                logger.debug(f"triggerSelenium() failed after 10 attempts")
                print(f"triggerSelenium() failed after 10 attempts")
                raise e

def create_stealth_driver():
    """Create a stealth Chrome driver with anti-detection measures"""
    options = uc.ChromeOptions()
    
    # Random user agents
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    
    # Basic anti-detection arguments (compatible with undetected_chromedriver)
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    options.add_argument('--disable-images')
    options.add_argument('--disable-javascript')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--disable-ipc-flooding-protection')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--disable-background-timer-throttling')
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-client-side-phishing-detection')
    options.add_argument('--disable-component-extensions-with-background-pages')
    options.add_argument('--disable-default-apps')
    options.add_argument('--disable-domain-reliability')
    options.add_argument('--disable-features=TranslateUI')
    options.add_argument('--disable-hang-monitor')
    options.add_argument('--disable-prompt-on-repost')
    options.add_argument('--disable-sync')
    options.add_argument('--disable-translate')
    options.add_argument('--metrics-recording-only')
    options.add_argument('--no-first-run')
    options.add_argument('--safebrowsing-disable-auto-update')
    options.add_argument('--enable-automation')
    options.add_argument('--password-store=basic')
    options.add_argument('--use-mock-keychain')
    options.add_argument('--force-device-scale-factor=1')
    options.add_argument('--high-dpi-support=1')
    options.add_argument('--force-color-profile=srgb')
    options.add_argument('--disable-background-networking')
    
    # Set random user agent
    options.add_argument(f'--user-agent={random.choice(user_agents)}')
    
    # Window size
    options.add_argument('--window-size=1920,1080')
    
    # Use compatible experimental options
    try:
        options.add_experimental_option("prefs", {
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_settings.popups": 0,
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.media_stream": 2,
        })
    except Exception as e:
        logger.debug(f"Could not set experimental options: {e}")
    
    try:
        # Try with version_main parameter first
        driver = uc.Chrome(options=options, version_main=141)
    except Exception as e:
        logger.debug(f"Failed with version_main=141, trying without: {e}")
        try:
            # Try without version_main parameter
            driver = uc.Chrome(options=options)
        except Exception as e2:
            logger.debug(f"Failed without version_main, trying with basic options: {e2}")
            # Try with minimal options
            basic_options = uc.ChromeOptions()
            basic_options.add_argument('--no-sandbox')
            basic_options.add_argument('--disable-dev-shm-usage')
            basic_options.add_argument('--disable-blink-features=AutomationControlled')
            basic_options.add_argument(f'--user-agent={random.choice(user_agents)}')
            driver = uc.Chrome(options=basic_options)
    
    # Execute stealth scripts
    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
        driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
        driver.execute_script("Object.defineProperty(navigator, 'permissions', {get: () => {query: () => Promise.resolve({state: 'granted'})}})")
        
        # Remove webdriver property
        driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;")
        driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;")
        driver.execute_script("delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;")
    except Exception as e:
        logger.debug(f"Could not execute stealth scripts: {e}")
    
    return driver

def handle_press_and_hold(driver):
    """Handle press and hold verification if it appears"""
    try:
        logger.debug("Checking for press and hold verification...")
        
        # Wait a moment for any verification elements to load
        time.sleep(3)
        
        # Log current page info for debugging
        logger.debug(f"Current URL: {driver.current_url}")
        logger.debug(f"Page title: {driver.title}")
        
        # Check if we're on an access denied or blocked page
        page_source = driver.page_source.lower()
        if "access denied" in page_source or "blocked" in page_source or "security check" in page_source:
            logger.warning("Detected access denied or security check page")
        
        # Comprehensive list of selectors for verification elements
        verification_selectors = [
            # Press and hold specific
            'div[class*="press"]',
            'div[class*="hold"]',
            'button[class*="press"]',
            'button[class*="hold"]',
            'div[class*="press-hold"]',
            'div[class*="hold-button"]',
            'div[class*="verification-button"]',
            'div[class*="verify-button"]',
            
            # General verification elements
            'div[class*="verification"]',
            'div[class*="captcha"]',
            'div[class*="challenge"]',
            'div[class*="security"]',
            'div[class*="bot-check"]',
            'div[class*="human-verification"]',
            'div[class*="human-check"]',
            
            # Button elements
            'button[class*="verification"]',
            'button[class*="captcha"]',
            'button[class*="challenge"]',
            'button[class*="security"]',
            'button[class*="verify"]',
            'button[class*="check"]',
            
            # Interactive elements
            'div[role="button"]',
            'div[tabindex]',
            'div[onclick]',
            'div[class*="clickable"]',
            
            # Common verification patterns
            'div[class*="slider"]',
            'div[class*="puzzle"]',
            'div[class*="checkbox"]',
            'div[class*="recaptcha"]',
            'div[class*="turnstile"]',
            
            # Text-based identifiers (using XPath)
            '//div[contains(text(), "Press")]',
            '//div[contains(text(), "Hold")]',
            '//div[contains(text(), "Verify")]',
            '//div[contains(text(), "Human")]',
            '//div[contains(text(), "Security")]',
            '//div[contains(text(), "Check")]',
            '//button[contains(text(), "Press")]',
            '//button[contains(text(), "Hold")]',
            '//button[contains(text(), "Verify")]',
            
            # Additional patterns
            'div[class*="interactive"]',
            'div[class*="click"]',
            'div[class*="tap"]',
            'div[class*="touch"]'
        ]
        
        # Try to find and interact with verification elements
        for selector in verification_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    logger.debug(f"Found verification element with selector: {selector}")
                    
                    for element in elements:
                        try:
                            # Scroll element into view
                            driver.execute_script("arguments[0].scrollIntoView(true);", element)
                            time.sleep(1)
                            
                            # Try different interaction methods
                            interaction_methods = [
                                # Method 1: Click and hold with ActionChains
                                lambda: ActionChains(driver).click_and_hold(element).pause(3).release().perform(),
                                
                                # Method 2: JavaScript click and hold
                                lambda: driver.execute_script("""
                                    var element = arguments[0];
                                    var mouseDown = new MouseEvent('mousedown', {
                                        bubbles: true,
                                        cancelable: true,
                                        view: window
                                    });
                                    element.dispatchEvent(mouseDown);
                                    setTimeout(function() {
                                        var mouseUp = new MouseEvent('mouseup', {
                                            bubbles: true,
                                            cancelable: true,
                                            view: window
                                        });
                                        element.dispatchEvent(mouseUp);
                                    }, 3000);
                                """, element),
                                
                                # Method 3: Simple click
                                lambda: driver.execute_script("arguments[0].click();", element),
                                
                                # Method 4: Mouse hover then click and hold
                                lambda: ActionChains(driver).move_to_element(element).click_and_hold().pause(3).release().perform(),
                                
                                # Method 5: Touch events (for mobile-like behavior)
                                lambda: driver.execute_script("""
                                    var element = arguments[0];
                                    var touchStart = new TouchEvent('touchstart', {
                                        bubbles: true,
                                        cancelable: true,
                                        view: window
                                    });
                                    element.dispatchEvent(touchStart);
                                    setTimeout(function() {
                                        var touchEnd = new TouchEvent('touchend', {
                                            bubbles: true,
                                            cancelable: true,
                                            view: window
                                        });
                                        element.dispatchEvent(touchEnd);
                                    }, 3000);
                                """, element),
                                
                                # Method 6: Press and hold with longer duration
                                lambda: ActionChains(driver).click_and_hold(element).pause(5).release().perform(),
                                
                                # Method 7: Multiple clicks
                                lambda: driver.execute_script("""
                                    var element = arguments[0];
                                    for(var i = 0; i < 3; i++) {
                                        element.click();
                                        setTimeout(function(){}, 500);
                                    }
                                """, element),
                                
                                # Method 8: Mouse movement simulation
                                lambda: driver.execute_script("""
                                    var element = arguments[0];
                                    var rect = element.getBoundingClientRect();
                                    var centerX = rect.left + rect.width / 2;
                                    var centerY = rect.top + rect.height / 2;
                                    
                                    // Mouse move to element
                                    var moveEvent = new MouseEvent('mousemove', {
                                        bubbles: true,
                                        cancelable: true,
                                        view: window,
                                        clientX: centerX,
                                        clientY: centerY
                                    });
                                    element.dispatchEvent(moveEvent);
                                    
                                    // Mouse down
                                    var mouseDown = new MouseEvent('mousedown', {
                                        bubbles: true,
                                        cancelable: true,
                                        view: window,
                                        clientX: centerX,
                                        clientY: centerY
                                    });
                                    element.dispatchEvent(mouseDown);
                                    
                                    // Hold for 3 seconds
                                    setTimeout(function() {
                                        var mouseUp = new MouseEvent('mouseup', {
                                            bubbles: true,
                                            cancelable: true,
                                            view: window,
                                            clientX: centerX,
                                            clientY: centerY
                                        });
                                        element.dispatchEvent(mouseUp);
                                    }, 3000);
                                """, element)
                            ]
                            
                            for i, method in enumerate(interaction_methods):
                                try:
                                    logger.debug(f"Trying interaction method {i+1}")
                                    method()
                                    time.sleep(2)
                                    
                                    # Check if verification was successful
                                    current_url = driver.current_url
                                    if "ajmadison.com" in current_url:
                                        logger.debug("Verification appears successful")
                                        return True
                                        
                                except Exception as e:
                                    logger.debug(f"Interaction method {i+1} failed: {e}")
                                    continue
                                    
                        except Exception as e:
                            logger.debug(f"Error interacting with element: {e}")
                            continue
                            
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {e}")
                continue
        
        # Check for iframes (common for verification challenges)
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        logger.debug(f"Found {len(iframes)} iframes")
        
        for iframe in iframes:
            try:
                driver.switch_to.frame(iframe)
                logger.debug("Switched to iframe")
                
                # Look for verification elements inside iframe
                iframe_elements = driver.find_elements(By.CSS_SELECTOR, 
                    'div[class*="press"], div[class*="hold"], div[class*="verification"], button[class*="verification"]')
                
                for element in iframe_elements:
                    try:
                        logger.debug("Found verification element in iframe")
                        ActionChains(driver).click_and_hold(element).pause(3).release().perform()
                        time.sleep(2)
                    except Exception as e:
                        logger.debug(f"Error with iframe element: {e}")
                        continue
                        
                driver.switch_to.default_content()
                
            except Exception as e:
                logger.debug(f"Error handling iframe: {e}")
                driver.switch_to.default_content()
                continue
        
        # Wait a bit more for any redirects to complete
        time.sleep(5)
        
        # Check if we're still on the target site
        current_url = driver.current_url
        page_title = driver.title.lower()
        
        logger.debug(f"Final URL after verification: {current_url}")
        logger.debug(f"Final page title: {driver.title}")
        
        if "ajmadison.com" in current_url and "access denied" not in page_title and "blocked" not in page_title:
            logger.debug("Successfully handled verification or no verification found")
            return True
        else:
            logger.warning(f"Still on verification page or redirected away. Current URL: {current_url}")
            return False
                
    except Exception as e:
        logger.error(f"Error in handle_press_and_hold: {e}")
        return False

def human_like_scroll(driver):
    """Perform human-like scrolling"""
    scroll_height = driver.execute_script("return document.body.scrollHeight")
    viewport_height = driver.execute_script("return window.innerHeight")
    
    # Scroll down in small increments
    current_position = 0
    while current_position < scroll_height:
        scroll_amount = random.randint(100, 300)
        current_position += scroll_amount
        driver.execute_script(f"window.scrollTo(0, {current_position});")
        time.sleep(random.uniform(0.5, 1.5))
    
    # Scroll back up partially
    scroll_up_amount = random.randint(200, 500)
    driver.execute_script(f"window.scrollTo(0, {scroll_height - scroll_up_amount});")
    time.sleep(random.uniform(0.5, 1.0))

def add_human_behavior(driver):
    """Add human-like behavior to avoid detection"""
    try:
        # Get viewport dimensions
        viewport_width = driver.execute_script("return window.innerWidth;")
        viewport_height = driver.execute_script("return window.innerHeight;")
        
        # Random mouse movements within viewport bounds
        for _ in range(random.randint(2, 5)):
            x = random.randint(50, max(100, viewport_width - 100))
            y = random.randint(50, max(100, viewport_height - 100))
            try:
                # Use JavaScript to move mouse to absolute position
                driver.execute_script(f"""
                    var event = new MouseEvent('mousemove', {{
                        'view': window,
                        'bubbles': true,
                        'cancelable': true,
                        'clientX': {x},
                        'clientY': {y}
                    }});
                    document.elementFromPoint({x}, {y}).dispatchEvent(event);
                """)
                time.sleep(random.uniform(0.1, 0.3))
            except Exception as e:
                logger.debug(f"Mouse movement failed: {e}")
                break
        
        # Random scrolling
        try:
            human_like_scroll(driver)
        except Exception as e:
            logger.debug(f"Scrolling failed: {e}")
        
        # Random clicks on empty areas within bounds
        for _ in range(random.randint(1, 3)):
            x = random.randint(50, max(100, viewport_width - 100))
            y = random.randint(50, max(100, viewport_height - 100))
            try:
                # Use JavaScript to click at absolute position
                driver.execute_script(f"""
                    var element = document.elementFromPoint({x}, {y});
                    if (element) {{
                        var clickEvent = new MouseEvent('click', {{
                            'view': window,
                            'bubbles': true,
                            'cancelable': true,
                            'clientX': {x},
                            'clientY': {y}
                        }});
                        element.dispatchEvent(clickEvent);
                    }}
                """)
                time.sleep(random.uniform(0.5, 1.0))
            except Exception as e:
                logger.debug(f"Random click failed: {e}")
                break
                
    except Exception as e:
        logger.debug(f"Error in add_human_behavior: {e}")

def random_pause(min_time=2, max_time=5):
    """
    Add a random pause to simulate human thinking or waiting.
    """
    time.sleep(uniform(min_time, max_time))

def getAllProUrl(driver,category_url):
    product_urls = set()
    new_category_url = category_url
    # open("soup.txt", "w", encoding="utf-8").write(str(soup))
    # exit()
    pageNumber = 1
    while True:
        if pageNumber == 1:
            url = category_url
        else:
            url = f'{new_category_url}?p={pageNumber}'
            # https://www.burkett.com/brand/blodgett
        
        driver.get(url)
        time.sleep(10)
        logger.debug(url)
        # page = requests.get(url, headers=headers, params=params)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        # open("souppage.txt", "w", encoding="utf-8").write(str(soup))
        # exit()
        products = soup.select('div#amasty-shopby-product-list ol.products.list.items.product-items li a.product-item-link')
        if not products:
            logger.debug(f"No products found on page {pageNumber} for {new_category_url}.")
            break

        for product in products:
            product_url = product.get('href')
            if product_url not in product_urls:
                product_urls.add(product_url)
                with open("ALLproductUrls.txt", "a", encoding="utf-8") as f:
                    f.write(product_url + '\n')

        print(f"Total unique product URLs found so far: {len(product_urls)}")
        pageNumber += 1
        # if pageNumber == 12:
        #     break

    return product_urls

def fetch_data(driver, product_url,vendor_id):
    """Scrape details from a single product page (not category listing)"""
    try:
        temp = {}
        temp2 = {}
        logger.debug(product_url)

        driver.get(product_url)
        time.sleep(2)
        # driver.refresh()
        # random_pause(2,100)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        # soup = BeautifulSoup(driver.page_source, 'html.parser')
        # open("souppage.txt", "w", encoding="utf-8").write(str(soup))
        maindiv = soup.select_one('div.column.main')
        if not maindiv:
            logger.debug(f"Maindiv not found!!")
        else:
            try:
                name_element = maindiv.select_one('h1.page-title span')
                temp["product_name"] = name_element.text.strip()
            except:
                temp["product_name"] = None
            
            if not temp['product_name']:
                print(f"⏩ Skipping product because name not found: {product_url}")
                return None, None    
            
            brand_name = maindiv.select_one('td[data-th="Brand"]').text.strip()
            temp["brand_name"] = brand_name
            
            try:
                mpn = maindiv.select_one('td[data-th="Manufacturer Model Number"]')
                temp["product_mpn"] = mpn.text.replace("Model #","").strip()
            except Exception as e:
                temp["product_mpn"] = None

            try:
                sku = maindiv.select_one('div.product.attribute.sku div')
                temp["vendor_sku"] = sku.text.replace("Model #","").strip()
            except Exception as e:
                temp["vendor_sku"] = None

            temp["product_url"] = product_url
            temp2['url'] = product_url
            # Image
            try:
                img_element = maindiv.select_one('img.fotorama__img')
                temp["product_image"] = img_element.get("src")
            except:
                temp["product_image"] = None

            # Price  span[data-price-type="finalPrice"] span'
            try:
                price_element = maindiv.select_one('div.product-info-price div.price-box span.price-wrapper[data-price-type="finalPrice"] span.price')
                if not price_element:
                    price_element = maindiv.select_one('div.price-box span.price-wrapper span.price')
                base_price = price_element.text.replace("$", "").replace("Sale", "").replace("*", "").replace(",", "").strip()
            except Exception as e:
                base_price = None

            # MSRP / OLD PRICE
            try:
                msrp_element = maindiv.select_one('div.product-info-price div.price-box span.price-wrapper[data-price-type="oldPrice"] span.price')
                if not msrp_element:
                    msrp_element = maindiv.select_one('span[data-price-type="msrpPrice"] span.price')
                msrp = msrp_element.text.replace("$", "").replace("Was", "").replace("*", "").replace(",", "").strip()
            except Exception as e:
                msrp = None
            
            # try:
            #     add_to_cart = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="quantity-selector_button_add-to-cart"]')))
            #     driver.execute_script("arguments[0].click();", add_to_cart)
            #     print("Add to Cart clicked")
            #     time.sleep(5)

            #     try:
            #         price_element = driver.find_element(By.CSS_SELECTOR, 'p[data-testid="cart-item_typography_pricedisplay"]')
            #         base_price = price_element.text.replace("$","").replace("Sale","").replace("*","").replace(",","").strip()
            #     except:
            #         base_price = None

            #     try:
            #         remove_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'p[data-testid="cart-actions_link_clearcart"]')))
            #         driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", remove_button)
            #         time.sleep(0.5)

            #         try:
            #             remove_button.click()
            #             print("Clicked remove button normally.")
            #         except Exception:
            #             driver.execute_script("arguments[0].click();", remove_button)
            #             print("JS clicked remove button (fallback).")
            #         time.sleep(1)
            #     except Exception as e:
            #         print(f"Remove button not found or could not be clicked: {e}")
            # except Exception as e:
            #     logger.debug(f"Error to cart button.")
            

            # import re

            # # Define phrases considered as invalid/unavailable prices
            # invalid_patterns = [
            #     r"^best price!?$",
            #     r"^price unavailable!?$",
            #     r"^call for best price!?$"
            # ]

            # # Normalize and check
            # if page_price is None or str(page_price).strip() == "":
            #     base_price = page_price
            #     page_price = None
            #     In_cart_price = '0'
            # else:
            #     page_price_clean = str(page_price).strip().lower()
            #     # Check if page_price matches any invalid pattern
            #     if any(re.match(p, page_price_clean, re.IGNORECASE) for p in invalid_patterns):
            #         base_price = page_price
            #         page_price = None
            #         In_cart_price = '0'
            #     elif page_price == base_price:
            #         base_price = page_price
            #         page_price = None
            #         In_cart_price = '0'
            #     else:
            #         In_cart_price = '1'
            
            page_price = None
            In_cart_price = '0'
            temp['vendorprice_stock'] = None
            temp2['vendorprice_stock'] = None

            temp['vendorprice_stock_text'] = None
            temp2['vendorprice_stock_text'] = None
            temp['msrp'] = msrp
            temp2["msrp"] = msrp
            temp2['vendorprice_price'] = base_price
            temp2["vendorprice_finalprice"] = base_price
            temp2["product_page_price"] = page_price
            temp2['In_cart_price'] = In_cart_price
            temp2['scraped_by_system'] = "Preeti pc"
            temp2['source'] = "direct_from_website"
            temp2['product_condition'] = "New"


        # ✅ Save to CSV
        csv_file = "ProductCsv.csv"
        fieldnames = ["Name", "Brand", "SKU", "MPN", "base_price", "MSRP", "Image"]

        file_exists = os.path.isfile(csv_file)
        with open(csv_file, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "Name": temp.get("product_name"),
                "Brand": temp.get("brand_name"),
                "SKU": temp.get('vendor_sku'),
                "MPN": temp.get('product_mpn'),
                "base_price": temp.get("base_price"),
                "MSRP": temp.get("msrp"),
                "Image": temp.get("product_image"),
            })
        print("✅ Product data appended to CSV.")

        # ✅ Debug prints
        print("--------------------------------------------------------")
        print(temp)
        print(temp2)
        print("--------------------------------------------------------")

        # ✅ Insert into DB
        product_id, vendor_product_id = insertIntoMsp(temp, vendor_id)
        print("--------------------------------------------------------")
        print(product_id, vendor_product_id)
        print("--------------------------------------------------------")

        # ✅ Vendor price handling
        # if temp2['vendorprice_price'] is None:
        #     logger.debug("vendorprice_price not found!!")
        #     return
        # elif isinstance(temp2['vendorprice_price'], str):
        #     price_lower = temp2['vendorprice_price'].lower()
        #     if 'best price' in price_lower or 'price unavailable' in price_lower or 'call for best price' in price_lower:
        #         logger.debug(f"vendorprice_price not found!! - Price requires contact: {temp2['vendorprice_price']}")
        #         temp2['vendor_call_for_best_price'] = '1'
        #         insertall(product_id, vendor_product_id, temp2, vendor_id)
        #     else:
        #         temp2['vendor_call_for_best_price'] = '0'
        #         insertall(product_id, vendor_product_id, temp2, vendor_id)
        # evalRanking(vendor_id, product_id)

        price_text = temp2.get('vendorprice_price')

        if isinstance(price_text, str):
            # Normalize and clean punctuation
            price_lower = price_text.lower().strip()

            # Match using regex to catch "!" or extra spaces
            if re.search(r'\b(best price|price unavailable|call for best price|none)\b', price_lower):
                logger.debug(f"vendorprice_price not found!! - Price requires contact: {price_text}")
                temp2['vendorprice_price'] = '0.0'
                temp2['vendorprice_finalprice'] = '0.0'
                temp2['product_page_price'] = '0.0'
                temp2['In_cart_price'] = '0'
                temp2['vendor_call_for_best_price'] = '1'
            else:
                temp2['vendor_call_for_best_price'] = '0'
        else:
            temp2['vendor_call_for_best_price'] = '0'

        # Save the record and continue
        if temp2['vendor_call_for_best_price'] == '0' and float(temp2['vendorprice_price']) == 0.0:
            return None
        else:
            insertall(product_id, vendor_product_id, temp2, vendor_id)
        # insertall(product_id, vendor_product_id, temp2, vendor_id)
        evalRanking(vendor_id, product_id)
        return temp, temp2

    except Exception as e:
        print(f"Error getting product details: {e}")
        return []


def clean_value(value):
    """Convert 'N/A', 'null', or empty strings to None, else return stripped value."""
    if value is None:
        return None
    value = str(value).strip()
    if value.lower() in ['n/a', 'na', 'null', '--', '']:
        return None
    return value

def insertIntoMsp(row, vendor_id):
    product_id = vendor_product_id = None  # Initialize to None
    try:
        brand_id = checkInsertBrand(vendor_id, row['brand_name'])
        product_id = checkInsertProduct(vendor_id, brand_id, row['product_mpn'], row['product_name'], row['msrp'], row['product_image'])
        vendor_product_id = checkInsertProductVendor(vendor_id, product_id, row['vendor_sku'], row['product_name'], row['product_url'], row['msrp'])
        checkInsertProductVendorURL(vendor_id, vendor_product_id, row['product_url'])
    except Exception as e:
        logger.error(f"Error in insertIntoMsp: {e}")
    return product_id, vendor_product_id


def getBrandRawName(brand_name):
    letters, numbers, spaces = [], [], []
    for character in brand_name:
        if character.isalpha():
            letters.append(character)
        elif character.isnumeric():
            numbers.append(character)
        elif character.isspace():
            spaces.append(character)
    if len(letters) > 0: raw_name = "".join(spaces + letters)
    else: raw_name = "".join(spaces + numbers)
    return raw_name


# Add brand if doesn't exists
def checkInsertBrand(vendor_id,brand_name):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            this.execute("SELECT brand_id FROM BrandSynonyms WHERE brand_synonym = %s", (brand_name,))
            brand_id = this.fetchone()
            if brand_id:
                logger.info(f"{vendor_id} >> Found brand synonym: {brand_name} ({brand_id[0]})")
                return brand_id[0]
            else:
                brandRawNname = getBrandRawName(brand_name)
                brandRaw = brandRawNname.lower().strip()
                this.execute("SELECT brand_id, brand_name FROM Brand WHERE brand_raw_name = %s",(brandRaw,))
                records = this.fetchone()
                if records:
                    fetchedBrandId = records[0]
                    fetchedBrandName = records[1]
                    if fetchedBrandName != brand_name:
                        insertBrandSynonymsQuery = "INSERT INTO BrandSynonyms (brand_id,brand_synonym) VALUES (%s,%s);"
                        this.execute(insertBrandSynonymsQuery,(fetchedBrandId,brand_name))
                        conn.commit()
                        logger.info(f"Inserted {brandRawNname} as a synonym for {fetchedBrandName}.")
                    else:
                        logger.info(f"{brandRaw} Brand Name Matched")
                        return fetchedBrandId
                else:
                    insertBrandQuery = "INSERT INTO Brand (brand_name,brand_key,brand_raw_name) VALUES (%s,%s,%s);"
                    this.execute(insertBrandQuery,(brand_name,brand_name.replace(" ", "-").lower(),brandRaw))
                    conn.commit()
                    logger.info(f'{vendor_id} >> Added new brand "{brand_name} ({this.lastrowid})".')
                    return this.lastrowid
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_id} >> MySQL ERROR checkInsertBrand() >> {e}")
        logger.warning(f"{vendor_id}, {brand_name}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Add product if doesn't exists
def checkInsertProduct(vendor_id, brand_id, mpn, name, msrp, image):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)

            # Check if product exists
            checkProductQuery = "SELECT product_id, product_name, product_image FROM Product WHERE brand_id = %s AND product_mpn = %s"
            this.execute(checkProductQuery, [brand_id, mpn])
            records = this.fetchone()

            if records is None:
                if msrp != '':
                    insertProductQuery = """INSERT INTO Product (brand_id, product_name, product_mpn, msrp, product_image) VALUES (%s, %s, %s, %s, %s)"""
                    this.execute(insertProductQuery, (brand_id, name, mpn, msrp, image))
                else:
                    insertProductQuery = """INSERT INTO Product (brand_id, product_name, product_mpn, product_image) VALUES (%s, %s, %s, %s)"""
                    this.execute(insertProductQuery, (brand_id, name, mpn, image))
                conn.commit()
                logger.info(f'{vendor_id} >> Added new product with mpn "{mpn} ({this.lastrowid})".')
                return this.lastrowid
            else:
                product_id, product_name, product_image = records
                if product_name is None:
                    this.execute("UPDATE Product SET product_name = %s WHERE product_id = %s", [name, product_id])
                if not product_image or "afsupply" not in product_image.lower():
                    this.execute("UPDATE Product SET product_image = %s WHERE product_id = %s", [image, product_id])
                if msrp != '':
                    this.execute("UPDATE Product SET msrp = %s WHERE product_id = %s AND msrp IS NULL", [msrp, product_id])
                conn.commit()
                logger.info(f'{vendor_id} >> Updated details for product with mpn "{mpn} ({product_id})".')
                return product_id
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_id} >> MySQL ERROR checkInsertProduct() >> {e}")
        logger.warning(f"{vendor_id}, {brand_id}, {mpn}, {name}, {msrp}, {image}")
        return None

    finally:
        if conn.is_connected():
            this.close()
            conn.close()

# Add product vendor if doesn't exists
def checkInsertProductVendor(vendor_id, product_id, sku, name, product_url, msrp):
    try:
        # First check if we have valid input
        if product_id is None:
            logger.warning(f"{vendor_id} >> Cannot insert vendor product: product_id is None")
            return None
            
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            if msrp == '' or msrp is None:
                msrp = None  # or set to 0.0 if you prefer a default value

            checkProductVendorQuery = "SELECT vendor_product_id, product_name FROM ProductVendor WHERE vendor_id = %s AND product_id = %s LIMIT 1"
            this.execute(checkProductVendorQuery, [vendor_id, product_id])
            records = this.fetchone()
            
            # Handle case where no records found
            if records is None:
                # Insert new record
                insertProductVendorQuery = "INSERT INTO ProductVendor (vendor_id, product_id, product_name, vendor_sku, msrp) VALUES (%s, %s, %s, %s, %s)"
                this.execute(insertProductVendorQuery, (vendor_id, product_id, name, sku, msrp))
                conn.commit()
                logger.info(f'{vendor_id} >> Added new product in ProductVendor "{vendor_id} x {product_id}".')
                return this.lastrowid
            else:
                # Update existing record
                # vp_id = int(records[0])
                vp_id, product_name = records
                if product_name == None:
                    this.execute("Update ProductVendor SET product_name = %s WHERE vendor_product_id = %s",[product_name,vp_id])
                updateProductDetailQuery = "UPDATE ProductVendor SET vendor_sku = %s, msrp = %s WHERE vendor_product_id = %s"
                this.execute(updateProductDetailQuery, [sku, msrp, vp_id])
                conn.commit()
                if this.rowcount == 1:
                    logger.info(f'{vendor_id} >> Updated details for vendor_product_id ({vp_id}).')
                logger.info(f'{vendor_id} >> Returned vendor_product_id ({vp_id}).')
                return vp_id
    except mysql.connector.Error as e:
        logger.error(f"{vendor_id} >> MySQL ERROR checkInsertProductVendor() >> {e}")
        return None
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Add product vendor url if doesn't exists
def checkInsertProductVendorURL(vendor_id, vendor_product_id, product_url):
    url = product_url.split('&')[0]
    try:
        if not vendor_product_id:
            logger.warning(f"{vendor_id} >> Invalid vendor_product_id: {vendor_product_id}")
            return  # Exit the function early
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            checkProductVendorURLQuery = "SELECT vendor_product_id FROM VendorURL WHERE vendor_product_id = %s"
            this.execute(checkProductVendorURLQuery, [vendor_product_id,])
            records = this.fetchall()
            if len(records) == 0:
                insertProductVendorURLQuery = "INSERT INTO VendorURL (vendor_product_id, vendor_raw_url, vendor_url) VALUES (%s, %s, %s)"
                this.execute(insertProductVendorURLQuery, [vendor_product_id, product_url, url])
                conn.commit()
                logger.info(f'{vendor_id} >> Added product vendor URL for vendor_product_id "{vendor_product_id}".')
                return this.lastrowid
            else:
                # fatchquary = "SELECT vendor_url_id, vendor_raw_url, vendor_url FROM VendorURL WHERE vendor_product_id = %s"
                # this.execute(fatchquary, [vendor_product_id])
                # results = this.fetchall()
                # if results[0][2] != url:
                # Update the existing record
                updateProductVendorURLQuery = """UPDATE VendorURL SET vendor_raw_url = %s, vendor_url = %s WHERE vendor_product_id = %s"""
                this.execute(updateProductVendorURLQuery, [product_url, url, vendor_product_id])
                conn.commit()
                logger.info(f'{vendor_id} >> Updated product vendor URL for vendor_product_id "{vendor_product_id}".')
                # else:
                #     logger.info(f'{vendor_id} >> Same Product vendor URL already exists for vendor_product_id "{vendor_product_id}".')
                # try:
                #     vendor_url_id, vendor_raw_url, vendor_url = results[0][0], results[0][1], results[0][2]
                #     checkProductVendorURLQuery = "SELECT vendor_bakup_url_id FROM BuilddotcomeDirectScraping_VendorURLBackup WHERE vendor_product_id = %s"
                #     this.execute(checkProductVendorURLQuery, [vendor_product_id,])
                #     Record = this.fetchone()
                #     if Record is None or len(Record) == 0:
                #         insertProductVendorURLQuery = "INSERT INTO BuilddotcomeDirectScraping_VendorURLBackup (vendor_url_id, vendor_product_id, vendor_raw_url, vendor_url) VALUES (%s, %s, %s, %s)"
                #         this.execute(insertProductVendorURLQuery, [vendor_url_id, vendor_product_id, vendor_raw_url, vendor_url])
                #         conn.commit()
                #         logger.info(f'Added product vendor_url for vendor_product_id "{vendor_product_id}" for vendor_bakup_url_id {this.lastrowid}.')
                #     else:
                #         if Record[0] is not None:
                #             fatchquary = "SELECT vendor_url_id, vendor_raw_url, vendor_url FROM BuilddotcomeDirectScraping_VendorURLBackup WHERE vendor_bakup_url_id = %s"
                #             this.execute(fatchquary, [Record[0],])
                #             Records = this.fetchone()
                #             if Records and Records[2] != vendor_url:
                #                 # Update the existing record
                #                 updateProductVendorURLQuery = """UPDATE BuilddotcomeDirectScraping_VendorURLBackup SET vendor_raw_url = %s, vendor_url = %s WHERE vendor_bakup_url_id = %s"""
                #                 this.execute(updateProductVendorURLQuery, [vendor_raw_url, vendor_url, Record[0]])
                #                 conn.commit()
                #                 logger.info(f'Updated vendor_raw_url, vendor_url for vendor_bakup_url_id "{Record[0]}".')
                #             else:
                #                 logger.info(f'Same Product vendor URL already exists for vendor_bakup_url_id "{Record[0]}".')
                # except mysql.connector.Error as e:
                #     logger.warning(f"MySQL ERROR checkInsertProductVendorURL() >> {e}")
                # results.append(Records)
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_id} >> MySQL ERROR checkInsertProductVendorURL() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# call all function into this function
def insertall(product_id, vendor_product_id, temp, vendor_id):
    try:
        price = temp['vendorprice_price']
        if (price is not None and price.strip() != ''):
            vendorTempPricing(vendor_product_id, temp)
            rpVendorPricingHistory(vendor_product_id, temp, vendor_id)
            # productMsrpUpdate(product_id, temp)
            # productVendorMsrpUpdate(vendor_product_id, temp)
        else:
            logger.info(f"Invalid price value: {price}")
    except Exception as e:
        logger.error(f"Error in insertall(): {e}")

def getDatetime():
    currentDatetime = datetime.now()
    return currentDatetime.strftime("%Y-%m-%d %H:%M:%S")

# Temp vnendor pricing data
def vendorTempPricing(vendor_product_id, temp):
    dateTime = getDatetime()
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            checkQuery = "SELECT vendor_product_id FROM TempVendorPricing WHERE vendor_product_id = %s AND source = %s LIMIT 1"
            this.execute(checkQuery, (vendor_product_id, temp['source']))
            records = this.fetchone()
            if records:
                getPricequary = "SELECT * FROM TempVendorPricing WHERE vendor_product_id = %s AND source = 'direct_from_website'"
                this.execute(getPricequary, (records[0],))
                result = this.fetchone()
                savedprice = str(result[2]).strip()
                scrapedprice = str(temp['vendorprice_price']).strip()
                if savedprice == scrapedprice:
                    logger.info(f"Same vendor price already exists for vendor_product_id {vendor_product_id}")
                else:
                    updateQuery = """UPDATE TempVendorPricing SET is_price_changed = %s, price_changed_date = %s WHERE vendor_product_id = %s AND source = %s"""
                    values = ("1", dateTime, vendor_product_id, temp['source'])
                    this.execute(updateQuery, values)
                    conn.commit()
                    logger.info(f"is_price_changed set 1 for vendor_product_id ({vendor_product_id}).")
                updateQuery = """UPDATE TempVendorPricing SET vendorprice_price = %s, vendorprice_finalprice = %s,product_page_price = %s, In_cart_price = %s, vendorprice_date = %s,vendor_call_for_best_price = %s, vendorprice_stock = %s, vendorprice_stock_text = %s, product_condition = %s, is_rp_calculated = %s, is_member = %s, scraped_by_system = %s
                    WHERE vendor_product_id = %s AND source = %s"""
                values = (temp['vendorprice_price'], temp['vendorprice_finalprice'], temp['product_page_price'], temp['In_cart_price'] , dateTime, temp['vendor_call_for_best_price'], temp['vendorprice_stock'], temp['vendorprice_stock_text'] ,temp['product_condition'], '2', '0', temp['scraped_by_system'], vendor_product_id, temp['source'])
                this.execute(updateQuery, values)
                conn.commit()
                logger.info(f"Record Updated for vendor_product_id ({vendor_product_id}) and source ({temp['source']})")
            else:
                insertQuery = """INSERT INTO TempVendorPricing (vendor_product_id, vendorprice_price, vendorprice_finalprice, product_page_price, In_cart_price, vendorprice_date, vendor_call_for_best_price, vendorprice_stock, vendorprice_stock_text, product_condition, source, is_rp_calculated, is_member, scraped_by_system) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s,%s)"""
                values = (vendor_product_id, temp['vendorprice_price'], temp['vendorprice_finalprice'], temp['product_page_price'], temp['In_cart_price'], dateTime, temp['vendor_call_for_best_price'], temp['vendorprice_stock'], temp['vendorprice_stock_text'], temp['product_condition'], temp['source'], '2', '0', temp['scraped_by_system'])
                this.execute(insertQuery, values)
                conn.commit()
                logger.info(f"Record Inserted for vendor_product_id ({vendor_product_id}) and source ({temp['source']})")
    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR vendorTempPricing() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close() 

def get_table_structure(host, db, user, password, table_name):
    """Retrieve column details from a table, preserving the column order."""
    try:
        conn = mysql.connector.connect(host=host, database=db, user=user, password=password)
        cursor = conn.cursor()            
        cursor.execute(f"DESCRIBE {table_name}")
        structure = [(row[0], row[1], row[2], row[3], row[4], row[5]) for row in cursor.fetchall()]  
        # (Column Name, Column Type, NULL, Key, Default, Extra)
    except Exception as e:
        logger.error(f"Error fetching table structure for {table_name}: {e}")
        structure = []
    finally:
        cursor.close()
        conn.close()
    return structure

def match_table_structure(source_structure, target_structure):
    """Find missing columns with full definitions and their correct positions."""
    target_columns = {col[0]: col for col in target_structure}  # {Column Name: Column Details}
    missing_columns = []

    for index, column in enumerate(source_structure):
        col_name, col_type, is_null, key, default, extra = column
        if col_name not in target_columns:
            after_column = source_structure[index - 1][0] if index > 0 else None
            missing_columns.append((col_name, col_type, is_null, key, default, extra, after_column))
    if missing_columns and len(missing_columns) > 0:
        logger.info(f"Missing columns: {missing_columns}")
    logger.info(f"History Table is up-to-date.")
    return missing_columns

def rpVendorPricingHistory(vendor_product_id, temp, vendor_id):
    dateTime = getDatetime()
    try:
        # save to AF/HP if vendor_id is one of them
        if vendor_id == 10021 or vendor_id == 10024: conn = mysql.connector.connect(host=HOST2, database=DB2, user=USER2, password=PASS2)
        else: conn = mysql.connector.connect(host=HOST3, database=DB3, user=USER3, password=PASS3)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            # check if vendor specific vendorPricing table exists or not
            vendor_pricing_table = f"z_{vendor_id}_VendorPricing"
            this.execute(f"""SELECT * 
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{vendor_pricing_table}'
            LIMIT 1""")
            result = this.fetchone()
            source_structure = get_table_structure(HOST, DB, USER, PASS, 'TempVendorPricing')
            if not result:
                logger.info(f"Table {vendor_pricing_table} does not exist. Creating table...")
                column_definitions = []
                primary_key = None  # Store primary key column if exists
                for col_name, col_type, is_null, key, default, extra in source_structure:
                    null_option = "NULL" if is_null == "YES" else "NOT NULL"
                    # Handle default values properly
                    if default is not None:
                        if "timestamp" in col_type.lower() or "datetime" in col_type.lower():
                            default_option = "DEFAULT CURRENT_TIMESTAMP" if default.lower() == "current_timestamp()" else ""
                        else:
                            default_option = f"DEFAULT {repr(default)}"
                    else:
                        default_option = ""
                    extra_option = extra if extra else ""
                    # Ensure AUTO_INCREMENT is properly handled
                    if "auto_increment" in extra.lower():
                        extra_option = "AUTO_INCREMENT"
                        primary_key = col_name  # Store primary key
                    column_definitions.append(f"`{col_name}` {col_type} {null_option} {default_option} {extra_option}")
                create_table_query = f"""
                    CREATE TABLE `{vendor_pricing_table}` (
                        {', '.join(column_definitions)}
                        {f", PRIMARY KEY (`{primary_key}`)" if primary_key else ""}
                    );
                """.strip()
                this.execute(create_table_query)
                conn.commit()
                logger.info(f"Table {vendor_pricing_table} created successfully.")
                logger.info(f"==========================================")
            else:
                if vendor_id == 10021 or vendor_id == 10024:
                    target_structure = get_table_structure(HOST2, DB2, USER2, PASS2, vendor_pricing_table)
                else:
                    target_structure = get_table_structure(HOST3, DB3, USER3, PASS3, vendor_pricing_table)
                missing_columns = match_table_structure(source_structure, target_structure)
                if missing_columns and len(missing_columns) > 0:
                    # Add missing columns if table exists
                    for col_name, col_type, is_null, key, default, extra, after_column in missing_columns:
                        null_option = "NULL" if is_null == "YES" else "NOT NULL"
                        # Handle default values properly
                        if default is not None:
                            if "timestamp" in col_type.lower() or "datetime" in col_type.lower():
                                default_option = "DEFAULT CURRENT_TIMESTAMP" if default.lower() == "current_timestamp()" else ""
                            else:
                                default_option = f"DEFAULT {repr(default)}"
                        else:
                            default_option = ""
                        extra_option = extra if extra else ""
                        after_option = f"AFTER `{after_column}`" if after_column else "FIRST"
                        # Prevent adding AUTO_INCREMENT column incorrectly
                        if "auto_increment" in extra.lower():
                            logger.warning(f"Skipping column `{col_name}` because it has AUTO_INCREMENT.")
                            continue  # Do not add AUTO_INCREMENT column
                        alter_query = f"""
                            ALTER TABLE `{vendor_pricing_table}`
                            ADD COLUMN `{col_name}` {col_type} {null_option} {default_option} {extra_option} {after_option};
                        """.strip()
                        this.execute(alter_query)
                    conn.commit()
                    logger.info(f"Table {vendor_pricing_table} altered successfully.")
                    logger.info(f"==========================================")

            insertQuery = f"""INSERT INTO {vendor_pricing_table} (vendor_product_id, vendorprice_price, vendorprice_finalprice,product_page_price, In_cart_price, vendorprice_date, vendor_call_for_best_price, vendorprice_stock, 
                vendorprice_stock_text, product_condition, source, is_rp_calculated, is_member, scraped_by_system) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            values = (vendor_product_id, temp['vendorprice_price'], temp['vendorprice_finalprice'], temp['product_page_price'], temp['In_cart_price'], dateTime, temp['vendor_call_for_best_price'], temp['vendorprice_stock'],
                       temp['vendorprice_stock_text'], temp['product_condition'], temp['source'], '2', '0', temp['scraped_by_system'])
            this.execute(insertQuery, values)
            conn.commit()
            logger.info(f"Record Inserted for vendor_product_id ({vendor_product_id}) and source ({temp['source']}) In {vendor_pricing_table} history table.")
    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR {vendor_pricing_table} >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Updating MSRF in Product table
def productMsrpUpdate(product_id, temp):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            this.execute("SELECT msrp FROM Product WHERE product_id = %s", (product_id,))
            result = this.fetchone()
            if result:
                # Update MSRP
                if temp['msrp']:
                    this.execute("UPDATE Product SET msrp = %s WHERE product_id = %s", (temp['msrp'], product_id))
                    conn.commit()
                    logger.info(f"Record Updated for product_id ({product_id}).")
    except mysql.connector.Error as e:
        logger.warning(f"{product_id} >> MySQL ERROR productMsrpUpdate() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

# Updating MSRF in ProductVendor table
def productVendorMsrpUpdate(vendor_product_id, temp):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor(buffered=True)
            this.execute("SELECT msrp FROM ProductVendor WHERE vendor_product_id = %s", (vendor_product_id,))
            result = this.fetchone()
            if result:
                # Update MSRP
                if temp['msrp']:
                    this.execute("UPDATE ProductVendor SET msrp = %s WHERE vendor_product_id = %s", (temp['msrp'], vendor_product_id))
                    conn.commit()
                    logger.info(f"Record Updated for vendor_product_id ({vendor_product_id}).")
    except mysql.connector.Error as e:
        logger.warning(f"{vendor_product_id} >> MySQL ERROR productVendorMsrpUpdate() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()
            

def random_boolean():
    return random.choice([True, False])

def getUrls(driver,vendor_id, vendor_url):
    try:
        conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=PASS)
        if conn.is_connected():
            this = conn.cursor()
            getVendorURLQuery = """
                SELECT 
                    ProductVendor.vendor_product_id,
                    Product.product_id,
                    VendorURL.vendor_url 
                FROM VendorURL
                INNER JOIN ProductVendor ON ProductVendor.vendor_product_id = VendorURL.vendor_product_id
                INNER JOIN Product ON Product.product_id = ProductVendor.product_id
                INNER JOIN Brand ON Brand.brand_id = Product.brand_id
                WHERE ProductVendor.vendor_id = %s AND Brand.brand_id = 61
            """
            this.execute(getVendorURLQuery, [vendor_id,])
            url_list = this.fetchall()

            if url_list:
                logger.info(f"Found {len(url_list)} URLs to process")
                # Process URLs sequentially instead of in parallel for better logging
                for value in url_list:
                    vendor_product_id, product_id, url = value[0], value[1], value[2].strip()
                    if "html&" in url: 
                        url = url.split("html&")[0] + "html"
                    logger.info(f"Processing URL: {url}")
                    try:
                        scraper_unit(driver, vendor_product_id, product_id, url, vendor_url, vendor_id)
                    except Exception as e:
                        logger.error(f"Error processing URL {url}: {e}")
                        continue
    except mysql.connector.Error as e:
        logger.warning(f"MySQL ERROR getUrls() >> {e}")
    finally:
        if conn.is_connected():
            conn.close()
            this.close()

def read_product_urls_from_file(filepath):
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip()]

if __name__ == '__main__':
    try:
        start = time.perf_counter()
        vendor_id = 17366
        vendor_url = "https://www.burkett.com/"
        domain = "https://www.burkett.com"

        options = ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--remote-debugging-port=9222")
        driver = uc.Chrome(version_main=141, options=options)

        # category_url = "https://www.burkett.com/brand/blodgett"
        # getAllProUrl(driver,category_url)
        
        product_urls = read_product_urls_from_file("ALLproductUrls.txt")
        if not product_urls:
            logger.debug("No products found in productUrls.txt.")
        else:
            logger.debug(f"Total {len(product_urls)} products found in productUrls.txt.")
            for product_url in product_urls:
                fetch_data(driver,product_url,vendor_id)

        # product_urls = ["https://www.burkett.com/vollrath-71001-full-size-pan-food-warmer-700-watts",
        #                 "https://www.burkett.com/adapter-plate-two-8-3-8-openings",
        #                 "https://www.burkett.com/viking-middleby-vcre515-ss33a-refrigerator-reach-in-999321"
        #                 ]
        # for product_url in product_urls:
        #     fetch_data(driver,product_url)

        end = time.perf_counter()
        logger.debug(f"Total execution time: {end - start:.2f} seconds")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if driver:
            driver.close()
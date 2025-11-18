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
logger = loggerInit(logFileName="Pricing.log")
# ---------------------------------------------------------------

def random_pause(min_time=2, max_time=5):
    """
    Add a random pause to simulate human thinking or waiting.
    """
    time.sleep(uniform(min_time, max_time))

def fetch_data(driver, vendor_product_id, product_id, given_product_mpn, product_url, vendor_id):
    """Scrape details from a single product page (not category listing)"""
    try:
        temp = {}
        temp2 = {}
        logger.debug(product_url)

        driver.get(product_url)
        time.sleep(2)
        base_price = None
        page_price = None
        In_cart_price = '0'
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        maindiv = soup.select_one('div.column.main')
        if not maindiv:
            logger.debug(f"Maindiv not found!!")
        else:
            temp["product_url"] = product_url
            temp2['url'] = product_url
            
            scraped_product_mpn_tag = maindiv.select_one('td[data-th="Manufacturer Model Number"]')
            if scraped_product_mpn_tag:
                scraped_product_mpn = scraped_product_mpn_tag.text.replace("Model #", "").strip()
                temp["product_mpn"] = scraped_product_mpn
            else:
                scraped_product_mpn = None
                temp["product_mpn"] = None

            if given_product_mpn != scraped_product_mpn:
                logger.warning(f"MPN mismatch for {product_url}: given_product_mpn:{given_product_mpn}, scraped_product_mpn:{scraped_product_mpn}")
                with open("OldMpnNotMatched.txt", mode="a", encoding="utf-8") as file:
                    file.write(f"{product_url} | given_product_mpn: {given_product_mpn} | scraped_product_mpn: {scraped_product_mpn}\n")
                return
            else:
                logger.warning(f"MPN matched for {product_url}: given_product_mpn:{given_product_mpn}, scraped_product_mpn:{scraped_product_mpn}")
                # Price
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
             
                # import re
                # # Define phrases considered as invalid/unavailable prices
                # invalid_patterns = [
                #     r"^best price!?$",
                #     r"^price unavailable!?$",
                #     r"^call for best price!?$"
                # ]
                # page_price_clean = str(page_price).strip().lower() if page_price else ""

                # if page_price is None or str(page_price).strip() == "":
                #     base_price = None
                #     page_price = None
                #     In_cart_price = '0'
                # elif any(re.match(p, page_price_clean, re.IGNORECASE) for p in invalid_patterns):
                #     base_price = None
                #     page_price = None
                #     In_cart_price = '0'
                # elif base_price not in (None, 0, "", "0") and str(page_price) == str(base_price):
                #     base_price = page_price
                #     page_price = None
                #     In_cart_price = '0'
                # elif base_price in (None, 0, "", "0") and page_price not in (None, "", 0, "0"):
                #     base_price = page_price
                #     page_price = None
                #     In_cart_price = '0'
                # else:
                #     In_cart_price = '1'

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

                price_text = temp2.get('vendorprice_price')

                if isinstance(price_text, str):
                    # Normalize and clean punctuation
                    price_lower = price_text.lower().strip()

                    # Match "Best Price", "Call for Price", "None", etc.
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

                # ✅ Debug prints
                print("--------------------------------------------------------")
                print(temp2)
                print("--------------------------------------------------------")

                # if temp2['vendor_call_for_best_price'] == '0' and float(temp2['vendorprice_price']) == 0.0:
                #     return None

                # ✅ Correct comparison syntax
                # if temp2['vendor_call_for_best_price'] == '0' and temp2['vendorprice_price'] == '0.0':
                #     return None
                # else:
                #     insertall(product_id, vendor_product_id, temp2, vendor_id)
                    
                # evalRanking(vendor_id, product_id)
                # return temp, temp2

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
                # if product_name is None:
                #     this.execute("UPDATE Product SET product_name = %s WHERE product_id = %s", [name, product_id])
                # if not product_image or "afsupply" not in product_image.lower():
                #     this.execute("UPDATE Product SET product_image = %s WHERE product_id = %s", [image, product_id])
                # if msrp != '':
                #     this.execute("UPDATE Product SET msrp = %s WHERE product_id = %s AND msrp IS NULL", [msrp, product_id])
                # conn.commit()
                logger.info(f'{vendor_id} >> Already details saved for product with mpn "{mpn} ({product_id})".')
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
        # price = temp['vendorprice_price']
        # if (price is not None and price.strip() != ''):
        vendorTempPricing(vendor_product_id, temp)
        rpVendorPricingHistory(vendor_product_id, temp, vendor_id)
            # productMsrpUpdate(product_id, temp)
            # productVendorMsrpUpdate(vendor_product_id, temp)
        # else:
        #     logger.info(f"Invalid price value: {price}")
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
                    Product.product_mpn,
                    VendorURL.vendor_url
                FROM VendorURL
                INNER JOIN ProductVendor ON ProductVendor.vendor_product_id = VendorURL.vendor_product_id
                INNER JOIN TempVendorPricing ON ProductVendor.vendor_product_id = TempVendorPricing.vendor_product_id
                INNER JOIN Product ON Product.product_id = ProductVendor.product_id
                INNER JOIN Brand ON Brand.brand_id = Product.brand_id
                WHERE ProductVendor.vendor_id = %s
            """
            this.execute(getVendorURLQuery, [vendor_id,])
            url_list = this.fetchall()

            if url_list:
                logger.info(f"Found {len(url_list)} URLs to process")
                # Process URLs sequentially instead of in parallel for better logging
                for value in url_list:
                    vendor_product_id, product_id, product_mpn, url = value[0], value[1], value[2], value[3].strip()
                    if "html&" in url: 
                        url = url.split("html&")[0] + "html"
                    logger.info(f"Processing URL: {url}")
                    try:
                        fetch_data(driver, vendor_product_id, product_id, product_mpn, url, vendor_id)
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
        
        getUrls(driver,vendor_id, vendor_url)

        end = time.perf_counter()
        logger.debug(f"Total execution time: {end - start:.2f} seconds")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if driver:
            driver.close()

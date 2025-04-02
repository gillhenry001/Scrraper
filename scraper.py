import re
import time
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import requests
import pandas as pd
from config import CRAIGSLIST_CITIES, CRAIGSLIST_BASE_URL, KEYWORDS, REMOTE_KEYWORDS, NON_REMOTE_KEYWORDS
from utils import random_delay, save_to_csv, load_from_csv, remove_duplicates, get_random_user_agent
import traceback

class CraigslistScraper:
    def __init__(self):
        self.use_headless = os.getenv('USE_HEADLESS', 'false').lower() == 'true'
        self.driver = self._setup_driver()
        self.links_file = os.getenv('LINKS_FILE', 'output/links.csv')
        self.output_file = os.getenv('OUTPUT_FILE', 'output/results.csv')
        self.batch_size = int(os.getenv('BATCH_SIZE', 10))
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        
    def _setup_driver(self):
        """Set up and return a Chrome WebDriver instance."""
        try:
            chrome_options = Options()
            if self.use_headless:
                chrome_options.add_argument("--headless=new")  # Updated for newer Chrome versions
            
            # Add additional Chrome options for stability
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-notifications")
            chrome_options.add_argument("--disable-infobars")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--remote-debugging-port=9222")  # Add debugging port
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # Hide automation
            
            # Add user agent
            chrome_options.add_argument(f"user-agent={get_random_user_agent()}")
            
            print("Setting up ChromeDriver...")
            
            # Use webdriver_manager with specific version
            from webdriver_manager.chrome import ChromeDriverManager
            from selenium.webdriver.chrome.service import Service
            
            # Get Chrome version
            try:
                import subprocess
                chrome_version = subprocess.check_output(
                    ['reg', 'query', 'HKEY_CURRENT_USER\\Software\\Google\\Chrome\\BLBeacon', '/v', 'version'],
                    stderr=subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL
                ).decode('UTF-8').strip().split()[-1]
                print(f"Detected Chrome version: {chrome_version}")
            except:
                print("Could not detect Chrome version, using default driver")
                chrome_version = None
            
            # Install ChromeDriver
            driver_manager = ChromeDriverManager()
            if chrome_version:
                driver_path = driver_manager.install()
                print(f"ChromeDriver installed at: {driver_path}")
            else:
                driver_path = driver_manager.install()
                print(f"ChromeDriver installed at: {driver_path}")
            
            service = Service(driver_path)
            
            # Create driver with increased timeout
            driver = webdriver.Chrome(
                service=service,
                options=chrome_options
            )
            
            # Set page load timeout
            driver.set_page_load_timeout(30)
            print("Chrome WebDriver initialized successfully")
            return driver
            
        except Exception as e:
            print(f"Error setting up Chrome WebDriver: {str(e)}")
            print("Full error details:", traceback.format_exc())
            raise
    
    def _load_page_with_retry(self, url, max_retries=3):
        """Load a page with retries for reliability."""
        for attempt in range(max_retries):
            try:
                self.driver.get(url)
                # Wait for page to be loaded
                WebDriverWait(self.driver, 10).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
                return True
            except Exception:
                if attempt < max_retries - 1:
                    random_delay(3, 5)  # Longer delay between retries
        
        return False

    def _has_keyword(self, text):
        """Check if the text contains any of the keywords defined in config.py."""
        if not text:
            return False
            
        text = text.lower()
        for keyword in KEYWORDS:
            if keyword.lower() in text:
                return True
        return False
        
    def _check_remote_status(self, text):
        """Check if the job is remote, non-remote, or not specified."""
        if not text:
            return "Not Specified"
            
        text = text.lower()
        
        for keyword in REMOTE_KEYWORDS:
            if keyword.lower() in text:
                return "Remote"
                
        for keyword in NON_REMOTE_KEYWORDS:
            if keyword.lower() in text:
                return "Non-Remote"
                
        return "Not Specified"

    def _notify_user_for_captcha(self):
        """Notify the user that CAPTCHA solving is needed."""
        print("=" * 50)
        print("CAPTCHA DETECTED! Please solve the CAPTCHA in the browser window.")
        print("The script will continue automatically after CAPTCHA is solved.")
        print("=" * 50)
        
        # Try to make a sound alert
        try:
            # Beep to alert user (Windows only)
            import winsound
            for _ in range(3):
                winsound.Beep(1000, 500)  # Frequency: 1000Hz, Duration: 500ms
        except:
            # For non-Windows systems, print bell character
            print("\a")

    def _check_for_blocking(self):
        """Check if Craigslist is blocking or throttling our requests."""
        try:
            # Look for common block indicators
            block_indicators = [
                "IP has been automatically blocked",
                "please solve the CAPTCHA below",
                "your connection has been limited",
                "detected unusual activity"
            ]
            
            page_source = self.driver.page_source.lower()
            
            for indicator in block_indicators:
                if indicator.lower() in page_source:
                    time.sleep(120)
                    return True
                    
            return False
        except:
            return False

    def scrape_listings(self, max_listings=None):
        """
        PHASE 1: Scrape job listings from Craigslist.
        """
        all_listings = []
        
        for city in CRAIGSLIST_CITIES:
            url = CRAIGSLIST_BASE_URL.format(city)
            
            if not self._load_page_with_retry(url):
                continue
                
            # Check if we're being blocked
            if self._check_for_blocking():
                pass
            
            random_delay()
            
            # Wait for the results to load - try multiple possible class names
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "result-info"))
                )
            except:
                # Try alternative class name if the first one fails
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "cl-static-search-result"))
                    )
                except:
                    pass
            
            # Try different ways to get listings
            listing_elements = []
            selectors_to_try = [
                "div.result-info",
                "li.cl-static-search-result",
                "div.cl-search-result"
            ]
            
            for selector in selectors_to_try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    listing_elements = elements
                    break
            
            if not listing_elements:
                continue
            
            for element in listing_elements:
                # Check if we've reached the max_listings limit
                if max_listings is not None and len(all_listings) >= max_listings:
                    break
                    
                try:
                    # Try different ways to find title and link
                    title_element = None
                    title_selectors = [
                        "a.posting-title", 
                        "a.title", 
                        "a.cl-app-anchor",
                        "a[data-testid='listing-title']"
                    ]
                    
                    for selector in title_selectors:
                        try:
                            title_element = element.find_element(By.CSS_SELECTOR, selector)
                            if title_element:
                                break
                        except:
                            continue
                    
                    if not title_element:
                        continue
                    
                    title = title_element.text.strip()
                    link = title_element.get_attribute("href")
                    
                    # Try different ways to find post date
                    date_element = None
                    date_selectors = [
                        "div.meta > span:first-child",
                        "time",
                        "span[data-testid='listing-date']",
                        "span.date"
                    ]
                    
                    for selector in date_selectors:
                        try:
                            date_element = element.find_element(By.CSS_SELECTOR, selector)
                            if date_element:
                                break
                        except:
                            continue
                    
                    post_date = "Unknown"
                    if date_element:
                        post_date = date_element.get_attribute("title") or date_element.text.strip()
                    
                    # Check if the title contains any of our keywords
                    if self._has_keyword(title):
                        all_listings.append({
                            "City": city,
                            "Title": title,
                            "Link": link,
                            "Post Date": post_date,
                            "Processed": False
                        })
                except Exception:
                    pass
                
                random_delay(0.5, 1.5)  # Short delay between processing each listing
                
            # Check if we've reached the max_listings limit
            if max_listings is not None and len(all_listings) >= max_listings:
                break
                
            # Random delay between cities
            min_city_delay = float(os.getenv('MIN_DELAY_BETWEEN_CITIES', 5))
            max_city_delay = float(os.getenv('MAX_DELAY_BETWEEN_CITIES', 10))
            random_delay(min_city_delay, max_city_delay)
        
        # Save the listings to CSV
        if all_listings:
            df = save_to_csv(all_listings, self.links_file)
            return df
        else:
            return pd.DataFrame()

    def clean_listings(self, df=None):
        """
        PHASE 2 - STEP 1: Remove duplicate listings with the same title.
        """
        if df is None:
            df = load_from_csv(self.links_file)
            
        if df.empty:
            return df
        
        # Clean up titles to improve duplicate detection
        def normalize_title(title):
            """Normalize titles by removing emojis, extra spaces, and lowercasing"""
            # Remove emojis and special characters
            title = re.sub(r'[^\x00-\x7F]+', '', title)
            # Remove extra spaces
            title = re.sub(r'\s+', ' ', title)
            # Lowercase
            return title.lower().strip()
        
        # Add normalized title for comparison
        df['NormalizedTitle'] = df['Title'].apply(normalize_title)
        
        # Remove duplicates based on normalized title
        df = df.drop_duplicates(subset=['NormalizedTitle'])
        
        # Drop the temporary column used for normalization
        df = df.drop(columns=['NormalizedTitle'])
        
        # Save the cleaned listings back to CSV
        save_to_csv(df, self.links_file)
        
        return df
        
    def _replace_empty_with_null(self, df):
        """Replace empty values with 'null' in rows that have at least some data."""
        # Create a copy to avoid modifying the original
        df_copy = df.copy()
        
        # Get rows that have at least some data (not all columns empty)
        has_data_mask = df_copy.notna().any(axis=1) & (df_copy != "").any(axis=1)
        
        # For rows with data, replace empty values with 'null'
        for idx in df_copy[has_data_mask].index:
            for col in df_copy.columns:
                if pd.isna(df_copy.at[idx, col]) or df_copy.at[idx, col] == "":
                    df_copy.at[idx, col] = "null"
        
        return df_copy

    def scrape_details(self, df=None, start_index=0, max_listings=None):
        """
        PHASE 2 - STEP 2: Visit each listing and extract email, description, and remote status.
        """
        if df is None:
            df = load_from_csv(self.links_file)
            
        if df.empty:
            return pd.DataFrame()
            
        results = []
        total = len(df)
        
        # Handle start_index and max_listings
        if start_index > 0:
            if start_index >= total:
                return pd.DataFrame()
            
            # Keep the original dataframe reference for correct indexing
            filtered_df = df.iloc[start_index:]
        else:
            filtered_df = df
        
        if max_listings is not None:
            filtered_df = filtered_df.iloc[:max_listings]
        
        # Add already processed listings to results
        if start_index > 0:
            already_processed_df = load_from_csv(self.output_file)
            if not already_processed_df.empty:
                for i in range(min(start_index, len(already_processed_df))):
                    results.append(already_processed_df.iloc[i].to_dict())
        
        # Create a loop counter for the remaining items
        remaining_total = len(filtered_df)
        
        # Process each listing
        for idx, (index, row) in enumerate(filtered_df.iterrows(), 1):
            if row.get('Processed', False):
                results.append(row.to_dict())
                continue
                
            listing_data = row.to_dict()
            
            for attempt in range(self.max_retries):
                try:
                    # Visit the listing page
                    if not self._load_page_with_retry(row['Link']):
                        if attempt == self.max_retries - 1:
                            listing_data['Description'] = "Error: Failed to load page"
                            listing_data['Remote'] = "Not Specified"
                            listing_data['Email'] = "Not Available"
                            listing_data['Default Mail'] = ""
                            listing_data['Gmail'] = ""
                            listing_data['Yahoo'] = ""
                            listing_data['Outlook'] = ""
                            listing_data['AOL'] = ""
                            listing_data['Processed'] = True
                            break
                        continue
                    
                    # Check if we're being blocked
                    if self._check_for_blocking():
                        pass
                    
                    random_delay()
                    
                    # Extract the description
                    try:
                        description_element = None
                        desc_selectors = ["#postingbody", "section#postingbody", "div[data-testid='postingbody']"]
                        
                        for selector in desc_selectors:
                            try:
                                description_element = WebDriverWait(self.driver, 10).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                                )
                                if description_element:
                                    break
                            except:
                                continue
                        
                        if description_element:
                            description = description_element.text.strip()
                            listing_data['Description'] = description
                            
                            # Determine if the job is remote
                            remote_status = self._check_remote_status(description)
                            listing_data['Remote'] = remote_status
                        else:
                            listing_data['Description'] = "Description Not Found"
                            listing_data['Remote'] = "Not Specified"
                    except Exception:
                        listing_data['Description'] = ""
                        listing_data['Remote'] = "Not Specified"
                    
                    # Initialize email fields
                    listing_data['Email'] = "Not Available"
                    listing_data['Default Mail'] = ""
                    listing_data['Gmail'] = ""
                    listing_data['Yahoo'] = ""
                    listing_data['Outlook'] = ""
                    listing_data['AOL'] = ""
                    
                    # Try to get email information
                    try:
                        # Find and click the reply button - try multiple selectors
                        reply_button = None
                        reply_selectors = [
                            "button.reply-button",
                            "button[data-href*='/reply/']",
                            "a.reply-button",
                            "a[href*='/reply/']"
                        ]
                        
                        for selector in reply_selectors:
                            try:
                                reply_button = WebDriverWait(self.driver, 5).until(
                                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                                )
                                if reply_button:
                                    break
                            except:
                                continue
                        
                        if not reply_button:
                            pass
                        else:
                            reply_button.click()
                            self._notify_user_for_captcha()
                            
                            # Wait for the user to solve the CAPTCHA and the email button to appear
                            email_found = False
                            email_button_selectors = [
                                "button.reply-option-header",
                                "button[class*='reply-email']",
                                "div[class*='reply-email']"
                            ]
                            
                            # Check periodically for 30 seconds
                            for _ in range(15):  # 15 iterations × 2 seconds = 30 seconds total wait time
                                for selector in email_button_selectors:
                                    try:
                                        email_button = WebDriverWait(self.driver, 2).until(
                                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                                        )
                                        email_button.click()
                                        email_found = True
                                        break
                                    except:
                                        continue
                                
                                if email_found:
                                    break
                                time.sleep(2)
                            
                            if email_found:
                                try:
                                    # Wait for the email content to appear - try multiple selectors
                                    email_container = None
                                    container_selectors = [
                                        "div.reply-content-email",
                                        "div[class*='reply-email']",
                                        "div.reply-info"
                                    ]
                                    
                                    for selector in container_selectors:
                                        try:
                                            email_container = WebDriverWait(self.driver, 10).until(
                                                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                                            )
                                            if email_container:
                                                break
                                        except:
                                            continue
                                    
                                    if not email_container:
                                        pass
                                    else:
                                        # Extract the default email address - try multiple selectors
                                        email_element = None
                                        email_selectors = [
                                            "div.reply-email-address a",
                                            "a[href^='mailto:']",
                                            "a[class*='email']"
                                        ]
                                        
                                        for selector in email_selectors:
                                            try:
                                                email_element = email_container.find_element(By.CSS_SELECTOR, selector)
                                                if email_element:
                                                    break
                                            except:
                                                continue
                                        
                                        if email_element:
                                            # Try to get email from text first
                                            email = email_element.text.strip()
                                            
                                            # If text is empty or doesn't contain @, try to extract from href
                                            if not email or '@' not in email:
                                                href = email_element.get_attribute("href")
                                                if href and href.startswith("mailto:"):
                                                    email = href.replace("mailto:", "").split("?")[0]
                                            
                                            listing_data['Email'] = email
                                            
                                            # Also save to Default Mail column
                                            href = email_element.get_attribute("href")
                                            if href and href.startswith("mailto:"):
                                                # Store the complete mailto: URL
                                                listing_data['Default Mail'] = href
                                                
                                                # For the main Email field, still extract just the email address
                                                email_part = href.replace("mailto:", "").split("?")[0]
                                                if not listing_data['Email'] or '@' not in listing_data['Email']:
                                                    listing_data['Email'] = email_part
                                        
                                        # Extract other email methods into separate columns
                                        webmail_links = email_container.find_elements(By.CSS_SELECTOR, "a[class*='webmail']")
                                        
                                        for link in webmail_links:
                                            href = link.get_attribute("href")
                                            if href:
                                                # Store the complete href value based on the class
                                                class_attr = link.get_attribute("class")
                                                if class_attr:
                                                    if "gmail" in class_attr:
                                                        listing_data['Gmail'] = href
                                                    elif "yahoo" in class_attr:
                                                        listing_data['Yahoo'] = href
                                                    elif "outlook" in class_attr:
                                                        listing_data['Outlook'] = href
                                                    elif "aol" in class_attr:
                                                        listing_data['AOL'] = href
                                    
                                except Exception:
                                    pass
                                
                    except Exception:
                        pass
                    
                    # Mark as processed and break the retry loop
                    listing_data['Processed'] = True
                    break
                    
                except Exception:
                    if attempt == self.max_retries - 1:
                        listing_data['Description'] = "Error: Failed to load page"
                        listing_data['Remote'] = "Not Specified"
                        listing_data['Email'] = "Not Available"
                        listing_data['Default Mail'] = ""
                        listing_data['Gmail'] = ""
                        listing_data['Yahoo'] = ""
                        listing_data['Outlook'] = ""
                        listing_data['AOL'] = ""
                        listing_data['Processed'] = True
                
                # Delay between retries
                random_delay()
            
            results.append(listing_data)
            
            # Save progress after each batch
            if idx % self.batch_size == 0 or idx == remaining_total:
                progress_df = pd.DataFrame(results)
                save_to_csv(progress_df, self.output_file)
                
                # Apply a longer delay between batches
                if idx < remaining_total:
                    min_batch_delay = float(os.getenv('MIN_DELAY_BETWEEN_BATCHES', 15))
                    max_batch_delay = float(os.getenv('MAX_DELAY_BETWEEN_BATCHES', 30))
                    random_delay(min_batch_delay, max_batch_delay)
        
        # Final save to ensure all data is saved
        final_df = pd.DataFrame(results)
        
        # Replace empty values with 'null' before saving
        final_df = self._replace_empty_with_null(final_df)
        
        save_to_csv(final_df, self.output_file)
        
        return final_df
        
    def close(self):
        """Close the browser."""
        if hasattr(self, 'driver') and self.driver:
            self.driver.quit() 
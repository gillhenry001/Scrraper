import os
import time
import random
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create directories if they don't exist
os.makedirs('output', exist_ok=True)

# List of user agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
]

def get_random_user_agent():
    """Return a random user agent from the list."""
    return random.choice(USER_AGENTS)

def random_delay(min_delay=None, max_delay=None):
    """Apply a random delay within the specified range."""
    if min_delay is None:
        min_delay = float(os.getenv('MIN_DELAY_BETWEEN_ACTIONS', 2))
    if max_delay is None:
        max_delay = float(os.getenv('MAX_DELAY_BETWEEN_ACTIONS', 5))
    
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)
    return delay

def save_to_csv(data, filepath):
    """Save data to a CSV file."""
    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False)
    return df

def load_from_csv(filepath):
    """Load data from a CSV file."""
    if os.path.exists(filepath):
        df = pd.DataFrame(pd.read_csv(filepath))
        return df
    return pd.DataFrame()

def remove_duplicates(df, column_name):
    """Remove duplicate rows based on a specific column."""
    df = df.drop_duplicates(subset=[column_name])
    return df

def save_screenshot(driver, filename_prefix):
    """Save a screenshot for debugging purposes."""
    try:
        screenshots_dir = "screenshots"
        os.makedirs(screenshots_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        filename = f"{screenshots_dir}/{filename_prefix}_{timestamp}.png"
        driver.save_screenshot(filename)
        return filename
    except Exception:
        return None

def save_html(driver, filename_prefix):
    """Save page HTML for debugging purposes."""
    try:
        html_dir = "html_dumps"
        os.makedirs(html_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        filename = f"{html_dir}/{filename_prefix}_{timestamp}.html"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
            
        return filename
    except Exception:
        return None

try:
    from tqdm import tqdm
    
    def create_progress_bar(total, desc="Progress"):
        """Create a progress bar for better user experience."""
        return tqdm(total=total, desc=desc, unit="listings")
except ImportError:
    def create_progress_bar(total, desc="Progress"):
        return range(total) 
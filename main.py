import os
import argparse
import traceback
from scraper import CraigslistScraper
from dotenv import load_dotenv

def main():
    try:
        # Load environment variables
        load_dotenv()
        
        print("Initializing Craigslist Scraper...")
        scraper = CraigslistScraper()
        
        print("Starting scraping process...")
        # Phase 1: Scrape job listings
        print("Phase 1: Scraping job listings...")
        listings_df = scraper.scrape_listings()
        
        if listings_df.empty:
            print("No listings found. Exiting...")
            return
            
        print(f"Found {len(listings_df)} listings")
        
        # Phase 2: Clean listings
        print("Phase 2: Cleaning listings...")
        cleaned_df = scraper.clean_listings(listings_df)
        print(f"Cleaned {len(cleaned_df)} listings")
        
        # Phase 3: Scrape details
        print("Phase 3: Scraping details...")
        results_df = scraper.scrape_details(cleaned_df)
        
        print("Scraping completed successfully!")
        print(f"Total results saved: {len(results_df)}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise
    finally:
        if 'scraper' in locals():
            print("Closing browser...")
            scraper.close()

if __name__ == "__main__":
    main() 
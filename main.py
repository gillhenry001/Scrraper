import os
import argparse
import traceback
from scraper import CraigslistScraper

def main():
    parser = argparse.ArgumentParser(description='Craigslist Computer Gigs Scraper')
    parser.add_argument('--phase', type=int, choices=[1, 2, 3], default=3,
                        help='Phase to run: 1 for listing scraping, 2 for detail scraping, 3 for both')
    parser.add_argument('--start-index', type=int, default=0,
                        help='Start processing from this index (for resuming interrupted runs)')
    parser.add_argument('--max-listings', type=int, default=None,
                        help='Maximum number of listings to process (useful for testing)')
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs('output', exist_ok=True)
    
    try:
        scraper = CraigslistScraper()
        
        if args.phase in [1, 3]:
            print("=== STARTING PHASE 1: SCRAPING LISTINGS ===")
            listings_df = scraper.scrape_listings(max_listings=args.max_listings)
            print(f"Phase 1 completed: Found {len(listings_df)} matching listings")
        
        if args.phase in [2, 3]:
            print("=== STARTING PHASE 2: PROCESSING LISTINGS ===")
            print("Step 1: Cleaning listings (removing duplicates)")
            cleaned_df = scraper.clean_listings()
            
            print("Step 2: Scraping details from each listing")
            results_df = scraper.scrape_details(cleaned_df, start_index=args.start_index, max_listings=args.max_listings)
            print(f"Phase 2 completed: Processed {len(results_df)} listings")
        
        print("Scraping process completed successfully!")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print(traceback.format_exc())
    
    finally:
        if 'scraper' in locals():
            scraper.close()

if __name__ == "__main__":
    main() 
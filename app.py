from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import shutil
import os
from datetime import datetime
import asyncio
from scraper import CraigslistScraper
from config import CRAIGSLIST_CITIES, CRAIGSLIST_BASE_URL, KEYWORDS, REMOTE_KEYWORDS, NON_REMOTE_KEYWORDS
import json
from dotenv import load_dotenv
import logging
import pandas as pd

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Craigslist Scraper API")

# Global variables to store configuration and scraper instance
current_config = {
    "cities": CRAIGSLIST_CITIES,
    "base_url": CRAIGSLIST_BASE_URL,
    "keywords": KEYWORDS,
    "remote_keywords": REMOTE_KEYWORDS,
    "non_remote_keywords": NON_REMOTE_KEYWORDS,
    "use_headless": os.getenv('USE_HEADLESS', 'false').lower() == 'true',
    "batch_size": int(os.getenv('BATCH_SIZE', 10)),
    "max_retries": int(os.getenv('MAX_RETRIES', 3)),
    "min_delay": float(os.getenv('MIN_DELAY', 2)),
    "max_delay": float(os.getenv('MAX_DELAY', 5)),
    "min_delay_between_cities": float(os.getenv('MIN_DELAY_BETWEEN_CITIES', 5)),
    "max_delay_between_cities": float(os.getenv('MAX_DELAY_BETWEEN_CITIES', 10)),
    "min_delay_between_batches": float(os.getenv('MIN_DELAY_BETWEEN_BATCHES', 15)),
    "max_delay_between_batches": float(os.getenv('MAX_DELAY_BETWEEN_BATCHES', 30))
}

scraper = None
scraping_status = {
    "is_running": False,
    "status": "idle",
    "message": "",
    "error": None
}

class ConfigUpdate(BaseModel):
    cities: Optional[list] = None
    base_url: Optional[str] = None
    keywords: Optional[list] = None
    remote_keywords: Optional[list] = None
    non_remote_keywords: Optional[list] = None
    use_headless: Optional[bool] = None
    batch_size: Optional[int] = None
    max_retries: Optional[int] = None
    min_delay: Optional[float] = None
    max_delay: Optional[float] = None
    min_delay_between_cities: Optional[float] = None
    max_delay_between_cities: Optional[float] = None
    min_delay_between_batches: Optional[float] = None
    max_delay_between_batches: Optional[float] = None

def update_config_file(config: Dict[str, Any]):
    """Update the config.py file with new configuration values."""
    config_content = f"""CRAIGSLIST_CITIES = {json.dumps(config['cities'], indent=4)}
CRAIGSLIST_BASE_URL = "{config['base_url']}"
KEYWORDS = {json.dumps(config['keywords'], indent=4)}
REMOTE_KEYWORDS = {json.dumps(config['remote_keywords'], indent=4)}
NON_REMOTE_KEYWORDS = {json.dumps(config['non_remote_keywords'], indent=4)}
"""
    with open('config.py', 'w') as f:
        f.write(config_content)

@app.post("/start-scraping")
async def start_scraping(background_tasks: BackgroundTasks):
    """Start the scraping process in the background."""
    global scraper, scraping_status
    
    if scraping_status["is_running"]:
        raise HTTPException(status_code=400, detail="Scraping is already running")
    
    try:
        scraper = CraigslistScraper()
        scraping_status["is_running"] = True
        scraping_status["status"] = "running"
        scraping_status["message"] = "Scraping started successfully"
        scraping_status["error"] = None
        
        background_tasks.add_task(run_scraper)
        return JSONResponse(
            content={
                "message": "Scraping started successfully",
                "status": "running"
            }
        )
    except Exception as e:
        scraping_status["is_running"] = False
        scraping_status["status"] = "error"
        scraping_status["message"] = "Failed to start scraping"
        scraping_status["error"] = str(e)
        scraper = None
        raise HTTPException(status_code=500, detail=str(e))

async def run_scraper():
    """Run the scraper process."""
    global scraper, scraping_status
    
    try:
        # Phase 1: Scrape listings
        scraping_status["message"] = "Phase 1: Scraping listings"
        df = scraper.scrape_listings()
        
        if not df.empty:
            # Phase 2 - Step 1: Clean listings
            scraping_status["message"] = "Phase 2: Cleaning listings"
            df = scraper.clean_listings(df)
            
            # Phase 2 - Step 2: Scrape details
            scraping_status["message"] = "Phase 2: Scraping details"
            df = scraper.scrape_details(df)
            
            # Update status for successful completion
            scraping_status["is_running"] = False
            scraping_status["status"] = "completed"
            scraping_status["message"] = "Scraping Complete"
            scraping_status["error"] = None
        else:
            # Update status for no results
            scraping_status["is_running"] = False
            scraping_status["status"] = "completed"
            scraping_status["message"] = "No listings found"
            scraping_status["error"] = None
            
    except Exception as e:
        # Update status for error
        scraping_status["is_running"] = False
        scraping_status["status"] = "error"
        scraping_status["message"] = "Error during scraping"
        scraping_status["error"] = str(e)
        print(f"Error during scraping: {str(e)}")
    finally:
        if scraper:
            scraper.close()
            scraper = None

@app.get("/scraping-status")
async def get_scraping_status():
    """Get the current status of the scraping process."""
    return scraping_status

@app.get("/download-results")
async def download_results():
    """Download the results CSV file."""
    output_file = os.getenv('OUTPUT_FILE', 'output/results.csv')
    
    if not os.path.exists(output_file):
        raise HTTPException(status_code=404, detail="Results file not found")
    
    return FileResponse(
        output_file,
        media_type='text/csv',
        filename='craigslist_results.csv'
    )

@app.post("/update-config")
async def update_config(config_update: ConfigUpdate):
    """Update the configuration values."""
    global current_config
    
    # Update only the provided fields
    update_dict = config_update.dict(exclude_unset=True)
    current_config.update(update_dict)
    
    # Update the config file
    update_config_file(current_config)
    
    return {"message": "Configuration updated successfully", "config": current_config}

@app.get("/current-config")
async def get_current_config():
    """Get the current configuration values."""
    return current_config

@app.post("/cleanup")
async def cleanup():
    """Clean up resources and stop any running scraping process."""
    global scraper, scraping_status
    
    if scraper:
        scraper.close()
        scraper = None
    
    scraping_status["is_running"] = False
    scraping_status["status"] = "idle"
    scraping_status["message"] = "Cleanup completed"
    scraping_status["error"] = None
    
    return {"message": "Cleanup completed successfully"}

if __name__ == "__main__":
    import uvicorn
    # Get port from environment variable, default to 8000 if not set
    port = int(os.getenv('PORT', 8000))
    logger.info(f"Starting server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port) 
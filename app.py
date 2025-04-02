from fastapi import FastAPI, HTTPException, BackgroundTasks, APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ValidationError
from typing import Optional, Dict, Any, List
import os
from scraper import CraigslistScraper
from config import CRAIGSLIST_CITIES, CRAIGSLIST_BASE_URL, KEYWORDS, REMOTE_KEYWORDS, NON_REMOTE_KEYWORDS
import json
import pandas as pd
from dotenv import load_dotenv
import base64
import asyncio
from datetime import datetime

# Load environment variables
load_dotenv()

# Create FastAPI app and router
app = FastAPI(title="Craigslist Scraper API", 
             description="API for scraping and managing Craigslist job listings",
             version="1.0.0")
router = APIRouter(prefix="/api")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
    expose_headers=["*"],  # Expose all headers
    max_age=3600,  # Cache preflight requests for 1 hour
)

# Global variables
scraper = None
scraping_status = {
    "is_running": False,
    "progress": 0,
    "total_listings": 0,
    "processed_listings": 0,
    "current_phase": "null",
    "last_completed": None,
    "no_results": False,
    "error": None
}

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

class ConfigUpdate(BaseModel):
    cities: Optional[List[str]] = None
    base_url: Optional[str] = None
    keywords: Optional[List[str]] = None
    remote_keywords: Optional[List[str]] = None
    non_remote_keywords: Optional[List[str]] = None
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

def reset_status():
    """Reset the scraping status to default values."""
    global scraping_status
    scraping_status = {
        "is_running": False,
        "progress": 0,
        "total_listings": 0,
        "processed_listings": 0,
        "current_phase": "Not Started",
        "last_completed": None,
        "no_results": False,
        "error": None
    }

@router.get("/")
async def root():
    """Root endpoint with API information."""
    response = {
        "name": "Craigslist Scraper API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "GET /api": "This information",
            "POST /api/start-scraping": "Start the scraping process",
            "GET /api/scraping-status": "Get current scraping status",
            "GET /api/download-results": "Download scraped results as CSV",
            "POST /api/update-config": "Update scraper configuration",
            "GET /api/current-config": "Get current configuration",
            "POST /api/cleanup": "Clean up resources and stop scraping"
        }
    }
    print("\n=== Root Endpoint Response ===")
    print(json.dumps(response, indent=2))
    return response

@router.post("/start-scraping")
async def start_scraping(background_tasks: BackgroundTasks):
    """Start the scraping process in the background."""
    global scraper, scraping_status
    
    if scraping_status["is_running"]:
        print("\n=== Start Scraping Error ===")
        print("Scraping is already running")
        raise HTTPException(status_code=400, detail="Scraping is already running")
    
    try:
        # Reset status if not in no_results state
        if not scraping_status["no_results"]:
            reset_status()
        
        scraper = CraigslistScraper()
        scraping_status["is_running"] = True
        scraping_status["progress"] = 0
        scraping_status["current_phase"] = None
        scraping_status["last_completed"] = None
        
        background_tasks.add_task(run_scraper)
        response = {
            "message": "Scraping started successfully",
            "status": "running"
        }
        print("\n=== Start Scraping Response ===")
        print(json.dumps(response, indent=2))
        return JSONResponse(content=response)
    except Exception as e:
        print("\n=== Start Scraping Error ===")
        print(f"Error: {str(e)}")
        scraping_status["is_running"] = False
        scraping_status["progress"] = 0
        scraping_status["current_phase"] = None
        scraping_status["last_completed"] = None
        scraper = None
        raise HTTPException(status_code=500, detail=str(e))

async def run_scraper():
    """Run the scraper process."""
    global scraper, scraping_status
    
    try:
        # Phase 1: Scrape listings
        scraping_status.update({
            "is_running": True,
            "progress": 0,
            "current_phase": "Phase 1: Scraping listings",
            "last_completed": "Starting Phase 1",
            "completed": False,
            "error": False,
            "no_results": False
        })
        
        df = scraper.scrape_listings()
        
        if df.empty:
            scraping_status.update({
                "is_running": False,
                "progress": 0,
                "current_phase": "Completed",
                "last_completed": "No listings found",
                "completed": True,
                "error": False,
                "no_results": True
            })
            return
            
        # Phase 2 - Step 1: Clean listings
        scraping_status.update({
            "is_running": True,
            "progress": 30,
            "current_phase": "Phase 2: Cleaning listings",
            "last_completed": f"Found {len(df)} listings",
            "completed": False,
            "error": False,
            "no_results": False
        })
        
        df = scraper.clean_listings(df)
        
        # Phase 2 - Step 2: Scrape details
        scraping_status.update({
            "is_running": True,
            "progress": 50,
            "current_phase": "Phase 2: Scraping details",
            "last_completed": f"Processing {len(df)} listings",
            "completed": False,
            "error": False,
            "no_results": False
        })
        
        results_df = scraper.scrape_details(df)
        
        # Update final status
        scraping_status.update({
            "is_running": False,
            "progress": 100,
            "current_phase": "Completed",
            "last_completed": "Scraping Complete",
            "completed": True,
            "error": False,
            "no_results": False
        })
        
    except Exception as e:
        scraping_status.update({
            "is_running": False,
            "progress": 0,
            "current_phase": "Error",
            "last_completed": "Error during scraping",
            "completed": False,
            "error": True,
            "no_results": False
        })
        scraping_status["error"] = str(e)
        raise
    finally:
        if scraper:
            scraper.close()
            scraper = None

@router.get("/scraping-status")
async def get_scraping_status():
    """Get the current status of the scraping process."""
    # Create a copy of the status to avoid race conditions
    status = scraping_status.copy()
    
    # Update the completed flag based on current state
    status["completed"] = (
        not status["is_running"] and 
        status["current_phase"] == "Completed" and 
        status["last_completed"] == "Scraping Complete"
    )
    
    # Update the error flag based on current state
    status["error"] = (
        not status["is_running"] and 
        status["current_phase"] == "Error"
    )
    
    # Update the no_results flag based on current state
    status["no_results"] = (
        not status["is_running"] and 
        status["last_completed"] == "No listings found"
    )
    
    print("\n=== Scraping Status Response ===")
    print(json.dumps(status, indent=2))
    return status

@router.get("/download-results")
async def download_results():
    """Download scraped results as CSV."""
    try:
        output_file = os.getenv('OUTPUT_FILE', 'output/results.csv')
        
        if not os.path.exists(output_file):
            print("\n=== Download Results Error ===")
            print("No results found. Please run the scraper first.")
            raise HTTPException(
                status_code=404,
                detail="No results found. Please run the scraper first."
            )
        
        # Read the CSV file
        with open(output_file, 'rb') as file:
            file_content = file.read()
            
        # Encode the content to base64
        base64_content = base64.b64encode(file_content).decode('utf-8')
        
        # Create response with metadata
        response = {
            "filename": "scraped_results.csv",
            "content": base64_content,  # Send the complete base64 content
            "content_type": "text/csv",
            "size": len(file_content)
        }
        
        # Log only the size and filename for debugging
        print("\n=== Download Results Response ===")
        print(f"Filename: {response['filename']}")
        print(f"Size: {response['size']} bytes")
        print(f"Base64 length: {len(response['content'])} characters")
        
        return response
        
    except Exception as e:
        print("\n=== Download Results Error ===")
        print(f"Error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error downloading results: {str(e)}"
        )

@router.post("/update-config")
async def update_config(request: Request, config_update: ConfigUpdate):
    """Update the configuration values."""
    global current_config, CRAIGSLIST_CITIES, CRAIGSLIST_BASE_URL, KEYWORDS, REMOTE_KEYWORDS, NON_REMOTE_KEYWORDS
    
    try:
        # Log the raw request body
        body = await request.body()
        print("\n=== Update Config Request ===")
        print("Raw request body:", body.decode())
        
        # Log the parsed config update
        print("\nParsed config update:", json.dumps(config_update.dict(), indent=2))
        
        # Update only the provided fields
        update_dict = config_update.dict(exclude_unset=True)
        
        # Validate the update before applying
        if 'cities' in update_dict and not isinstance(update_dict['cities'], list):
            raise HTTPException(status_code=422, detail="Cities must be a list")
            
        if 'keywords' in update_dict and not isinstance(update_dict['keywords'], list):
            raise HTTPException(status_code=422, detail="Keywords must be a list")
            
        if 'use_headless' in update_dict and not isinstance(update_dict['use_headless'], bool):
            raise HTTPException(status_code=422, detail="use_headless must be a boolean")
            
        if 'batch_size' in update_dict and not isinstance(update_dict['batch_size'], int):
            raise HTTPException(status_code=422, detail="batch_size must be an integer")
            
        if 'max_retries' in update_dict and not isinstance(update_dict['max_retries'], int):
            raise HTTPException(status_code=422, detail="max_retries must be an integer")
        
        # Update the current config
        current_config.update(update_dict)
        
        # Update the config file
        update_config_file(current_config)
        
        # Reload the config module to get updated values
        import importlib
        import config
        importlib.reload(config)
        
        # Update the global variables with new values
        CRAIGSLIST_CITIES = config.CRAIGSLIST_CITIES
        CRAIGSLIST_BASE_URL = config.CRAIGSLIST_BASE_URL
        KEYWORDS = config.KEYWORDS
        REMOTE_KEYWORDS = config.REMOTE_KEYWORDS
        NON_REMOTE_KEYWORDS = config.NON_REMOTE_KEYWORDS
        
        response = {
            "message": "Configuration updated successfully",
            "config": current_config
        }
        
        print("\n=== Update Config Response ===")
        print(json.dumps(response, indent=2))
        return response
        
    except ValidationError as e:
        print("\n=== Update Config Validation Error ===")
        print("Validation error:", json.dumps(e.errors(), indent=2))
        raise HTTPException(status_code=422, detail=e.errors())
    except HTTPException as he:
        raise he
    except Exception as e:
        print("\n=== Update Config Error ===")
        print(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error updating configuration: {str(e)}")

@router.get("/current-config")
async def get_current_config():
    """Get the current configuration values."""
    print("\n=== Current Config Response ===")
    print(json.dumps(current_config, indent=2))
    return current_config

@router.post("/cleanup")
async def cleanup():
    """Clean up resources and stop any running scraping process."""
    global scraping_status, scraper
    
    try:
        # Close the scraper if it's running
        if scraper:
            scraper.close()
            scraper = None
        
        # Clean up output files
        output_dir = "output"
        if os.path.exists(output_dir):
            for filename in os.listdir(output_dir):
                file_path = os.path.join(output_dir, filename)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                except Exception as e:
                    print(f"Error deleting {file_path}: {str(e)}")
        
        # Reset status to default values
        scraping_status = {
            "is_running": False,
            "progress": 0,
            "total_listings": 0,
            "processed_listings": 0,
            "current_phase": "Not Started",
            "last_completed": None,
            "no_results": False,
            "error": None
        }
        
        print("\n=== Cleanup Response ===")
        print("Files cleaned from output directory")
        print("Status reset to default values")
        print(json.dumps(scraping_status, indent=2))
        
        return {
            "message": "Cleanup completed successfully",
            "files_cleaned": True,
            "status_reset": True
        }
    except Exception as e:
        print("\n=== Cleanup Error ===")
        print(f"Error during cleanup: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Include the router in the app
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('PORT', 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=True) 
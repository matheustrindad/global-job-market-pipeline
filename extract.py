import requests
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

# 1. Advanced Logging Setup
os.makedirs('logs', exist_ok=True) # Ensure logs folder exists

# Error Logger
error_handler = logging.FileHandler('logs/errors.log')
error_handler.setLevel(logging.ERROR)

# Pipeline Logger (Success & Info)
info_handler = logging.FileHandler('logs/pipeline.log')
info_handler.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[error_handler, info_handler]
)

load_dotenv()

def fetch_adzuna_data(country_code):
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    
    # 3. Validate Credentials 
    if not app_id or not app_key:
        logging.error("Missing API credentials. Check your .env file.")
        return False

    url = f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/1?app_id={app_id}&app_key={app_key}/results_per_page=10&what=data%20engineer"
    
    print(f"Starting extraction: {country_code.upper()}...")
    
    try:
        os.makedirs(f"data/raw/{country_code}", exist_ok=True)
        
        response = requests.get(url, timeout=10)
        response.raise_for_status() 
        
        data = response.json()

        # 2. Validate if API returned results 
        if not data.get('results'):
            logging.warning(f"Empty results for {country_code.upper()}")
            return False
            
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"data/raw/{country_code}/jobs_{today}.json"
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        # 1. Log Success with stats 
        logging.info(f"SUCCESS: Extracted {len(data['results'])} jobs from {country_code.upper()}")
        return True

    except Exception as e:
        logging.error(f"FAILED: Error extracting {country_code.upper()}: {str(e)}")
        return False

if __name__ == "__main__":
    countries = ['br', 'us', 'gb', 'at'] 
    success_count = 0

    for country in countries:
        if fetch_adzuna_data(country):
            success_count += 1
    
    # 4. Final Summary 
    summary_msg = f"Extraction complete: {success_count}/{len(countries)} countries successful."
    print(f"\n{summary_msg}")
    logging.info(summary_msg)
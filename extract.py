import requests
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

# Logging configuration to record failures
logging.basicConfig(
    filename='error.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()

def fetch_adzuna_data(country_code):
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    
    # Dynamic URL based on country_code
    url = f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/1?app_id={app_id}&app_key={app_key}&results_per_page=10&what=data%20engineer"
    
    print(f"Starting extraction: {country_code.upper()}...")
    
    try:
        # Create country subfolder if it doesn't exist
        os.makedirs(f"data/raw/{country_code}", exist_ok=True)
        
        response = requests.get(url, timeout=10)
        response.raise_for_status() 
        
        data = response.json()
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"data/raw/{country_code}/jobs_{today}.json"
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        print(f"Success! {country_code.upper()} saved to: {filename}")

    except Exception as e:
        error_msg = f"Error extracting {country_code}: {str(e)}"
        print(error_msg)
        logging.error(error_msg)

if __name__ == "__main__":
    # List of countries to monitor: Brazil, USA, United Kingdom, Austria
    countries = ['br', 'us', 'gb', 'at'] 
    
    for country in countries:
        fetch_adzuna_data(country)
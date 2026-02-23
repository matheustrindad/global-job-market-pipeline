# Global Job Market Data Pipeline 
 
An automated data engineering pipeline designed to extract and monitor Data Engineering job opportunities from multiple countries using the Adzuna API. 
 
## ?? Features 
- **Multi-country support**: Brazil (BR), USA (US), UK (GB), and Austria (AT). 
- **Security**: API keys protected via .env file. 
- **Resilience**: Error logging implemented with try/except blocks. 
 
## ??? Tech Stack 
- Python 3.x, Requests, Python-dotenv, and Git. 
 
## ?? How to Run 
1. Setup .env with ADZUNA_APP_ID and ADZUNA_APP_KEY. 
2. Install dependencies: pip install -r requirements.txt. 
3. Run: python extract.py. 

# Global Job Market Data Pipeline

An automated data engineering pipeline designed to extract and monitor Data Engineering job opportunities from multiple countries using the Adzuna API.

## ?? Features

* **Multi-country support**: Brazil (BR), USA (US), UK (GB), and Austria (AT).
* **Security**: API keys protected via .env file.
* **Resilience**: Error logging implemented with try/except blocks.

## ??? Tech Stack

* Python 3.x, Requests, Python-dotenv, and Git.

## ?? How to Run

1. Setup .env with ADZUNA\_APP\_ID and ADZUNA\_APP\_KEY.
2. Install dependencies: pip install -r requirements.txt.
3. Run: python extract.py.
4. \## 🗄️ Step 4: Database Persistence \& Analytics
5. 
6. The pipeline now includes a robust storage layer using \*\*SQLite\*\*. Data is automatically loaded into a relational structure, allowing for complex queries and persistent historical analysis.
7. 
8. \### 📊 Data Warehouse Structure
9. \- \*\*Table `jobs`\*\*: Stores all cleaned and validated job postings.
10. \- \*\*View `v\_salary\_analysis`\*\*: Aggregates average salaries by country and seniority level.
11. \- \*\*View `v\_jobs\_trend`\*\*: Tracks the volume of job postings over time.
12. 
13. \### 🔍 Engineering Insights (Day 3)
14. During the analysis of the first 67 processed jobs, some key technical observations were documented:
15. 1\. \*\*Currency Heterogeneity\*\*: Salaries are stored in the local currency of each country (e.g., BRL for Brazil, USD for USA, GBP for UK). Future versions will implement a real-time currency conversion layer.
16. 2\. \*\*Salary Reporting Patterns\*\*: 
17. &nbsp;  - \*\*US/UK\*\*: Salaries are typically reported as annual values (e.g., ~$150k/year).
18. &nbsp;  - \*\*Brazil/Austria\*\*: High frequency of "0.0" values, reflecting the local market trend of "Salary to be discussed" or unlisted compensation.
19. 3\. \*\*Market Snapshot\*\*: The initial run identified a high concentration of Senior/Mid-level roles compared to Junior positions in the Brazilian sample.








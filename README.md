# 🌍 Global Job Market Data Pipeline

> An automated, end-to-end data engineering pipeline that extracts, validates, transforms, and analyzes Data Engineering job postings from 4 countries daily — fully orchestrated and self-running.

![Pipeline Status](https://img.shields.io/badge/Pipeline-Passing-brightgreen)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![SQLite](https://img.shields.io/badge/Database-SQLite-lightgrey)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 📌 Overview

This project implements a production-grade **Medallion Architecture** (Bronze → Silver → Gold) to collect and analyze the global demand for Data Engineers across Brazil, the United States, the United Kingdom, and Austria.

The pipeline runs automatically every day, collecting fresh job market data without any manual intervention.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    DATA SOURCES                         │
│              Adzuna Jobs API (4 countries)              │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│                 BRONZE LAYER                            │
│         Raw JSON files — never modified                 │
│         data/bronze/{country}/jobs_YYYY-MM-DD.json      │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│                 SILVER LAYER                            │
│   Cleaned, validated, deduplicated CSV                  │
│   • Data quality checks (quarantine for invalid rows)   │
│   • Seniority classification (Junior / Mid / Senior)    │
│   data/silver/jobs_clean_YYYY-MM-DD.csv                 │
│   data/quarantine/invalid_YYYY-MM-DD.csv                │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│                  GOLD LAYER                             │
│   Aggregated analytical tables                          │
│   • Average salary by country and seniority             │
│   • Job volume trends over time                         │
│   data/gold/salary_by_country.csv                       │
│   data/gold/jobs_by_seniority.csv                       │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│               DATABASE LAYER                            │
│   SQLite with analytical views                          │
│   • v_salary_analysis — avg salary by country/seniority │
│   • v_jobs_trend — job volume over time                 │
└─────────────────────────────────────────────────────────┘
```

---

## ⚙️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| Data Processing | Pandas |
| API Client | Requests |
| Database | SQLite (compatible with PostgreSQL / Snowflake) |
| Orchestration | GitHub Actions (daily schedule) |
| Containerization | Docker + Docker Compose |
| Dashboard | Streamlit |
| Version Control | Git with Conventional Commits |

---

## 🔍 Engineering Insights

During analysis of the first collected dataset (68 processed jobs across 4 countries), the following patterns were documented:

**1. Currency Heterogeneity**
Salaries are stored in each country's local currency (BRL, USD, GBP, EUR). A real-time currency conversion layer is planned for a future version.

**2. Salary Reporting Patterns**
- **US / UK**: Salaries are typically reported as annual values (~$150k/year for senior roles)
- **Brazil / Austria**: High frequency of unlisted salaries (0.0), reflecting the local market trend of "salary to be discussed"

**3. Seniority Distribution**
Initial data shows a high concentration of Senior and Mid-Level roles compared to Junior positions, particularly in the Brazilian sample — suggesting the local market publicly lists fewer entry-level data engineering roles.

---

## 🚀 How to Run

### Option 1 — Docker (Recommended)

```bash
git clone https://github.com/YOUR_USERNAME/global-job-market-pipeline.git
cd global-job-market-pipeline
cp .env.example .env   # Add your Adzuna API credentials
docker-compose up
```

### Option 2 — Local Python

```bash
git clone https://github.com/YOUR_USERNAME/global-job-market-pipeline.git
cd global-job-market-pipeline
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env      # Add your Adzuna API credentials
python main.py
```

---

## 🔐 Environment Variables

Create a `.env` file in the root directory:

```
ADZUNA_APP_ID=your_app_id_here
ADZUNA_APP_KEY=your_app_key_here
```

Get your free API credentials at [developer.adzuna.com](https://developer.adzuna.com)

---

## 📁 Project Structure

```
global-job-market-pipeline/
├── extract.py          # Bronze layer — API extraction with error logging
├── transform.py        # Silver layer — cleaning, validation, quarantine
├── gold.py             # Gold layer — analytical aggregations
├── load.py             # Database layer — SQLite persistence
├── main.py             # Pipeline orchestrator
├── schema.sql          # Database schema and views
├── requirements.txt
├── .env.example
├── Dockerfile
├── docker-compose.yml
├── data/
│   ├── bronze/         # Raw JSON files (immutable)
│   ├── silver/         # Cleaned CSV files
│   ├── gold/           # Aggregated analytical tables
│   └── quarantine/     # Invalid records for review
└── logs/
    └── pipeline.log    # Full execution history
```

---

## 📊 Data Quality

The pipeline implements a validation layer that checks every record before promotion to the Silver layer:

- Job title must not be empty
- Salary minimum must not be negative
- Salary max must be greater than or equal to salary min
- Invalid records are **not deleted** — they are moved to the quarantine folder for review

---

## 🔄 Automation

The pipeline runs automatically every day at 09:00 UTC via GitHub Actions. The workflow:

1. Checks out the repository
2. Sets up Python and installs dependencies
3. Runs the full pipeline: extract → transform → gold → load
4. Commits updated data files back to the repository

---

## 📈 Dashboard

Live dashboard available at: **[Add Streamlit URL here after deploy]**

---

## 🗺️ Roadmap

- [ ] Currency normalization layer (convert all salaries to USD)
- [ ] Streamlit dashboard (in progress)
- [ ] Migrate database to PostgreSQL / Snowflake
- [ ] Add more countries (Canada, Germany, Australia)
- [ ] NLP-based skills extraction from job descriptions

---

## 👤 Author

**Matheus Trindad**
Data Engineer | Python • SQL • ETL • PySpark

[LinkedIn](https://www.linkedin.com/in/matheus-coimbra-a83010218/) · [GitHub](https://github.com/matheustrindad)
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY, 
    title TEXT,
    company_name TEXT,
    location_name TEXT,
    salary_min REAL,
    salary_max REAL,
    date_posted TEXT,
    country TEXT,
    seniority TEXT,
    extracted_at TEXT,
    redirect_url TEXT
);

CREATE VIEW IF NOT EXISTS v_salary_analysis AS
SELECT 
    country,
    seniority,
    COUNT(*) as job_count,
    ROUND(AVG(salary_min), 2) as avg_min_salary,
    ROUND(AVG(salary_max), 2) as avg_max_salary
FROM jobs
GROUP BY country, seniority;

CREATE VIEW IF NOT EXISTS v_jobs_trend AS
SELECT 
    date(date_posted) as post_date,
    country,
    COUNT(*) as total_jobs
FROM jobs
GROUP BY post_date, country;
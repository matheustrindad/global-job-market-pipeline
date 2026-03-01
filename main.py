import time
import logging
import os
from datetime import datetime
from extract import fetch_adzuna_data  # Importa sua função de extração
from transform import transform_data    # Importa sua função de transformação
from gold import generate_gold          # Importa sua função de agregação
from load import load_data

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/jobs_db')

# Configuração de Log Centralizado
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/pipeline.log'), logging.StreamHandler()]
)

def run_pipeline():
    start_time = time.time()
    logging.info("=== INICIANDO PIPELINE GLOBAL JOB MARKET ===")

    try:
        # STEP 1: EXTRACTION (BRONZE)
        countries = ['br', 'us', 'gb', 'at']
        logging.info(f"Step 1/4: Extracting data for {countries}...")
        for country in countries:
            fetch_adzuna_data(country)

        # STEP 2: TRANSFORMATION AND VALIDATION (SILVER)
        logging.info("Step 2/4: Starting Transformation and Validation (Silver)...")
        metrics = transform_data()
        
        # STEP 3: ANALYTIC AGGREGATION (GOLD)
        logging.info("Step 3/4: Generating aggregation tables (Gold)...")
        generate_gold()

        # STEP 4: DATABASE LOADING (POSTGRESQL)
        logging.info("Step 4/4: Loading data into PostgreSQL Database...") # Atualizado de SQLite para Postgres
        load_data()


        # CÁLCULO DE TEMPO E CONCLUSÃO    
        end_time = time.time()
        duration = end_time - start_time
        
        # 1. Mensagem de conclusão em Inglês
        finish_msg = f"=== PIPELINE COMPLETED SUCCESSFULLY IN {duration:.2f}s ==="
        
        # 2. Logando a mensagem
        logging.info(finish_msg)
        
        # 3. Printando no terminal para visibilidade imediata
        print(f"\n{finish_msg}")

        if metrics:
            # 4. Resumo final também em Inglês
            print(f"Summary: {metrics['processed']} clean jobs and {metrics['quarantined']} in quarantine.")


    except Exception as e:
        logging.error(f"PIPELINE FAILED: {e}")
        print(f"\nPIPELINE FAILED: {e}")

if __name__ == "__main__":
    run_pipeline()
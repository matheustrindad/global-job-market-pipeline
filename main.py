import time
import logging
import os
from datetime import datetime
from extract import fetch_adzuna_data  # Importa sua função de extração
from transform import transform_data    # Importa sua função de transformação
from gold import generate_gold          # Importa sua função de agregação

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
        # PASSO 1: EXTRAÇÃO (BRONZE)
        countries = ['br', 'us', 'gb', 'at']
        logging.info(f"Passo 1/3: Extraindo dados para {countries}...")
        for country in countries:
            fetch_adzuna_data(country)

        # PASSO 2: TRANSFORMAÇÃO E VALIDAÇÃO (SILVER)
        logging.info("Passo 2/3: Iniciando Transformação e Validação (Silver)...")
        metrics = transform_data()
        
        # PASSO 3: AGREGAÇÃO ANALÍTICA (GOLD)
        logging.info("Passo 3/3: Gerando tabelas de agregação (Gold)...")
        generate_gold()

        end_time = time.time()
        duration = end_time - start_time
        
        logging.info(f"=== PIPELINE FINALIZADO COM SUCESSO EM {duration:.2f}s ===")
        if metrics:
            print(f"\nResumo: {metrics['processed']} vagas limpas e {metrics['quarantined']} na quarentena.")

    except Exception as e:
        logging.error(f"FALHA NO PIPELINE: {e}")

if __name__ == "__main__":
    run_pipeline()
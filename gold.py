import pandas as pd
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def generate_gold():
    logging.info("Iniciando Camada Gold...")
    silver_path = "data/silver"
    os.makedirs("data/gold", exist_ok=True)
    
    # Encontrar o arquivo mais recente na Silver
    files = [f for f in os.listdir(silver_path) if f.endswith('.csv')]
    if not files:
        logging.warning("Nenhum dado encontrado na Silver.")
        return
    
    df = pd.read_csv(os.path.join(silver_path, files[-1]))

    # 1. Agregação: Média Salarial por País
    salary_summary = df.groupby('country')['salary_min'].mean().reset_index()
    salary_summary.to_csv("data/gold/salary_by_country.csv", index=False)

    # 2. Agregação: Contagem de vagas por Senioridade (usando a nova coluna)
    if 'seniority' in df.columns:
        seniority_summary = df['seniority'].value_counts().reset_index()
        seniority_summary.to_csv("data/gold/jobs_by_seniority.csv", index=False)

    logging.info("Tabelas Gold geradas com sucesso!")

if __name__ == "__main__":
    generate_gold()
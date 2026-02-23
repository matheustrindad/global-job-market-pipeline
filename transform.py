import pandas as pd
import json
import os
import logging
from datetime import datetime

# Configuração do Logging (Rastreabilidade)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/pipeline.log'), logging.StreamHandler()]
)

def validate_job(row):
    """
    Diferencial: Validação avançada para a Tabela de Quarentena.
    """
    # 1. Título não pode ser vazio
    if not row['title'] or str(row['title']).strip() == "":
        return False
    
    # 2. Salário mínimo não pode ser negativo
    if row['salary_min'] < 0:
        return False
        
    # 3. Melhoria: Validar consistência entre min e max [DICA DA IA]
    if row['salary_max'] > 0 and row['salary_max'] < row['salary_min']:
        return False
        
    return True

def transform_data():
    today = datetime.now().strftime("%Y-%m-%d")
    raw_base_path = "data/raw"
    
    # Melhoria 1: Garantir que as pastas existem antes de salvar
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/quarantine", exist_ok=True)

    all_jobs = []
    logging.info("Starting data transformation...")

    # Leitura dos arquivos (Multi-country)
    for country in os.listdir(raw_base_path):
        country_path = os.path.join(raw_base_path, country)
        if os.path.isdir(country_path):
            for file in os.listdir(country_path):
                if file.endswith(".json"):
                    with open(os.path.join(country_path, file), 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        results = data.get('results', [])
                        for job in results:
                            job['country_code'] = country.upper()
                        all_jobs.extend(results)

    if not all_jobs:
        logging.warning("No data found to transform.")
        return

    df = pd.DataFrame(all_jobs)

    # Tratamento de dicionários e renomeação (conforme já fizemos)
    for col in ['company', 'location', 'category']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x.get('display_name') if isinstance(x, dict) else x)

    rename_map = {
        'title': 'title',
        'company': 'company_name',
        'location': 'location_name',
        'salary_min': 'salary_min',
        'salary_max': 'salary_max',
        'created': 'date_posted',
        'country_code': 'country'
    }
    
    df = df[[c for c in rename_map.keys() if c in df.columns]]
    df.rename(columns=rename_map, inplace=True)

    # Limpeza e aplicação da Validação
    df.drop_duplicates(inplace=True)
    df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce').fillna(0)
    df['salary_max'] = pd.to_numeric(df['salary_max'], errors='coerce').fillna(0) # Adicionado

    df['is_valid'] = df.apply(validate_job, axis=1)

    processed_df = df[df['is_valid'] == True].drop(columns=['is_valid'])
    quarantine_df = df[df['is_valid'] == False].drop(columns=['is_valid'])

    # Salvando os arquivos
    processed_df.to_csv(f"data/processed/jobs_clean_{today}.csv", index=False)
    quarantine_df.to_csv(f"data/quarantine/invalid_{today}.csv", index=False)

    # Melhoria 3: Logar o resumo para rastreabilidade
    logging.info(f"TRANSFORMATION SUMMARY: {len(processed_df)} jobs processed, {len(quarantine_df)} jobs sent to quarantine.")

if __name__ == "__main__":
    transform_data()
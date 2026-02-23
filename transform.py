import pandas as pd
import json
import os
import logging
from datetime import datetime

# Função de ajuda para Senioridade [NOVO]
def classify_seniority(title):
    t = str(title).lower()
    if any(x in t for x in ['sr', 'senior', 'sênior', 'lead', 'pleno']): return 'Senior/Pleno'
    if any(x in t for x in ['jr', 'junior', 'júnior', 'estagiário', 'intern']): return 'Junior'
    return 'Mid/Not Specified'

def validate_job(row):
    if not row['title'] or str(row['title']).strip() == "": return False
    if row['salary_min'] < 0: return False
    if row['salary_max'] > 0 and row['salary_max'] < row['salary_min']: return False
    return True

def transform_data():
    today = datetime.now().strftime("%Y-%m-%d")
    raw_base_path = "data/bronze" # Atualizado para Bronze
    
    os.makedirs("data/silver", exist_ok=True)
    os.makedirs("data/quarantine", exist_ok=True)

    all_jobs = []
    logging.info("Starting data transformation...")

    # Leitura (Multi-country)
    if not os.path.exists(raw_base_path):
        logging.error("Pasta Bronze não encontrada!")
        return {"processed": 0, "quarantined": 0}

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
        return {"processed": 0, "quarantined": 0}

    df = pd.DataFrame(all_jobs)

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
    
    df = df[[c for c in rename_map.keys() if c in df.columns]].copy()
    df.rename(columns=rename_map, inplace=True)

    # Limpeza e Novas Colunas [Diferencial]
    df.drop_duplicates(inplace=True)
    df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce').fillna(0)
    df['salary_max'] = pd.to_numeric(df['salary_max'], errors='coerce').fillna(0)
    df['seniority'] = df['title'].apply(classify_seniority) # IA de classificação simples
    df['extracted_at'] = datetime.now().isoformat() # Rastreabilidade temporal

    df['is_valid'] = df.apply(validate_job, axis=1)

    processed_df = df[df['is_valid'] == True].drop(columns=['is_valid'])
    quarantine_df = df[df['is_valid'] == False].drop(columns=['is_valid'])

    # Salvando em Silver
    processed_df.to_csv(f"data/silver/jobs_clean_{today}.csv", index=False)
    quarantine_df.to_csv(f"data/quarantine/invalid_{today}.csv", index=False)

    logging.info(f"TRANSFORMATION SUMMARY: {len(processed_df)} jobs processed.")
    
    # RETORNO PARA O MAIN.PY [VITAL]
    return {
        "processed": len(processed_df),
        "quarantined": len(quarantine_df)
    }

if __name__ == "__main__":
    # Quando rodar sozinho, apenas chama a função
    transform_data()
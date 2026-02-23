import pandas as pd
import json
import os
from datetime import datetime

def validate_job(row):
    """
    Diferencial: Função de validação para a Tabela de Quarentena.
    Retorna True se o dado for válido, False caso contrário.
    """
    # Checa: título não vazio e salário (se existir) não negativo
    if not row['title'] or str(row['title']).strip() == "":
        return False
    if row['salary_min'] < 0:
        return False
    return True

def transform_data():
    today = datetime.now().strftime("%Y-%m-%d")
    raw_base_path = "data/raw"
    all_jobs = []

    print("Starting data transformation...")

    # 1. Ler todos os JSONs coletados (Multi-country support)
    for country in os.listdir(raw_base_path):
        country_path = os.path.join(raw_base_path, country)
        if os.path.isdir(country_path):
            for file in os.listdir(country_path):
                if file.endswith(".json"):
                    with open(os.path.join(country_path, file), 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        results = data.get('results', [])
                        # Adiciona a informação do país em cada linha
                        for job in results:
                            job['country_code'] = country.upper()
                        all_jobs.extend(results)

    if not all_jobs:
        print("No data found to transform.")
        return

    df = pd.DataFrame(all_jobs)

    # CORREÇÃO: Antes de tudo, vamos converter colunas que podem ser dicionários em texto
    # A Adzuna envia 'company', 'location' e 'category' como objetos complexos
    for col in ['company', 'location', 'category']:
        if col in df.columns:
            # Extrai apenas o nome/display_name se for um dicionário, senão mantém como está
            df[col] = df[col].apply(lambda x: x.get('display_name') if isinstance(x, dict) else x)

    # 2. Renomear colunas para nomes claros em inglês
    rename_map = {
        'title': 'title',
        'company': 'company_name',
        'location': 'location_name',
        'salary_min': 'salary_min',
        'salary_max': 'salary_max',
        'created': 'date_posted',
        'country_code': 'country'
    }
    
    available_cols = [c for c in rename_map.keys() if c in df.columns]
    df = df[available_cols]
    df.rename(columns=rename_map, inplace=True)

    # 3. Agora sim: remover duplicatas (não há mais dicionários nas colunas selecionadas)
    df.drop_duplicates(inplace=True)
    df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce').fillna(0)

    # 4. Aplicar Validação (Quarentena)
    df['is_valid'] = df.apply(validate_job, axis=1)

    processed_df = df[df['is_valid'] == True].drop(columns=['is_valid'])
    quarantine_df = df[df['is_valid'] == False].drop(columns=['is_valid'])

    # 5. Salvar arquivos com data
    processed_path = f"data/processed/jobs_clean_{today}.csv"
    quarantine_path = f"data/quarantine/invalid_{today}.csv"

    processed_df.to_csv(processed_path, index=False, encoding='utf-8')
    quarantine_df.to_csv(quarantine_path, index=False, encoding='utf-8')

    print(f"Transformation complete!")
    print(f"Clean records: {len(processed_df)} saved to {processed_path}")
    print(f"Quarantine records: {len(quarantine_df)} saved to {quarantine_path}")

if __name__ == "__main__":
    transform_data()
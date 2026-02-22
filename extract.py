import requests
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

# Configuração do Logging para registrar falhas
logging.basicConfig(
    filename='error.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()

def extract_adzuna(country_code):
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    
    # URL dinâmica baseada no país (country_code)
    url = f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/1?app_id={app_id}&app_key={app_key}&results_per_page=10&what=data%20engineer"
    
    print(f"Iniciando extração: {country_code.upper()}...")
    
    try:
        # Cria a subpasta do país se não existir
        os.makedirs(f"data/raw/{country_code}", exist_ok=True)
        
        response = requests.get(url, timeout=10)
        response.raise_for_status() 
        
        data = response.json()
        hoje = datetime.now().strftime("%Y-%m-%d")
        filename = f"data/raw/{country_code}/jobs_{hoje}.json"
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        print(f"Sucesso! {country_code.upper()} salvo em: {filename}")

    except Exception as e:
        erro_msg = f"Erro ao extrair {country_code}: {str(e)}"
        print(erro_msg)
        logging.error(erro_msg)

if __name__ == "__main__":
    # Lista de países que você quer monitorar
    paises = ['br', 'us', 'gb', 'at'] # Brasil, EUA, Reino Unido, Áustria
    
    for p in paises:
        extract_adzuna(p)
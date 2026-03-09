import pandas as pd
import os
import logging
from sqlalchemy import create_engine, text

# Conexão bilingue: Nuvem (GitHub) ou Local (Seu PC)
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/jobs_db')
engine = create_engine(DATABASE_URL)

def load_data():
    silver_path = "data/silver"
    
    # Busca o arquivo mais recente
    if not os.path.exists(silver_path):
        logging.error("Silver path does not exist!")
        return False
        
    files = [f for f in os.listdir(silver_path) if f.endswith('.csv')]
    if not files:
        logging.error("No silver CSV found to load!")
        return False
    
    latest_file = os.path.join(silver_path, sorted(files)[-1])
    df = pd.read_csv(latest_file)

    try:
        # Abre conexão com o PostgreSQL
        with engine.begin() as conn:
            # Limpa duplicatas do dia para permitir re-execução sem erro
            today = pd.Timestamp.now().strftime('%Y-%m-%d')
            conn.execute(text("DELETE FROM jobs WHERE date(extracted_at) = :today"), {"today": today})
            
            # Carrega os dados
            df.to_sql('jobs', conn, if_exists='append', index=False)

        logging.info(f"SUCCESS: Loaded {len(df)} jobs into PostgreSQL!")
        return True
    except Exception as e:
        logging.error(f"LOAD FAILED: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_data()
import sqlite3
import pandas as pd
import os
import logging
from datetime import datetime
from sqlalchemy import create_engine

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/jobs_db')
engine = create_engine(DATABASE_URL)

def load_data():
    db_path = "data/job_market.db"
    sql_path = "schema.sql"
    silver_path = "data/silver"
    
    files = [f for f in os.listdir(silver_path) if f.endswith('.csv')]
    if not files:
        logging.error("No silver CSV found to load!")
        return False
    
    latest_file = os.path.join(silver_path, sorted(files)[-1])
    df = pd.read_csv(latest_file)

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Garante que as tabelas e views existam
        with open(sql_path, 'r', encoding='utf-8') as f:
            cursor.executescript(f.read())

        # --- CORREÇÃO DE DUPLICATAS ---
        # Definimos o dia de hoje baseado na extração
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Removemos registros que foram extraídos hoje para evitar duplicar se rodarmos o script de novo
        cursor.execute("DELETE FROM jobs WHERE date(extracted_at) = ?", (today,))
        conn.commit()
        # ------------------------------

        # Carrega os novos dados
        df.to_sql('jobs', conn, if_exists='append', index=False)
        
        logging.info(f"SUCCESS: Loaded {len(df)} jobs into {db_path} (cleaned daily duplicates)")
        conn.close()
        return True
    except Exception as e:
        logging.error(f"LOAD FAILED: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_data()
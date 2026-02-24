import sqlite3
import pandas as pd
import os
import logging

def load_data():
    db_path = "data/job_market.db"
    sql_path = "schema.sql"
    silver_path = "data/silver"
    
    # Busca o CSV mais recente
    files = [f for f in os.listdir(silver_path) if f.endswith('.csv')]
    if not files:
        logging.error("No silver CSV found to load!")
        return False
    
    latest_file = os.path.join(silver_path, sorted(files)[-1])
    df = pd.read_csv(latest_file)

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Executa o schema
        with open(sql_path, 'r', encoding='utf-8') as f:
            cursor.executescript(f.read())

        # Carrega os dados
        df.to_sql('jobs', conn, if_exists='append', index=False)
        
        logging.info(f"SUCCESS: Loaded {len(df)} jobs into {db_path}")
        conn.close()
        return True
    except Exception as e:
        logging.error(f"LOAD FAILED: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_data()
import os
import time
import pandas as pd
import logging
from sqlalchemy import create_engine

# Setup logging
logging.basicConfig(
    filename="logs/ingestion.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# Create engine (change to your DB)
engine = create_engine("sqlite:///inventory.db")

def ingest_db(file, table_name, engine, chunksize=10000):
    for chunk in pd.read_csv(file, chunksize=chunksize):
        chunk.to_sql(
            table_name,
            con=engine,
            if_exists='append',
            index=False
        )
        logging.info(f"Inserted {len(chunk)} rows into {table_name}")
        
def load_raw_data():
    """Loop through CSVs in data folder and load into DB"""
    start = time.time()

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            logging.info(f'Ingesting {file} into db')
            ingest_db(os.path.join('data', file), file[:-4], engine)

    end = time.time()
    total_time = (end - start) / 60
    logging.info('Ingestion Complete')
    logging.info(f'Total time taken: {total_time:.2f} minutes')

if __name__ == '__main__':
    load_raw_data()
    logging.shutdown()
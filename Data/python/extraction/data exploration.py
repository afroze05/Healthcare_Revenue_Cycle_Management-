# ==============================================================================
# PHASE 1.5: DATA EXPLORATION
# ==============================================================================

import pandas as pd
from sqlalchemy import create_engine, text
import logging
import os
import glob

# --- 1. Configuration ---
# Configure logging to see the output clearly
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [DataExplorer] - %(message)s')

# Database connection details
DB_CONFIG = {
    'hospital_a_db': {'user': 'root', 'password': 'root', 'host': '127.0.0.1', 'port': '3306', 'db': 'hospital_a_db'},
    'hospital_b_db': {'user': 'root', 'password': 'root', 'host': '127.0.0.1', 'port': '3306', 'db': 'hospital_b_db'}
}
# Path to the claims folder
CLAIMS_FOLDER = './Data/claims'

# --- 2. Main Exploration Logic ---

def explore_database(db_name, config):
    """Connects to a single database and profiles its tables."""
    print("\n" + "="*80)
    print(f"🔬 EXPLORING DATABASE: {db_name}")
    print("="*80)
    
    try:
        # Create a connection engine
        connection_str = f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['db']}"
        engine = create_engine(connection_str)
        
        with engine.connect() as connection:
            logging.info(f"Successfully connected to {db_name}.")
            
            # Get the list of all tables in this database
            tables_query = "SHOW TABLES;"
            tables = pd.read_sql(text(tables_query), connection).iloc[:, 0].tolist()
            print(f"\nTables found: {tables}\n")
            
            # For each table, get its schema (column names and types) and a data sample
            for table_name in tables:
                print(f"--- Profiling Table: {table_name} ---")
                
                # Get schema
                schema_query = f"DESCRIBE {table_name};"
                schema_df = pd.read_sql(text(schema_query), connection)
                print("Schema (Columns and Data Types):")
                print(schema_df)
                
                # Get a small sample of the data
                sample_query = f"SELECT * FROM {table_name} LIMIT 3;"
                sample_df = pd.read_sql(text(sample_query), connection)
                print("\nSample Data (First 3 Rows):")
                print(sample_df)
                print("-" * 50 + "\n")
                
    except Exception as e:
        logging.error(f"Could not connect to or explore database '{db_name}'. Error: {e}")

def explore_csv_files(folder_path):
    """Finds and profiles all CSV files in a given folder."""
    print("\n" + "="*80)
    print(f"📄 EXPLORING CSV FILES IN: {folder_path}")
    print("="*80)

    csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
    
    if not csv_files:
        logging.warning(f"No CSV files found in '{folder_path}'.")
        return
        
    for file_path in csv_files:
        print(f"--- Profiling File: {os.path.basename(file_path)} ---")
        try:
            df = pd.read_csv(file_path)
            
            print(f"Shape: {df.shape} (Rows, Columns)")
            
            print("\nColumns:")
            print(df.columns.tolist())
            
            print("\nData Types:")
            print(df.info())
            
            print("\nSample Data (First 3 Rows):")
            print(df.head(3))
            print("-" * 50 + "\n")

        except Exception as e:
            logging.error(f"Could not read or profile CSV file '{file_path}'. Error: {e}")

# --- 3. Run the Exploration ---
if __name__ == "__main__":
    # Explore each database defined in the configuration
    for db_name, config in DB_CONFIG.items():
        explore_database(db_name, config)
        
    # Explore the claims folder
    explore_csv_files(CLAIMS_FOLDER)
import os
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from sqlalchemy import text # Import the 'text' function

# --- Configuration ---

# MySQL Connection Details for Hospital A
MYSQL_USER_A = 'root'  # <-- Replace with your MySQL username
MYSQL_PASSWORD_A = 'root'  # <-- Replace with your MySQL password
MYSQL_HOST_A = '127.0.0.1'
MYSQL_PORT_A = '3306'
MYSQL_DB_A = 'hospital_a_db'

# MySQL Connection Details for Hospital B
# CORRECTED VARIABLE NAMES HERE
MYSQL_USER_B = 'root'  # <-- Replace with your MySQL username
MYSQL_PASSWORD_B = 'root'  # <-- Replace with your MySQL password
MYSQL_HOST_B = '127.0.0.1'
MYSQL_PORT_B = '3306'
MYSQL_DB_B = 'hospital_b_db'

# Google BigQuery Details
# GOOGLE_APPLICATION_CREDENTIALS environment variable must be set
# IMPORTANT: Your dataset ID and project ID might be different. 
# The dataset ID should be 'healthcare_rcm' as per the PDF.
BIGQUERY_PROJECT_ID = 'healthcare-rcm-project' # <-- Replace with your Google Cloud Project ID
BIGQUERY_DATASET_ID = 'healthcare_rcm'         # <-- Corrected Dataset ID as per PDF

def test_mysql_connection(db_config, db_name):
    """Tests connection to a MySQL database and fetches table names."""
    print(f"--- Testing MySQL Connection to {db_name} ---")
    try:
        connection_str = (
            f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['db']}"
        )
        engine = create_engine(connection_str)
        with engine.connect() as connection:
            # Use the text() function to wrap the SQL string
            result = connection.execute(text("SHOW TABLES;"))
            tables = [row[0] for row in result]
            print(f"✅ Connection to {db_name} successful.")
            print(f"   Found tables: {tables}")
            return True
    except Exception as e:
        print(f"❌ Connection to {db_name} failed: {e}")
        return False

def test_bigquery_connection(project_id, dataset_id):
    """Tests connection to Google BigQuery by listing datasets."""
    print("\n--- Testing Google BigQuery Connection ---")
    try:
        client = bigquery.Client(project=project_id)
        # Make an API request to verify connection and permissions.
        client.get_dataset(dataset_id)
        print(f"✅ Connection to BigQuery project '{project_id}' successful.")
        print(f"   Successfully accessed dataset: '{dataset_id}'")
        return True
    except Exception as e:
        print(f"❌ Connection to BigQuery failed.")
        print(f"   Please ensure the following:")
        print(f"   1. The 'GOOGLE_APPLICATION_CREDENTIALS' environment variable is set correctly.")
        print(f"   2. The BigQuery API is enabled in your project.")
        print(f"   3. The service account has permissions for the dataset '{dataset_id}'.")
        print(f"   Error: {e}")
        return False

if __name__ == "__main__":
    print("Starting Environment Setup Verification...\n")
    
    # Define connection configs
    config_a = {
        'user': MYSQL_USER_A, 'password': MYSQL_PASSWORD_A,
        'host': MYSQL_HOST_A, 'port': MYSQL_PORT_A, 'db': MYSQL_DB_A
    }
    config_b = {
        'user': MYSQL_USER_B, 'password': MYSQL_PASSWORD_B,
        'host': MYSQL_HOST_B, 'port': MYSQL_PORT_B, 'db': MYSQL_DB_B
    }

    # Test MySQL connections
    mysql_a_ok = test_mysql_connection(config_a, "Hospital A")
    mysql_b_ok = test_mysql_connection(config_b, "Hospital B")
    
    # Test BigQuery connection
    bq_ok = test_bigquery_connection(BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID)

    print("\n--- Verification Summary ---")
    print(f"Hospital A DB Connection: {'SUCCESS' if mysql_a_ok else 'FAIL'}")
    print(f"Hospital B DB Connection: {'SUCCESS' if mysql_b_ok else 'FAIL'}")
    print(f"Google BigQuery Connection: {'SUCCESS' if bq_ok else 'FAIL'}")

    if all([mysql_a_ok, mysql_b_ok, bq_ok]):
        print("\n🎉 All environment checks passed. You are ready for Phase 2!")
    else:
        print("\n⚠️ Please review the error messages and correct your setup.")
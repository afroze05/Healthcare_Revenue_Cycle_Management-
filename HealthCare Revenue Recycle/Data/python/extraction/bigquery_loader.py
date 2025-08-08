# ==============================================================================
# SCRIPT: fix_data.py (Version 2 - The Correct Fix)
# DESCRIPTION:
# This script corrects the true root cause: 'ServiceDate' is missing from
# BOTH fact tables. It uses dim_date as the source of truth to repair
# fact_transactions first, and then uses the repaired transactions table
# to fix fact_claims.
# ==============================================================================

import pandas as pd
import os
import logging

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s] - %(message)s')
STAGING_DIR = r'C:\Users\Afroze\OneDrive\Desktop\HealthCare Revenue Recycle (2)\HealthCare Revenue Recycle\Data\staging'

# --- File Paths ---
transactions_path = os.path.join(STAGING_DIR, 'fact_transactions.parquet')
claims_path = os.path.join(STAGING_DIR, 'fact_claims.parquet')
date_dim_path = os.path.join(STAGING_DIR, 'dim_date.parquet')

# --- Main Execution Block ---
if __name__ == "__main__":
    try:
        logging.info("--- Starting Data Repair Process (V2) ---")

        # 1. Read all necessary files
        logging.info(f"Reading date dimension from: {date_dim_path}")
        df_date = pd.read_parquet(date_dim_path)

        logging.info(f"Reading transactions data from: {transactions_path}")
        df_trans = pd.read_parquet(transactions_path)

        logging.info(f"Reading claims data from: {claims_path}")
        df_claims = pd.read_parquet(claims_path)

        # --- PART 1: FIX fact_transactions ---
        logging.info("--- Repairing fact_transactions ---")
        if 'ServiceDate' in df_trans.columns:
            logging.info("ServiceDate already exists in fact_transactions. Skipping.")
        else:
            logging.warning("ServiceDate is MISSING from fact_transactions. Adding it from dim_date.")
            # Rename 'full_date' in the dimension table to 'ServiceDate' for the merge
            date_lookup = df_date[['date_sk', 'full_date']].rename(columns={'full_date': 'ServiceDate'})
            
            # Merge to add the ServiceDate column
            df_trans = pd.merge(df_trans, date_lookup, on='date_sk', how='left')
            
            # Overwrite the old transactions file with the fixed one
            df_trans.to_parquet(transactions_path, index=False)
            logging.info("SUCCESS: fact_transactions.parquet has been fixed and saved.")

        # --- PART 2: FIX fact_claims ---
        logging.info("--- Repairing fact_claims ---")
        if 'ServiceDate' in df_claims.columns:
            logging.info("ServiceDate already exists in fact_claims. Skipping.")
        else:
            logging.warning("ServiceDate is MISSING from fact_claims. Adding it from the repaired transactions table.")
            # Now we can use the method from before, because df_trans is fixed
            service_dates = df_trans[['TransactionID', 'ServiceDate']].drop_duplicates()
            df_claims = pd.merge(df_claims, service_dates, on='TransactionID', how='left')

            # Overwrite the old claims file with the fixed one
            df_claims.to_parquet(claims_path, index=False)
            logging.info("SUCCESS: fact_claims.parquet has been fixed and saved.")

        logging.info("--- ✅✅✅ All Data Repaired Successfully! ✅✅✅ ---")

    except FileNotFoundError as e:
        logging.error(f"FATAL ERROR: Could not find a required file. Please ensure all three files exist: fact_transactions, fact_claims, and dim_date.")
        logging.error(f"Missing file: {e.filename}")
    except Exception as e:
        logging.error("<<<<<<<<<< AN UNEXPECTED ERROR OCCURRED >>>>>>>>>>", exc_info=True)
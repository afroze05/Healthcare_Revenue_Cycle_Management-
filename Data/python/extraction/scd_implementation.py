# ==============================================================================
# FINAL MASTER SCRIPT (PHASE 5 - FINAL & SIMPLE)
# ==============================================================================
import pandas as pd
from datetime import datetime, timedelta
import logging
import os

from extraction import run_extraction
from transform import run_all_transformations
from dimensional_modeling import run_modeling

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s] - %(message)s')
STAGING_DIR = './Data/staging'

# ==============================================================================
#                   --- NEW, SIMPLER, CORRECT SCD LOGIC ---
# ==============================================================================
def apply_scd_type2(new_dim_patients: pd.DataFrame, existing_dim_patients: pd.DataFrame) -> pd.DataFrame:
    """A new, simplified, and robust function to apply SCD Type 2 logic."""
    logging.info("Applying NEW, SIMPLIFIED and Correct SCD Type 2 logic...")
    
    attributes_to_track = ['Address', 'LastName']
    
    # Handle the very first run
    if existing_dim_patients.empty:
        new_dim_patients['version'] = 1
        new_dim_patients['effective_date'] = datetime.now().date()
        new_dim_patients['expiry_date'] = pd.NaT
        new_dim_patients['is_current'] = True
        return new_dim_patients

    # Perform the merge against currently active records
    merged = pd.merge(
        existing_dim_patients[existing_dim_patients['is_current']],
        new_dim_patients, on='unified_patient_id', how='outer',
        suffixes=('_old', '_new'), indicator=True
    )
    
    # --- Step 1: Prepare column name lists ---
    # This is the key to avoiding the KeyError. We define our column sets clearly.
    # Columns from the new batch (e.g., 'FirstName_new', 'age_new')
    new_cols = [c for c in merged.columns if c.endswith('_new')]
    # Columns from the existing dimension (e.g., 'FirstName_old', 'version_old')
    old_cols = [c for c in merged.columns if c.endswith('_old')]

    # --- Step 2: Handle records that have CHANGED ---
    changed_mask = (merged['_merge'] == 'both')
    change_detected_mask = False
    for attr in attributes_to_track:
        change_detected_mask |= (merged[f'{attr}_old'].fillna('') != merged[f'{attr}_new'].fillna(''))
    
    changed_df = merged[changed_mask & change_detected_mask]

    # Create expired versions for the changed records
    expired_records = pd.DataFrame()
    new_versions = pd.DataFrame()
    if not changed_df.empty:
        logging.info(f"  > Found {len(changed_df)} patients with updated information.")
        
        # Take the '_old' columns to create the expired record
        expired_records = changed_df[['unified_patient_id'] + old_cols].copy()
        expired_records.columns = ['unified_patient_id'] + [c.replace('_old', '') for c in old_cols]
        expired_records['is_current'] = False
        expired_records['expiry_date'] = datetime.now().date() - timedelta(days=1)
        
        # Take the '_new' columns to create the new version
        new_versions = changed_df[['unified_patient_id'] + new_cols].copy()
        new_versions.columns = ['unified_patient_id'] + [c.replace('_new', '') for c in new_cols]
        new_versions['version'] = changed_df['version_old'].values + 1
        new_versions['effective_date'] = datetime.now().date()
        new_versions['expiry_date'] = pd.NaT
        new_versions['is_current'] = True
        
    # --- Step 3: Handle BRAND NEW records ---
    new_records_df = merged[merged['_merge'] == 'right_only']
    if not new_records_df.empty:
        logging.info(f"  > Found {len(new_records_df)} new patient records.")
        new_records = new_records_df[['unified_patient_id'] + new_cols].copy()
        new_records.columns = ['unified_patient_id'] + [c.replace('_new', '') for c in new_cols]
        new_records['version'] = 1
        new_records['effective_date'] = datetime.now().date()
        new_records['expiry_date'] = pd.NaT
        new_records['is_current'] = True
    else:
        new_records = pd.DataFrame()

    # --- Step 4: Keep records that were NOT affected ---
    # This includes unchanged records and retired records from the old data.
    keys_of_changed_patients = changed_df['unified_patient_id'].tolist()
    final_unchanged = existing_dim_patients[
        ~existing_dim_patients['unified_patient_id'].isin(keys_of_changed_patients)
    ].copy()
    
    # --- Step 5: Assemble the final dimension ---
    final_dimension = pd.concat([
        final_unchanged,
        expired_records,
        new_versions,
        new_records
    ], ignore_index=True)

    final_dimension.sort_values(by=['unified_patient_id', 'version'], inplace=True)
    final_dimension.reset_index(drop=True, inplace=True)
    final_dimension['patient_sk'] = final_dimension.index
    
    return final_dimension
    
# --- Main Execution Block ---
if __name__ == "__main__":
    try:
        logging.info("<<<<<<<<<< STARTING FULL DATA PROCESSING PIPELINE (Phases 2-5) >>>>>>>>>>")
        
        # Run all previous phases
        raw_db_data, raw_claims_data = run_extraction()
        transformed_db_data, transformed_claims_data = run_all_transformations(raw_db_data, raw_claims_data)
        final_dimensions, final_facts = run_modeling(transformed_db_data, transformed_claims_data)
        
        # The output of the modeling phase
        new_dim_patients_from_pipeline = final_dimensions['dim_patients']
        
        # Load the result from the last run
        existing_dim_path = os.path.join(STAGING_DIR, 'dim_patients.parquet')
        if os.path.exists(existing_dim_path):
            existing_dim_patients = pd.read_parquet(existing_dim_path)
        else:
            existing_dim_patients = pd.DataFrame()
            
        # Apply SCD
        final_dim_patients = apply_scd_type2(new_dim_patients_from_pipeline, existing_dim_patients)
        
        # Replace the fresh dimension with our final, history-enabled one
        final_dimensions['dim_patients'] = final_dim_patients
        
        # Save ALL final tables
        logging.info(f"--- Saving all final data models to: {STAGING_DIR} ---")
        os.makedirs(STAGING_DIR, exist_ok=True)
        for name, df in final_dimensions.items():
            path = os.path.join(STAGING_DIR, f"{name}.parquet")
            df.to_parquet(path, index=False)
        for name, df in final_facts.items():
            path = os.path.join(STAGING_DIR, f"{name}.parquet")
            df.to_parquet(path, index=False)

        print("\n\n" + "="*80)
        print("✅  SUCCESS: DATA PROCESSING COMPLETE. ALL FINAL TABLES SAVED TO STAGING. ✅")
        
    except Exception as e:
        logging.error("<<<<<<<<<< PIPELINE FAILED >>>>>>>>>>", exc_info=True)
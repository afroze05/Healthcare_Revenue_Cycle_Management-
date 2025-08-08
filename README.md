# Healthcare_Revenue_Cycle_Management(RCM)
**Project Overview:**
This repository contains a complete data engineering solution for Healthcare Revenue Cycle Management (RCM). The goal is to address real-world challenges faced by hospital networks in managing and analyzing financial, claims, and operational data by consolidating disparate systems, improving data quality, and enabling actionable analytics.

Here, data is sourced from hospital databases and monthly insurance claim files, processed with Python, and loaded into Google BigQuery for rich analytics and reporting.

**Directory Structure:**
<img width="700" height="728" alt="image" src="https://github.com/user-attachments/assets/63b7b9d3-cfa3-42f0-a98a-2945c5541f65" />


**1. Environment Setup:**

Python Environment
Create and activate a virtual environment. Install requirements as listed:

text
python -m venv venv
source venv/bin/activate  # Or venv\Scripts\activate for Windows
pip install pandas sqlalchemy mysql-connector-python google-cloud-bigquery
Database Initialization
Set up local MySQL with both hospital1_db and hospital2_db. Load the provided sample data under Data/data_extracts/hospital_dbs.

BigQuery Setup
Set up a GCP project and enable BigQuery. Prepare authentication via your service account credentials.

**2. Configuration:**
Update script parameters (database connections, file paths, BigQuery dataset credentials) as required in relevant Python modules.

**💡 Project Workflow**

**Data Extraction:**
Extracts patient, provider, claims, and transactions from hospital databases and claim CSVs.
See: python_extraction/extraction.py, python_extraction/data_exploration.py.

**Data Transformation & Cleaning:**
Cleanses data for quality: removing duplicates, standardizing formats, validating information.
See: python_extraction/transform.py.

**Dimensional Modeling:**
Builds star schema: fact tables for transactions and claims; dimensions for patients, providers, procedures, and date.
See: python_extraction/dimensional_modeling.py.

**Slowly Changing Dimension (SCD) Type 2:**
Implements SCDv2 logic for patient data to track historical changes and versioning.
See: python_extraction/scd_implementation.py.

**Data Loading:**
Final, cleaned tables are loaded into BigQuery.
See: python_extraction/bigquery_loader.py.

**Analytics & Visualization:**
Analytical SQL queries for KPIs and dashboards.
Sample dashboard: rcm_analytics/Dashboard.png.

**Sample Dashboard & Reporting:**

Dashboard Highlights (see rcm_analytics/Dashboard.png):
Monthly revenue summary
Comparison of total collections vs. billed revenue
Monthly revenue trend visualization
Tabular breakdown of revenue across months

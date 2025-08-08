# Healthcare_Revenue_Cycle_Management(RCM)
**Project Overview:**
This repository contains a complete data engineering solution for Healthcare Revenue Cycle Management (RCM). The goal is to address real-world challenges faced by hospital networks in managing and analyzing financial, claims, and operational data by consolidating disparate systems, improving data quality, and enabling actionable analytics.

Data is sourced from hospital databases and monthly insurance claim files, processed using Python, and loaded into Google BigQuery for high-performance analytics and reporting.

**Project Overview**
`HEALTHCARE REV .../
├── Data/
│   ├── claims/
│   │   ├── hospital1_claim_data.csv
│   │   └── hospital2_claim_data.csv
│   ├── cptcode/
│   │   └── cptcodes.csv
│   └── data_extracts/
│       ├── hospital_dbs/
│       │   ├── hospital1_db/
│       │   └── hospital2_db/
├── python_extraction/
│   ├── bigquery_loader.py
│   ├── data_exploration.py
│   ├── dimensional_modeling.py
│   ├── extraction.py
│   ├── scd_implementation.py
│   ├── transform.py
│   └── __pycache__/
├── rcm_analytics/
│   └── Dashboard.png
└── staging/
`
**1. Environment Setup:**
Python Environment
Create and activate a virtual environment, then install the necessary packages:
python -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate
pip install pandas sqlalchemy mysql-connector-python google-cloud-bigquery

**Database Initialization:**
Set up your local MySQL instance with the two hospital databases (hospital1_db and hospital2_db). Load the provided sample data located in Data/data_extracts/hospital_dbs.

**BigQuery Setup:**
Create a Google Cloud Platform project, enable the BigQuery API, and configure authentication by placing your service account credentials appropriately.

**2. Configuration:**
Update script parameters such as database connection strings, file paths, and BigQuery dataset details in the relevant Python modules under python_extraction/ to match your local and cloud environment.

**Project Workflow**
**Data Extraction:**
Extract patient, provider, claims, and transaction data from hospital databases and monthly claims CSV files.
See: python_extraction/extraction.py, python_extraction/data_exploration.py

**Data Transformation & Cleaning:**
Clean the data by removing duplicates, standardizing formats, and validating records to ensure high quality.
See: python_extraction/transform.py

**Dimensional Modeling:**
Construct the star schema with fact tables for transactions and claims, and dimension tables for patients, providers, procedures, and dates.
See: python_extraction/dimensional_modeling.py

**Slowly Changing Dimension (SCD) Type 2:**
Implement SCD Type 2 logic to track historical changes in patient data over time for accurate versioning and auditing.
See: python_extraction/scd_implementation.py

**Data Loading:**
Load the final transformed and modeled data into Google BigQuery for scalable querying and analysis.
See: python_extraction/bigquery_loader.py

**Analytics & Visualization:**
Perform analytical SQL queries to generate KPIs and dashboards.
Sample dashboard image available at rcm_analytics/Dashboard.png

**Sample Dashboard & Reporting:**
Dashboard Highlights (refer to rcm_analytics/Dashboard.png):
Monthly revenue summary
Comparison of total collections vs. billed revenue
Monthly revenue trend visualization
Tabular breakdown of revenue across months


# sql_data_warehouse_project

Welcome to my first build of a data warehouse with SQL server, including ETL processes, analytics and data modelling

---- Process and journey to be updated as I progress

Personal goals for the project:
    Utilise standardized naming conventions for ease of reading and writing as well as experience for future team projects
    For this project snake_case was utilised. 
    Table name format: layer.sourcetype_sourcedata
    E.g., bronze.crm_prd_info
    Column name format: snake_case
    E.g., cst_key

-- Specifications:
* Data Sources: import data from 2 sources (ERP and CRM) provided as CSV files.
* Data Quality: cleanse and resolve data quality issues prior to analysis
* Integration: combine both sources into a single, user-friendly data model design for analytical querying
* Scope: focus on the latest dataset only; historization of data is nnot required
* Documentation: provide clear documentation of the data model to support both business stakeholders and analytics usage


Process Beginnings: 
1) Download of source ERP/CRM datasets
2) Creation of the bronze.layer by making tables to match source datasets before importing the data, truncating and inserting into related tables
3) Data checked post truncate to ensure quality before a stored procedure was created to load the bronze layer
4) Silver layer tables created. Contents of tables populated with cleaned data from the bronze layer tables by truncate and insert
5) Stored procedure to load the silver layer (cleaned data) created
6) Queries created for the gold layer that joined several cleaned tables into dimensions (customers, products), and a fact table (fact_sales) containing sales information and linked to the dimensions by surrogate keys


Issues Faced:
Error occured when having multiple queries using the same Database - SQL server permissions changed to allow multiple users to access at once (although this is a local project, it was helpful to understand if required in future)
Halfway through the project (when cleaning silver layers), the database no longer recognised the imported data. Various checks were done, path for dataset checked, reaffirmed, source data checked, row and column terminator checked. After various tests and no success, deletion and redownload worked immediately
Error by myself when creating the tablets where I named one of the tablets differently to the standard format within the rest of the tables. This caused issues with insert/truncate and the stored procedure process. This was amended by re-creating the table with the correct information and running the stored procedure to load everything


#  PySpark Data Analysis Workflow with AWS S3 and Databricks
## Overview

This PySpark script demonstrates a comprehensive data analysis workflow using Databricks. It covers essential steps, including mounting an S3 bucket, data cleaning, and analysis tasks. The provided code assumes a Spark environment on Databricks with a DataFrame named cleaned_df_user and cleaned_df_pin.

### 1. Mounting S3 Bucket to Databricks
To access data stored in an S3 bucket, it's necessary to mount the bucket to Databricks. This process is typically done using AWS access keys.

### 2. Data Cleaning
Cleaning tasks perfomed on 3 dataframes with the data received from S3 bucket: df_pin, df_geo, df_user, resulting in three new dataframes: cleaned_df_pin, cleaned_df_geo, cleaned_df_user.
Cleaning tasks included: 
- Replace empty entries and entries with no relevant data in each column
- Perform the necessary transformations on columns
- Updating entries
- Reordering, creating, droping, renaming columns

### 3. Analysis Tasks
Performing analysis tasks on dataframes in Databricks by using Data Transformation techniques: window functions, join, groupby, aggregate, select, using aliases.

### 4. Additional Information
Usage: Replace DataFrame names and adjust configurations based on your specific environment.
Output: The provided code generates DataFrames with insights into median follower counts and is adaptable for further analyses.


# MWAA-Databricks Integration with Airflow DAG
## Overview

This project facilitates the integration of AWS Managed Workflows for Apache Airflow (MWAA) and Databricks. You have already been granted access to the MWAA environment (Databricks-Airflow-env) and its associated S3 bucket (mwaa-dags-bucket). This guide explains the process of creating an Airflow DAG to trigger a Databricks Notebook on a daily schedule.

## Prerequisites
AWS account with MWAA environment access.
Permissions to upload/update files in mwaa-dags-bucket.
No need to create an API token or set up MWAA-Databricks connection.
Ensure your DAG file is named <your_UserId_dag.py>.
Steps taken:
Create Airflow DAG:
Develop an Airflow DAG to trigger the execution of a Databricks Notebook. Save the DAG file with the correct name <your_UserId_dag.py>.

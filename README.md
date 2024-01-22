# Pinterest Data Pipeline project

## Overview

Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, you'll create a similar system using the AWS Cloud.

### Milestone 1. Set up the environment.

Task 1. Set up Github
Task 2.  Set up AWS.


### Milestone 2. Get Started.

Task 1. Download the Pinterest infrastructure
Inside you will find the user_posting_emulation.py, that contains the login credentials for a RDS database, which contains three tables with data resembling data received by the Pinterest API when a POST request is made by a user uploading data to Pinterest:
pinterest_data contains data about posts being updated to Pinterest
geolocation_data contains data about the geolocation of each Pinterest post found in pinterest_data
user_data contains data about the user that has uploaded each post found in pinterest_data
Run the provided script and print out pin_result, geo_result and user_result. These each represent one entry in their corresponding table. Get familiar with the data, as you will be working with it throughout the project.

Task 2. Sign in to AWS console.

 
### Milestone 3.  Batch processing: configure the EC2 Kafka client

Task 1.  Create a .pem key file locally
Task 2. Connect to the EC2 instance.
Task 3. Set up Kafka on the EC2 instance.
Task 4. Create Kafka topics.


### Milestone 4. Batch processing: Connect an MSK cluster to a S3 bucket.

Task 1. Create a custom plugin with MSK connect.
Task 2. Create a connector with MSK connect.


### Milestone 5. Batch processing: configuring an API and API Getaway

Task 1. Build a Kafka REST proxy integration method for an API
Task 2. Set up the Kafka REST proxy on the EC2 client
Task 3. Send data to the API (user_posting_emulation_streaming.py)


### Milestone 6. Batch processing: Databricks (databricks-s3-mount.py)

Task 1. Set up your own Databricks account
Task 2. Mount an S3 bucket to Databricks
Task 3. Document your experience and update latest code changes to Github


### Milestone 7. Batch processing: Spark on Databricks (databricks-spark.py)

The PySpark script created in this milestone demonstrates a comprehensive data analysis workflow using Databricks. It covers essential steps, including mounting an S3 bucket, data cleaning, and analysis tasks. 

Task 1. Clean the DataFrame that contains information about Pinterest posts
Task 2. Clean the DataFrame that contains information about geolocation
Task 3. Clean the DataFrame that contains information about users
Task 4. Find the most popular category in each country
Task 5. Find which was the most popular category each year
Task 6. Find the users with most followers in each country
Task 7. Find the most popular category for different age groups
Task 8. Find the median follower count for different age groups
Task 9. Find how many users have joined each year
Task 10. Find the median follower count of users based on their joining year 
Task 11. Find the median follower count of users based on their joining year and age group
Task 12. Document your experience
Task 13. Update latest code changes to GitHub


### Milestone 8: Batch processing: AWS MWAA (12951463f185_dag.py)

Task 1: Create and upload a DAG to a MWAA environment
You should schedule the DAG to run daily.
Task 2. Trigger a DAG that runs a Databricks notebook


### Milestone 9. Stream Processing: AWS Kinesis (kinesis-streaming.py)

Task 1. Create Data Streams using Kinesis Data Streams
Task 2. Configure an API with Kinesis Proxy integration
Task 3. Send data to Kinesis streams (user_posting_emulation_streaming)
Task 4. Read data from Kinesis streams in Databricks
Task 5. Transform Kinesis streams in Databricks
Task 6. Write the streaming data to Delta Tables
Task 7. Document your experience
Task 8. Update the latest code changes to Github


#  Milestone 7: PySpark Data Analysis Workflow with AWS S3 and Databricks
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

from pyspark.sql.functions import *
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-12951463f185-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/my-mount-point"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# Display if correctly mounted
display(dbutils.fs.ls("/mnt/my-mount-point"))

%sql
SET spark.databricks.delta.formatCheck.enabled=false

# Load pin folder
file_location = "/mnt/my-mount-point/topics/12951463f185.pin/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_pin)

# Load geo folder
file_location = "/mnt/my-mount-point/topics/12951463f185.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_geo)

# Load user folder
file_location = "/mnt/my-mount-point/topics/12951463f185.user/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_user)
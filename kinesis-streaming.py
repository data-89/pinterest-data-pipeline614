from pyspark.sql.types import *
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

%sql
SET spark.databricks.delta.formatCheck.enabled=false


# Reading Kinesis 'user' stream
df_user = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-12951463f185-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Define a streaming schema using StructType
streaming_schema_user = StructType([
    StructField("age", StringType(), True),
    StructField("date_joined", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("ind", StringType(), True),
    StructField("last_name", StringType(), True)
    # Add more fields as needed
])

new_df_user = df_user.withColumn("jsonData",from_json(col("data"),streaming_schema_user)).select("jsonData.*")
display(new_df_user)

# Reading Kinesis 'geo' stream
df_geo = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-12951463f185-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Define a streaming schema using StructType
streaming_schema_geo = StructType([
    StructField("country", StringType(), True),
    StructField("ind", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("timestamp", StringType(), True)
    # Add more fields as needed
])

new_df_geo = df_geo.withColumn("jsonData",from_json(col("data"),streaming_schema_geo)).select("jsonData.*")
display(new_df_geo)

# Reading Kinesis 'pin' stream
df_pin = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-12951463f185-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

from pyspark.sql.types import StructType, StructField, StringType

# Define a streaming schema using StructType
streaming_schema_pin = StructType([
    StructField("category", StringType(), True),
    StructField("description", StringType(), True),
    StructField("downloaded", StringType(), True),
    StructField("follower_count", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("index", StringType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("save_location", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("title", StringType(), True),
    StructField("unique_id", StringType(), True)
    # Add more fields as needed
])

new_df_pin = df_pin.withColumn("jsonData",from_json(col("data"),streaming_schema_pin)).select("jsonData.*")
display(new_df_pin)

# Spark Datacleaning
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, expr, from_json, array

# Replace empty entries and entries with no relevant data in each column with Nones
cleaned_df_pin = new_df_pin.replace({"": "None"})
cleaned_df_pin = cleaned_df_pin.dropDuplicates()
print(cleaned_df_pin)

# Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int
cleaned_df_pin = cleaned_df_pin.withColumn(
    "follower_count",
    expr("CAST(regexp_replace(regexp_replace(follower_count, '[kK]', '000'), '[mM]', '000000') AS INT)")
)

# Show all columns and its types
cleaned_df_pin.printSchema()

# Ensure that each column containing numeric data has a numeric data type
cleaned_df_pin = cleaned_df_pin.withColumn("index", cleaned_df_pin["index"].cast("int"))
cleaned_df_pin = cleaned_df_pin.withColumn("downloaded", cleaned_df_pin["downloaded"].cast("int"))

# Clean the data in the save_location column to include only the save location path
save_location_expr = "(?:Local save in )?(.*)"
cleaned_df_pin = cleaned_df_pin.withColumn(
    "cleaned_save_location",
    regexp_extract("save_location", save_location_expr, 1)
)

# Rename the index column to ind
cleaned_df_pin = cleaned_df_pin.withColumnRenamed("index", "ind")

# Reorder the DataFrame columns
cleaned_df_pin = cleaned_df_pin.select("ind", "unique_id", "title", "description", 
    "follower_count", "poster_name", "tag_list", 
    "is_image_or_video", "image_src", "save_location", 
    "category")

# Clean the df_geo DataFrame:
# Create a new column coordinates that contains an array based on the latitude and longitude columns
cleaned_df_geo = new_df_geo.withColumn("coordinates", array("latitude", "longitude"))
cleaned_df_geo = cleaned_df_geo.dropDuplicates()

# Drop the latitude and longitude columns from the DataFrame
cleaned_df_geo = cleaned_df_geo.drop("latitude", "longitude")

# Convert the timestamp column from a string to a timestamp data type
cleaned_df_geo = cleaned_df_geo.withColumn("timestamp", to_timestamp("timestamp"))

# Reorder the DataFrame columns
cleaned_df_geo = cleaned_df_geo.select("ind", "country", "coordinates", "timestamp")

# Clean the df_user DataFrame
# Create a new column user_name that concatenates the information found in the first_name and last_name columns
cleaned_df_user = new_df_user.withColumn("user_name", array("first_name", "last_name"))
cleaned_df_user = cleaned_df_user.dropDuplicates()

# Drop the first_name and last_name columns from the DataFrame
cleaned_df_user = cleaned_df_user.drop("first_name", "last_name")

# Convert the date_joined column from a string to a timestamp data type
cleaned_df_user = cleaned_df_user.withColumn("date_joined", to_timestamp("date_joined"))

# Reorder the DataFrame columns 
cleaned_df_user = cleaned_df_user.select("ind", "user_name", "age", "date_joined")
cleaned_df_user.printSchema()

# Write the streaming data to Delta Tables
query_pin = (
    cleaned_df_pin
    .writeStream
    .format("delta") 
    .outputMode("append")
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/")
    .table("12951463f185_pin_table")
)

query_geo = (
    cleaned_df_geo
    .writeStream
    .format("delta") 
    .outputMode("append")
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/")
    .table("12951463f185_geo_table")
)

query_user = (
    cleaned_df_user
    .writeStream
    .format("delta") 
    .outputMode("append")
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/")
    .table("12951463f185_user_table")
)
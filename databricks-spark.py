# Spark Datacleaning
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, expr

# Replace empty entries and entries with no relevant data in each column with Nones
cleaned_df_pin = df_pin.replace({"": "None"})
df_pin.show()

# Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int
cleaned_df_pin = cleaned_df_pin.withColumn(
    "follower_count",
    expr("CAST(regexp_replace(regexp_replace(follower_count, '[kK]', '000'), '[mM]', '000000') AS INT)")
)
display(cleaned_df_pin)


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
cleaned_df_pin.show()


# Clean the df_geo DataFrame:
# Create a new column coordinates that contains an array based on the latitude and longitude columns
cleaned_df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))

# Drop the latitude and longitude columns from the DataFrame
cleaned_df_geo = cleaned_df_geo.drop("latitude", "longitude")

# Convert the timestamp column from a string to a timestamp data type
cleaned_df_geo = cleaned_df_geo.withColumn("timestamp", to_timestamp("timestamp"))

# Reorder the DataFrame columns
cleaned_df_geo = cleaned_df_geo.select("ind", "country", "coordinates", "timestamp")
cleaned_df_geo.show()


# Clean the df_user DataFrame
# Create a new column user_name that concatenates the information found in the first_name and last_name columns
cleaned_df_user = df_user.withColumn("user_name", array("first_name", "last_name"))

# Drop the first_name and last_name columns from the DataFrame
cleaned_df_user = cleaned_df_user.drop("first_name", "last_name")

# Convert the date_joined column from a string to a timestamp data type
cleaned_df_user = cleaned_df_user.withColumn("date_joined", to_timestamp("date_joined"))

# Reorder the DataFrame columns 
cleaned_df_user = cleaned_df_user.select("ind", "user_name", "age", "date_joined")
cleaned_df_user.printSchema()
cleaned_df_user.show()


# Find the most popular Pinterest category people post to based on their country.
from pyspark.sql.functions import desc, dense_rank, col
from pyspark.sql.window import Window

# Joining pin and geo dataframes
joined_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_pin["ind"] == cleaned_df_geo["ind"], how="outer")

# Group by country and category, and count the number of posts in each category for each country
category_counts = joined_df.groupBy("country", "category").count()

max_category_per_country = (
    category_counts
    .withColumn("row_number", 
                dense_rank().over(Window.partitionBy("country").orderBy(desc("count"))))
    .filter(col("row_number") == 1)
    .drop("row_number")
)
max_category_per_country = max_category_per_country.withColumnRenamed("count", "category_count")
max_category_per_country.show()


# Find how many posts each category had between 2018 and 2022
from pyspark.sql.functions import year

# Joining pin and geo dataframes
joined_df = cleaned_df_pin.join(cleaned_df_geo, cleaned_df_pin["ind"] == cleaned_df_geo["ind"], how="outer")

# Extract the year from the 'timestamp' column and filter for the specified years (2018-2022)
filtered_df = joined_df.withColumn("post_year", year("timestamp")).filter("post_year BETWEEN 2018 AND 2022")

# Group by 'post_year' and 'category', and count the number of posts in each category for each year
category_counts = filtered_df.groupBy("post_year", "category").count()

# Rename the count column to 'category_count'
category_counts = category_counts.withColumnRenamed("count", "category_count")

# Show the result
category_counts.show()
joined_df.show()


# For each country find the user with the most followers
from pyspark.sql.functions import max, col

# Join all three dfs
joined_df = cleaned_df_geo.join(cleaned_df_pin, "ind", how="outer")
all_df_joined = cleaned_df_user.join(joined_df, cleaned_df_user["ind"] == joined_df["ind"], how="outer")

# Find the user with the most followers for each country
result_df = (
    all_df_joined
    .groupBy("country", "poster_name")
    .agg(
        max("follower_count").alias("max_follower_count")
    )
)

# Show the result
result_df.show()


# Based on the above query, find the country with the user with most followers.
result_df_global = (
    result_df
    .groupBy("country")
    .agg(
        max("max_follower_count").alias("max_follower_count")
    )
    .orderBy(col("max_follower_count").desc())  # Order by max_follower_count in descending order
    .limit(1)  # Limit to only one entry
    .select("country", "max_follower_count")  # Select the required columns
)

# Show the result
result_df_global.show()


# Find the most popular category for different age groups
# Create age groups
all_df_joined = all_df_joined.withColumn(
    "age_group",
    when((col("age") >= 18) & (col("age") <= 24), "18-24")
    .when((col("age") >= 25) & (col("age") <= 35), "25-35")
    .when((col("age") >= 36) & (col("age") <= 50), "36-50")
    .when(col("age") > 50, "+50")
    .otherwise("Unknown")
)

# Group by 'age_group' and 'category', and count the number of posts in each category for each age group
category_counts = all_df_joined.groupBy("age_group", "category").count()

# Find the most popular category for each age group based on the count
max_category_per_age_group = (
    category_counts
    .withColumn("row_number", row_number().over(Window.partitionBy("age_group").orderBy(desc("count"))))
    .filter(col("row_number") == 1)
    .drop("row_number")
    .select("age_group", "category", "count")
    .withColumnRenamed("count", "category_count")
)

# Show the result
max_category_per_age_group.show()


# Find the median follower count for users in different age groups
from pyspark.sql.functions import col, when, percentile_approx

# Create age groups
age_groups_df = all_df_joined.withColumn(
    "age_group",
    when((col("age") >= 18) & (col("age") <= 24), "18-24")
    .when((col("age") >= 25) & (col("age") <= 35), "25-35")
    .when((col("age") >= 36) & (col("age") <= 50), "36-50")
    .when(col("age") > 50, "+50")
    .otherwise("Other")
)

# Group by 'age_group' and calculate the median follower count for each age group
median_follower_count_by_age = (
    all_df_joined
    .groupBy("age_group")
    .agg(
        percentile_approx("follower_count", 0.5).alias("median_follower_count")
    )
)

# Show the result
median_follower_count_by_age.show()


# Find how many users have joined between 2015 and 2020
from pyspark.sql.functions import year, col

# Extract the year from the 'timestamp' column
cleaned_df_user = cleaned_df_user.withColumn("post_year", year("date_joined"))

# Filter the data for users who joined between 2015 and 2020
users_joined_between_2015_and_2020 = (
    cleaned_df_user
    .filter((col("post_year") >= 2015) & (col("post_year") <= 2020))
    .groupBy("post_year")
    .agg(
        col("post_year"),
        count("ind").alias("number_users_joined")
    )
    .select("post_year", "number_users_joined")
)

# Show the result
users_joined_between_2015_and_2020.show()


# Find the median follower count of users have joined between 2015 and 2020
# Extract the year from the 'timestamp' column
all_df_joined = all_df_joined.withColumn("post_year", year("date_joined"))

# Filter the data for users who joined between 2015 and 2020
filtered_df = (
    all_df_joined
    .filter((col("post_year") >= 2015) & (col("post_year") <= 2020))
)

# Group by 'post_year' and calculate the median follower count for each year
median_follower_count_by_year = (
    filtered_df
    .groupBy("post_year")
    .agg(
        col("post_year"),
        percentile_approx("follower_count", 0.5).alias("median_follower_count")
    )
    .select("post_year", "median_follower_count")
)

# Show the result
median_follower_count_by_year.show()


# Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of

# Filter the data for users who joined between 2015 and 2020
filtered_df = (
    age_groups_df
    .filter((col("post_year") >= 2015) & (col("post_year") <= 2020))
)

# Group by 'age_group' and 'post_year' and calculate the median follower count for each combination
median_follower_count_by_age_and_year = (
    filtered_df
    .groupBy("age_group", "post_year")
    .agg(
        col("age_group"),
        col("post_year"),
        percentile_approx("follower_count", 0.5).alias("median_follower_count")
    )
    .select("age_group", "post_year", "median_follower_count")
)

# Show the result
median_follower_count_by_age_and_year.show()

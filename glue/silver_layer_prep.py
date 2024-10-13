from pyspark.sql import SparkSession

bronze_layer_bucket = "s3a://bronze-layer-raw-data-finstream/"
silver_layer_bucket = "s3a://silver-layer-cleaned-data-finstream/"

# Initialize a Spark session
spark = SparkSession.builder.appName("FinStream AI - Silver Layer Data Preparation").getOrCreate()

# Create raw DataFrame
df_raw = spark.read.parquet(bronze_layer_bucket)

# Drop rows with any null values
df_cleaned_na = df_raw.na.drop()

# Remove negative customer_ids
df_cleaned_user_id = df_cleaned_na.filter(df_cleaned_na.customer_id >= 0)

# Remove 'invalid_type' transaction_types
df_cleaned_transaction_type = df_cleaned_user_id \
    .filter(df_cleaned_user_id.transaction_type != 'invalid_type')

# Remove 'unknown_currency' currency
df_cleaned_currency = df_cleaned_transaction_type \
    .filter(df_cleaned_transaction_type.currency != 'unknown_currency')

# Remove records with out of range 'year', 'month', 'day'
df_cleaned_dates = df_cleaned_currency \
    .filter(df_cleaned_currency.year.between(2000, 2024)) \
    .filter(df_cleaned_currency.month.between(1, 12)) \
    .filter(df_cleaned_currency.day.between(1, 31))

# Write to silver layer bucket as parquet partitioned by year and month
df_cleaned_dates.write.mode("append").partitionBy("year", "month").parquet(silver_layer_bucket)

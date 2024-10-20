import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

bronze_layer_bucket = "bronze-layer-raw-data-finstream"
silver_layer_bucket = "s3a://silver-layer-cleaned-data-finstream/"

# Initialize a Spark session
spark = SparkSession.builder.appName("FinStream AI - Silver Layer Data Preparation").getOrCreate()

# Initialize boto3 client
s3_client = boto3.client('s3')

# List the objects in the bucket
response = s3_client.list_objects_v2(Bucket=bronze_layer_bucket)

# Filter only valid parquet paths (exclude paths like 'error/')
valid_paths = [f"s3a://{bronze_layer_bucket}/{obj['Key']}" for obj in response['Contents'] if obj['Key'].endswith('.parquet')]

if valid_paths:
    # Create raw DataFrame
    df_raw = spark.read.parquet(*valid_paths)
    
    # Exclude firehose error directory
    df_raw_filtered = df_raw.filter(~F.input_file_name().contains('/error/'))
    
    # Drop rows with any null values
    df_cleaned_na = df_raw_filtered.na.drop()
    
    # Remove negative customer_ids
    df_cleaned_user_id = df_cleaned_na.filter(df_cleaned_na.customer_id >= 0)
    
    # Remove 'invalid_type' transaction_types
    df_cleaned_transaction_type = df_cleaned_user_id \
        .filter(df_cleaned_user_id.transaction_type != 'invalid_type')
    
    # Remove 'unknown_currency' currency
    df_cleaned_currency = df_cleaned_transaction_type \
        .filter(df_cleaned_transaction_type.currency != 'unknown_currency')
    
    # Remove records with out of range 'year', 'month', 'day'
    df_cleaned_dates = df_cleaned_currency \
        .filter(df_cleaned_currency.year.between(2000, 2024)) \
        .filter(df_cleaned_currency.month.between(1, 12)) \
        .filter(df_cleaned_currency.day.between(1, 31))
    
    # Write to silver layer bucket as parquet partitioned by year, month and day
    df_cleaned_dates.write.mode("append").partitionBy("year", "month", "day").parquet(silver_layer_bucket)
else:
    print("No valid parquet files found.")
    

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import config  # Importing configuration file

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CSV to BigQuery - Config-based") \
    .config("spark.jars", r"C:\Users\O.Danielz\Documents\su-de-assessment\spark-bigquery-with-dependencies_2.13-0.41.0.jar") \
    .getOrCreate()

# Access BigQuery configuration from config.py
project_id = config.BIGQUERY_PROJECT_ID
dataset_name = config.BIGQUERY_DATASET_NAME
temporary_gcs_bucket = config.TEMP_GCS_BUCKET
csv_to_table_mapping = config.CSV_TO_TABLE_MAPPING

# Process each CSV file and write it to its respective BigQuery table
for csv_path, table_name in csv_to_table_mapping.items():
    try:
        # Read the CSV file
        df = spark.read.option("header", True).csv(csv_path)
        
        # Add a modified_date column
        df_with_date = df.withColumn("modified_date", current_timestamp())
        
        # Define the full BigQuery table path
        bigquery_table = f"{project_id}:{dataset_name}.{table_name}"
        
        # Write the DataFrame to BigQuery
        df_with_date.write.format("bigquery") \
            .option("table", bigquery_table) \
            .option("temporaryGcsBucket", temporary_gcs_bucket) \
            .mode("overwrite") \
            .save()
        
        print(f"Successfully wrote {csv_path} to {bigquery_table}")
    
    except Exception as e:
        print(f"Failed to process {csv_path}. Error: {str(e)}")

# Stop the Spark session
spark.stop()
s
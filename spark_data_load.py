from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

# Step 1: Initialize SparkSession with Optimized Configurations
spark = SparkSession.builder \
    .appName("LoadCSVAndProcess") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.jars", r"/Users/samsonakporotu/SparkingFlow/postgresql-42.7.4.jar") \
    .getOrCreate()


# Step 2: Load the CSV File
csv_file_path = r"/Users/samsonakporotu/SparkingFlow/custom_1988_2020.csv"  # Replace with the actual path to your CSV file
raw_df = spark.read.csv(csv_file_path, header=False, inferSchema=True, sep=",")  # Ensure the delimiter is correct

# Limit the data to the first 50 million rows
raw_df = raw_df.limit(30000000)

# Cache the raw data if reused multiple times
raw_df.cache()

# Step 3: Rename Columns
column_names = [
    "year_month", 
    "exp_imp_flag", 
    "country", 
    "customs", 
    "hs_code", 
    "q1", 
    "quantity",  # Rename q2 to quantity
    "value"
]
df = raw_df.toDF(*column_names)

# Step 4: Write Data to PostgreSQL
db_url = "jdbc:postgresql://localhost:5433/postgres"  # Replace with your PostgreSQL details
db_properties = {
    "user": "postgres",  # Replace with your PostgreSQL username
    "password": "samzy",  # Replace with your PostgreSQL password
    "driver": "org.postgresql.Driver"
}
table_name = "sr_de_assessment.japan_customs_data"

# Write with repartitioning to optimize write performance
df.repartition(10).write \
    .jdbc(url=db_url, table=table_name, mode="overwrite", properties=db_properties)

# Step 5: Perform Aggregation
# Reduce shuffle data size by using repartition
df = df.repartition(100, "hs_code")  # Partition based on hs_code to balance processing

aggregated_df = df.drop("q1").groupBy(
    "exp_imp_flag", "country", "customs"
).agg(
    _sum("quantity").alias("total_quantity"),
    _sum("value").alias("total_value")
)

# Cache the aggregated data to reuse it efficiently
aggregated_df.cache()

# Step 6: Write Aggregated Data to PostgreSQL
aggregated_table_name = "sr_de_assessment.aggregated_data"

aggregated_df.write \
    .jdbc(url=db_url, table=aggregated_table_name, mode="overwrite", properties=db_properties)

# Step 9: Stop the Spark Session
spark.stop()

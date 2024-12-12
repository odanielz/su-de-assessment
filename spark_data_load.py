from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

#Initialize SparkSession
spark = SparkSession.builder \
    .appName("LoadCSVAndProcess") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.jars", r"/Users/odanielz/su-de-assessment/postgresql-42.7.4.jar") \
    .getOrCreate()


# read dataset from csv file
csv_file_path = r"/Users/odanielz/su-de-assessment/custom_1988_2020.csv"  
raw_df = spark.read.csv(csv_file_path, header=False, inferSchema=True, sep=",")  

raw_df = raw_df.limit(30000000)
raw_df.cache()

# Rename Columns because dataset has no headers
column_names = [
    "year_month", 
    "exp_imp_flag", 
    "country", 
    "customs", 
    "hs_code", 
    "q1", 
    "quantity",
    "value"
]
df = raw_df.toDF(*column_names)

# initiate postgres connection
db_url = "jdbc:postgresql://localhost:5433/postgres"  
db_properties = {
    "user": "postgres",  
    "password": "******",  
    "driver": "org.postgresql.Driver"
}
table_name = "sr_de_assessment.japan_customs_data"

# repartition to optimize write performance
df.repartition(10).write \
    .jdbc(url=db_url, table=table_name, mode="overwrite", properties=db_properties)

# define aggregated table
df = df.repartition(100, "hs_code") 

aggregated_df = df.drop("q1").groupBy(
    "exp_imp_flag", "country", "customs"
).agg(
    _sum("quantity").alias("total_quantity"),
    _sum("value").alias("total_value")
)

aggregated_df.cache()


aggregated_table_name = "sr_de_assessment.aggregated_data"

aggregated_df.write \
    .jdbc(url=db_url, table=aggregated_table_name, mode="overwrite", properties=db_properties)


spark.stop()

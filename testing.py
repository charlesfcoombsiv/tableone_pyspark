##############################
# Initialize a Spark Session
##############################

# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

# Print my_spark
print(my_spark)

# Print the tables in the catalog
print(my_spark.catalog.listTables())

print("hey")
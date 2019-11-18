##############################
# Initialize a Spark Session
##############################

# Import SparkSession from pyspark.sql
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pandas as pd

df_pan = pd.DataFrame({"PTID": [1, 2, 3, 4, 5], "name": ['John', 'Paul', 'Jones', 'Quincy', 'Amelie'],
                      "plan": ['PPO', 'HMO', 'PPO', 'FFS', 'HMO']})

df_spark = spark.createDataFrame(df_pan)

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

# Print my_spark
print(my_spark)

# Print the tables in the catalog
print(my_spark.catalog.listTables())

print("hey")
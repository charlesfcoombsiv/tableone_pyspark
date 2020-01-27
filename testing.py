##############################
# Initialize a Spark Session
##############################

# Import SparkSession from pyspark.sql
# from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from tableone_pyspark import tableone_pyspark


# Create spark session
spark = SparkSession.builder.getOrCreate()
print(spark)

# Create dataframe to analyze
df_pan = pd.DataFrame({"PID": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                       "name": ['John', 'John', 'John', 'Sally', 'Sally', 'Sally', 'Sally', 'Susi', None, None],
                       "region": ["East", "East", "East", "West", "West", "West", "South", "South", "South", "Arctic"],
                       "plan": ['PPO', 'HMO', 'PPO', 'FFS', None, 'PPO', 'PPO', 'HMO', None, 'FFS'],
                       "age": [18, 25, 21, 55, 65, 70, 85, 19, 34, 28],
                       "number_of_claims": [10, 20, 15, 29, 55, np.nan, np.nan, 20, 16, 18]})

df_spark = spark.createDataFrame(df_pan)

# Create the analysis dataframe
stat_out = tableone_pyspark(df= df_spark, spark_session = spark, col_to_strat= "plan", cols_to_analyze_list= ["name", "age"],
                            beautify= True, p_values= True)

stat_out.show()

# Print the tables in the catalog
print(spark.catalog.listTables())
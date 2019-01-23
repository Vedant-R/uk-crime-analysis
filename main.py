import glob
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

path = '<directory-path-to-your-dataset>'
crime_directories = glob.glob(path + '/*/*.csv')

# Creating Spark Config and Context
conf = SparkConf().setAppName("<your-app-name>")
sc = SparkContext(conf = conf)

# Configuring Spark Session
spark = SparkSession(sc)

# Creating SQL Context
sqlContext = SQLContext(sc)

# Loading CSV dataset in Spark Dataframe
df = sqlContext.read.format('com.databricks.spark.csv')\
                    .options(header='true')\
                    .load(crime_directories)

# Monthly crime counts by type for every street in the UK
df_permonth = df\
            .select("Month", "Crime type", "Location")\
            .groupBy("Month", "Crime type", "Location")\
            .count()\
            .show()

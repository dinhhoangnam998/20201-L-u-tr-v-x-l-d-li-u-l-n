import warnings
import os
from pyspark.sql import SparkSession

warnings.filterwarnings('ignore')

# Create Spark session
spark = SparkSession.builder \
    .appName('Amazon Review Analytic') \
    .master('local[*]') \
    .config("spark.executor.memory", "6g") \
    .config("spark.scheduler.mode", "FAIR") \
    .getOrCreate()

dir_path = '../data/json/'
for filename in os.listdir(dir_path):
    data = spark.read.json(dir_path + filename)
    asin = data.select('asin').distinct()
    asin.toPandas().to_csv(f'../data/asin_list/{filename}.csv')
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import warnings
from pyspark.ml import *
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import *
from pyspark.ml.evaluation import *
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import rand, lower
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from time import time


spark = SparkSession.builder \
    .appName('Amazon Review Analytic Spark Streaming') \
    .master('spark://hoangnam-msi:7077')\
    .config('spark.executor.memory', '3g')\
    .config('spark.driver.memory', '2g')\
    .config('spark.scheduler.mode', 'FAIR')\
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "hoangnam-msi:9092") \
    .option("subscribe", "bigdata") \
    .load()
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


def test(df, eid):

    print(df)
    df.show()


query = df.writeStream.foreachBatch(test).start()
query.awaitTermination()


# df.writeStream.foreachBatch(foreach_batch_function).start()


# def predict(df):
#     df = df.dropna(subset=['reviewText'])
#     df = df.withColumn('review', lower(df["reviewText"]))
#     model = PipelineModel.load('hdfs://hoangnam-msi:9000/user/hoangnam/btl/AMS_Model')
#     prediction = model.transform(df)
#     selected = prediction.select(['prediction'])
#     for row in selected.collect():
#         print(row)
#     return prediction

# def foreach_batch_function(df, epoch_id):
#     df.show()
#     # Transform and write batchDF
#     df = predict(df)
#     ds = df \
#     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "192.168.1.5:9092") \
#     .option("topic", "bigdata") \
#     .start()
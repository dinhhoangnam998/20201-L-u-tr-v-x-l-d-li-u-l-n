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
from pyspark.sql.functions import to_json, col, struct


spark = SparkSession.builder \
    .appName('Amazon Review Analytic Spark Streaming') \
    .master('spark://hoangnam-msi:7077')\
    .config('spark.executor.memory', '3g')\
    .config('spark.driver.memory', '2g')\
    .config("spark.mongodb.output.uri", "mongodb://hoangnam-msi/btl.predict_output") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "hoangnam-msi:9092") \
    .option("subscribe", "bigdata") \
    .load() \
    .selectExpr("CAST(value AS STRING)")


def foreach_batch_function(dfBatch, bid):
    rdd = dfBatch.rdd.map(lambda r: r.value)
    df = spark.read.json(rdd)
    # Transform and write batchDF
    df = predict(df)
    if (df is not None):
        df.select(to_json(struct([col(c).alias(c) for c in df.columns])).alias("value")) \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "hoangnam-msi:9092") \
            .option("topic", "predict_output") \
            .save()
        # df.write.format("mongo").mode("append").save()


def predict(df):
    # df = df.dropna(subset=['reviewText'])
    if(df.count() > 0):
        df = df.withColumn('review', lower(df["reviewText"]))
        model = PipelineModel.load(
            'hdfs:///user/hoangnam/btl/AMS_Model')
        prediction = model.transform(df)
        selected = prediction.select(['prediction'])
        prediction.show()
        return prediction


query = df.writeStream.foreachBatch(foreach_batch_function).start()
query.awaitTermination()

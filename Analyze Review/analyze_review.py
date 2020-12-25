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

warnings.filterwarnings('ignore')


def pipeline_lm(Model=NaiveBayes, numFeatures=10000):
    tokenizer = Tokenizer(inputCol="review", outputCol="words")
    # remove stop words
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    # Convert to TF words vector
    hashingTF = HashingTF(inputCol="filtered", outputCol="tf", numFeatures=numFeatures)

    idf = IDF(inputCol='tf', outputCol='features', minDocFreq=10)
    nb = Model(featuresCol='features', labelCol='label', predictionCol='prediction',
                    smoothing=1.0, modelType="multinomial")
    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, nb])
    return pipeline


def train(train_df, test_df, Model=NaiveBayes, numFreatures=10000, model_path='AMS_Model'):
    pl = pipeline_lm(Model=Model, numFeatures=numFreatures)
    train_df.cache()
    model = pl.fit(train_df)
    model.write().overwrite().save(model_path)
    test_df.cache()
    prediction = model.transform(test_df)

    acc_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                      metricName="accuracy")
    f1_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

    accuracy_score = acc_evaluator.evaluate(prediction)
    f1_score = f1_evaluator.evaluate(prediction)

    print('Accuracy:', accuracy_score)
    print('F1-score:', f1_score)
    return accuracy_score, f1_score


def predict(df, model_path='AMS_Model'):
    df = df.withColumn('review', lower(df["reviewText"]))
    model = PipelineModel.load(model_path)
    prediction = model.transform(df)
    return prediction


if __name__ == '__main__':
    # Convert rating to label
    # data = spark.read.json('hdfs://hoangnam-msi:9000/user/hoangnam/btl/dataset/Arts_Crafts_and_Sewing_5.json')
    # Create Spark session
    print('================================= Create Session ========================================')
    print('================================= Create Session ========================================')
    print('================================= Create Session ========================================')
    logger = open('log_train.txt', 'w')
    start_all = time()
    start_time = time()
    spark = SparkSession.builder \
        .appName('Amazon Review Analytic') \
        .master('spark://hoangnam-msi:7077')\
        .config('spark.executor.memory', '6g')\
        .config('spark.driver.memory', '4g')\
        .config('spark.scheduler.mode', 'FAIR')\
        .getOrCreate()
    # .config("spark.cores.max", "8") \

    logger.write('Time to create session: {}\n'.format(time() - start_time))

    start_time = time()
    print('================================= Load Data ========================================')
    print('================================= Load Data ========================================')
    print('================================= Load Data ========================================')
    # data = spark.read.json('data/review/*')

    data = spark.read.json('hdfs://hoangnam-msi:9000/user/hoangnam/btl/dataset/*')
    
    logger.write('Time to load data: {}\n'.format(time() - start_time))
    print('================================= Load Done ========================================')
    print('================================= Load Done ========================================')
    print('================================= Load Done ========================================')
    start_time = time()
    review = data.select(['reviewerID', 'reviewText', 'vote'])
    review = review.dropna(subset=['reviewText'])
    review = review\
        .withColumn('review', lower(review["reviewText"])) \
        .withColumn('label', when(review["vote"] > 10, 1).when(review["vote"].isNull(), 0).otherwise(None))
    review = review.drop('reviewText')
    review = review.dropna(subset=['label'])
    print(review.count())
    review_1 = review.filter(review['label']==1)
    review_0 = review.filter(review['label']==0)

    n_0 = review_0.count()
    n_s0 = int(0.5*n_0)
    review_0 = review_0.limit(n_s0)
    review = review_1.unionByName(review_0)
    print(review.count())

    train_df, test_df = review.randomSplit(weights=[0.9, 0.1], seed=1227)
    logger.write('Time to create label: {}\n'.format(time() - start_time))
    start_time = time()
    print('Training........................................')
    print('================================= Training ========================================')
    print('================================= Training ========================================')
    print('================================= Training ========================================')

    model_path = 'AMS_Model'
    accuracy_score, f1_score = train(train_df, test_df, Model=NaiveBayes, numFreatures=10000, model_path=model_path)
    print('================================= Training Done ========================================')
    print('================================= Training Done ========================================')
    print('================================= Training Done ========================================')
    logger.write('Time to train: {}\n'.format(time() - start_time))
    logger.write('All time: {}\n'.format(time() - start_all))

    logger.write('accuracy_score: {}\n'.format(accuracy_score))
    logger.write('f1_score: {}\n'.format(f1_score))
    logger.close()

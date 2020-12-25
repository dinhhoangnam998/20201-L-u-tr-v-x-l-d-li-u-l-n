import warnings
from pyspark.sql.functions import when, udf, countDistinct, date_format
from pyspark.sql import SparkSession
import json
warnings.filterwarnings('ignore')
import time

def stat_basic(data):
    basic = {}
    n_review = data.count()
    n_reviewer = data.select(countDistinct('reviewerID')).first()[0]
    n_product = data.select(countDistinct('asin')).first()[0]
    n_ratting = data.select(countDistinct('overall')).first()[0]
    basic['n_review'] = n_review
    basic['n_reviewer'] = n_reviewer
    basic['n_product'] = n_product
    basic['n_ratting'] = n_ratting
    with open('stat_basic.json', 'w') as pf:
        json.dump(basic, pf)


def stat_rating(data):
    rating = {}
    count_rating = data.groupBy('overall').count()
    for r in count_rating.collect():
        rating[r['overall']] = r['count']
    with open('rating.json', 'w') as pf:
        json.dump(rating, pf)


def stat_review_by_user(data):
    review_count = {}
    count_reviewer = data.groupBy('reviewerID').count()
    count_reviewer = count_reviewer.withColumn('scope', when(count_reviewer['count'] <= 5, '5')
                                               .when(count_reviewer['count'] < 10, '10')
                                               .when(count_reviewer['count'] < 15, '15')
                                               .when(count_reviewer['count'] < 20, '20').otherwise('>20'))
    hier_count_reviewer = count_reviewer.groupBy('scope').count()
    for r in hier_count_reviewer.collect():
        review_count[r['scope']] = r['count']
    with open('stat_review_by_user.json', 'w') as pf:
        json.dump(review_count, pf)

def stat_review_for_product(data):
    asin_count = {}
    count_asin = data.groupBy('asin').count()
    count_asin = count_asin.withColumn('scope', when(count_asin['count'] <= 5, '5')
                                               .when(count_asin['count'] < 10, '10')
                                               .when(count_asin['count'] < 15, '15')
                                               .when(count_asin['count'] < 20, '20').otherwise('>20'))
    hier_count_asin = count_asin.groupBy('scope').count()
    for r in hier_count_asin.collect():
        asin_count[r['scope']] = r['count']
    with open('stat_review_for_product.json', 'w') as pf:
        json.dump(asin_count, pf)

def stat_user_product(data):
    user_product = {}
    count = data.groupBy('asin').agg(countDistinct("reviewerID").alias('count'))
    count = count.withColumn('scope', when(count['count'] <= 5, '5')
                                               .when(count['count'] < 10, '10')
                                               .when(count['count'] < 15, '15')
                                               .when(count['count'] < 20, '20').otherwise('>20'))

    hier_count_reviewer = count.groupBy('scope').count()
    for r in hier_count_reviewer.collect():
        user_product[r['scope']] = r['count']
    with open('stat_user_product.json', 'w') as pf:
        json.dump(user_product, pf)

def stat_vote(data):
    data = data.select('vote')
    vote = {}
    n_null = data.filter(data['vote'].isNull()).count()
    vote['0'] = n_null
    vote_df = data.filter(data['vote'].isNotNull())
    vote_df = vote_df.withColumn('scope', when(vote_df['vote'] <= 5, '5')
                                 .when(vote_df['vote'] < 10, '10')
                                 .when(vote_df['vote'] < 20, '20')
                                 .when(vote_df['vote'] < 50, '50')
                                 .when(vote_df['vote'] < 100, '100')
                                 .when(vote_df['vote'] < 200, '200')
                                 .when(vote_df['vote'] < 500, '500')
                                 .when(vote_df['vote'] < 1000, '1000').otherwise('>1000'))
    vote_count = vote_df.groupBy('scope').count()
    for r in vote_count.collect():
        vote[r['scope']] = r['count']
    with open('vote.json', 'w') as pf:
        json.dump(vote, pf)

def stat_vote_rating(data, rating=1.0):
    data = data.filter(data['overall'] == float(rating))
    data = data.select('vote')
    vote = {}
    n_null = data.filter(data['vote'].isNull()).count()
    vote['0'] = n_null
    vote_df = data.filter(data['vote'].isNotNull())
    vote_df = vote_df.withColumn('scope', when(vote_df['vote'] <= 5, '5')
                                 .when(vote_df['vote'] < 10, '10')
                                 .when(vote_df['vote'] < 20, '20')
                                 .when(vote_df['vote'] < 50, '50')
                                 .when(vote_df['vote'] < 100, '100')
                                 .when(vote_df['vote'] < 200, '200')
                                 .when(vote_df['vote'] < 500, '500')
                                 .when(vote_df['vote'] < 1000, '1000').otherwise('>1000'))
    vote_count = vote_df.groupBy('scope').count()
    for r in vote_count.collect():
        vote[r['scope']] = r['count']
    with open('vote_rating_{}.json'.format(rating), 'w') as pf:
        json.dump(vote, pf)

def stat_time(data):
    review_time = data.withColumn("year", date_format(data["unixReviewTime"], "yyyy MM dd"))
    review_time.show()


if __name__ == '__main__':
    # Create Spark session
    log_time = open('log_stat.txt', 'w')
    start_time = time.time()
    start_all = start_time
    spark = SparkSession.builder \
        .appName('Amazon Review Statistic') \
        .getOrCreate()
    #         .master('local') \
    #         .config("spark.executor.memory", "4g") \
    #         .config("spark.driver.memory", "2g") \
    log_time.write('Time to create session: {}\n'.format(time.time() - start_time))

    start_time = time.time()
    data = spark.read.json('data/review/*')
    # data = spark.read.json('hdfs://hoangnam-msi:9000/user/hoangnam/btl/dataset')
    log_time.write('Time to load data: {}\n'.format(time.time() - start_time))
    # stat_time(data)
    start_time = time.time()
    stat_basic(data)
    log_time.write('Time to stat_basic: {}\n'.format(time.time() - start_time))

    start_time = time.time()
    stat_vote(data)
    log_time.write('Time to stat_vote: {}\n'.format(time.time() - start_time))

    start_time = time.time()
    stat_rating(data)
    log_time.write('Time to stat_rating: {}\n'.format(time.time() - start_time))

    start_time = time.time()
    stat_review_by_user(data)
    log_time.write('Time to stat_review_by_user: {}\n'.format(time.time() - start_time))

    start_time = time.time()
    stat_review_for_product(data)
    log_time.write('Time to stat_review_for_product: {}\n'.format(time.time() - start_time))

    start_time = time.time()
    stat_user_product(data)
    log_time.write('Time to stat_user_product: {}\n'.format(time.time() - start_time))

    start_time = time.time()
    stat_vote_rating(data, rating=1.0)
    log_time.write('Time to stat_vote_1.0: {}\n'.format(time.time() - start_time))

    start_time = time.time()
    stat_vote_rating(data, rating=2.0)
    log_time.write('Time to stat_vote_2.0: {}\n'.format(time.time() - start_time))

    start_time = time.time()
    stat_vote_rating(data, rating=3.0)
    log_time.write('Time to stat_vote_3.0: {}\n'.format(time.time() - start_time))

    start_time = time.time()
    stat_vote_rating(data, rating=4.0)
    log_time.write('Time to stat_vote_4.0: {}\n'.format(time.time() - start_time))

    start_time = time.time()
    stat_vote_rating(data, rating=5.0)
    log_time.write('Time to stat_vote_5.0: {}\n'.format(time.time() - start_time))



    log_time.write('All time: {}\n'.format(time.time() - start_all))
    log_time.close()

    # verify_review = data.groupBy('verified').count()
    # print('verified\tcount')
    # for r in verify_review.collect():
    #     print('{}\t{}'.format(r['verified'], r['count']))

    # -------------------------------------------
    # print('-'*50)
    # print('Thống kế số nhận xét được xác thực (verified-True) theo rating')
    # true_rating = data.filter("verified == True")
    # true_rating = true_rating.groupBy('overall').count()
    # print('rating\tcount')
    # for r in true_rating.collect():
    #     print('{}\t{}'.format(r['overall'], r['count']))
    #
    # # -------------------------------------------
    # print('-'*50)
    # print('Thống kế số nhận xét được xác thực (verified-False) theo rating')
    # true_rating = data.filter("verified == False")
    # true_rating = true_rating.groupBy('overall').count()
    # print('rating\tcount')
    # for r in true_rating.collect():
    #     print('{}\t{}'.format(r['overall'], r['count']))

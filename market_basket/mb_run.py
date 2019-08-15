from utility_functions.benchmark import timer
from pyspark.sql.functions import col, lit, countDistinct, rand
from pyspark import StorageLevel
from utility_functions.custom_errors import *
from utility_functions.databricks_uf import rdd_to_df
import numpy
import logging
from connect2Databricks.spark_init import spark_init

if 'spark' not in locals():
    print('Environment: Databricks-Connect')
    spark, sqlContext, setting = spark_init()

module_logger = logging.getLogger('CVM.mb_run')
sc = spark.sparkContext

@timer
def market_basket_proc(data):
    coupons = data.select('coupon').distinct().toPandas()


@timer
def market_basket_sql(data):
    module_logger.info('===== market_basket_sql : START ======')
    total_basket_count = data.agg(countDistinct('basket_key'))
    total_basket_count = int(total_basket_count.toPandas()['count(DISTINCT basket_key)'][0])
    df = data.select('coupon_key', 'basket_key').\
        groupBy('coupon_key').\
        agg(countDistinct('basket_key').alias('basket_count'))

    print(f'Before filtering by basket_count: {df.count()}')

    basket_counts = df.select('basket_count').toPandas()
    basket_threshold = max(numpy.float64(numpy.percentile(basket_counts, 20)).item(), 1)
    df = df.filter(col('basket_count') > lit(basket_threshold))

    print(f'After filtering by basket_count: {df.count()}')

    df1 = data.join(df, ['coupon_key'], how='inner').alias('df1').\
        withColumnRenamed('basket_count', 'basket_count_X').\
        withColumnRenamed('coupon_key', 'coupon_key_X').\
        withColumnRenamed('coupon', 'coupon_X')

    df2 = data.join(df, ['coupon_key'], how='inner').alias('df2'). \
        withColumnRenamed('basket_count', 'basket_count_Y'). \
        withColumnRenamed('coupon_key', 'coupon_key_Y').\
        withColumnRenamed('coupon', 'coupon_Y')

    matrix = df1.join(df2, (df1.coupon_key_X != df2.coupon_key_Y) & (df1.basket_key == df2.basket_key),
                      how = 'inner'). \
        select('coupon_key_X', 'coupon_X', 'coupon_key_Y', 'coupon_Y',
               'basket_count_X', 'basket_count_Y',
               'df1.basket_key').\
        withColumnRenamed('df1.basket_key', 'basket_key').\
        groupBy('coupon_key_X', 'coupon_X', 'coupon_key_Y', 'coupon_Y', 'basket_count_X', 'basket_count_Y'). \
        agg(countDistinct('basket_key').alias('basket_count_XY')). \
        select('coupon_key_X', 'coupon_X', 'coupon_key_Y', 'coupon_Y', 'basket_count_X', 'basket_count_Y',
               'basket_count_XY')

    matrix = matrix.\
        withColumn('confidence', col('basket_count_XY')/col('basket_count_X'))

    matrix = matrix.\
        withColumn('lift', col('confidence')/col('basket_count_Y')*lit(total_basket_count))

    matrix = matrix.\
        withColumn('support', col('basket_count_XY')/lit(total_basket_count))

    conf_check_1 = matrix.filter(col('confidence') > 1.0).count()
    conf_check_2 = matrix.filter(col('confidence') < 0).count()

    if conf_check_1 > 0:
        raise OutputOutOfBoundError(
            f'Confidence should never be larger than 1 but there are {conf_check_1} records >0.'
        )

    if conf_check_2 > 0:
        raise OutputOutOfBoundError(
            f'Confidence should never be negative but there are {conf_check_2} records with negative confidence.'
        )

    matrix = matrix.cache()

    rec_count = matrix.count()

    module_logger.info(f'===== market_basket_sql : total_basket_count = {total_basket_count} ======')
    module_logger.info(f'===== market_basket_sql : total_rec_count = {rec_count} ======')
    module_logger.info('===== market_basket_sql : END ======')
    return total_basket_count, df, matrix


def market_basket_stats(data, matrix):

    # Permute the basket columns
    i = 1
    while i <= 1000:
        rand_basket = data.select('basket_key')

        i += 1

    return df, matrix



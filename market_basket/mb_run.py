from utility_functions.benchmark import timer
from pyspark.sql.functions import col, lit, countDistinct, rand
from utility_functions.custom_errors import *
from utility_functions.databricks_uf import clone
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
def market_basket_sql(data, debug: bool = False, t: int = 20):
    """
    Author: Allison Wu
    Description: this is the core algorithm for market_basket.  It accepts any input table with the same format as
    specified in 'data'.
    :param data: contains the following columns
        'basket_key': int, numeric basket key to specifies the baskets
        'prod_id': str, product id
        'coupon': str, if not product grouping is necessary, please use prod_id for this column.
        'coupon_key': int, internal numeric numbers for coupons for better performance of joining
    :param debug: True or False for debugging mode.  If True, it will print out intermediate steps for debugging.
    :param t: percent threshold for basket_key_count to remove coupons with too few views.
    :return: total_basket_count, df, matrix
    """
    module_logger.info('===== market_basket_sql : START ======')
    total_basket_count = data.agg(countDistinct('basket_key'))
    total_basket_count = int(total_basket_count.toPandas()['count(DISTINCT basket_key)'][0])
    df = data.select('coupon_key', 'basket_key').\
        groupBy('coupon_key').\
        agg(countDistinct('basket_key').alias('basket_count'))

    print(f'Before filtering by basket_count: {df.count()}')

    basket_counts = df.select('basket_count').toPandas()
    basket_threshold = max(numpy.float64(numpy.percentile(basket_counts, t)).item(), 1)
    df = df.filter(col('basket_count') > lit(basket_threshold))
    df = clone(df)

    print(f'After filtering by basket_count: {df.count()}')

    df1 = data.join(df, ['coupon_key'], how='inner').alias('df1').\
        withColumnRenamed('basket_count', 'basket_count_X').\
        withColumnRenamed('coupon_key', 'coupon_key_X').\
        withColumnRenamed('coupon', 'coupon_X')

    if debug:
        print(f'row counts for data: {data.count()}')
        print(f'row counts for df1: {df1.count()}')

    df2 = data.join(df, ['coupon_key'], how='inner').alias('df2'). \
        withColumnRenamed('basket_count', 'basket_count_Y'). \
        withColumnRenamed('coupon_key', 'coupon_key_Y').\
        withColumnRenamed('coupon', 'coupon_Y')

    df1 = clone(df1)
    df2 = clone(df2)

    if debug:
        df1.show()
        df2.show()

    matrix = df1.join(df2, (df1.coupon_key_X != df2.coupon_key_Y) & (df1.basket_key == df2.basket_key),
                      how = 'inner'). \
        drop(df2.basket_key).\
        withColumnRenamed('df1.basket_key', 'basket_key'). \
        select('coupon_key_X', 'coupon_X', 'coupon_key_Y', 'coupon_Y',
               'basket_count_X', 'basket_count_Y',
               'basket_key'). \
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

    matrix = clone(matrix)
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



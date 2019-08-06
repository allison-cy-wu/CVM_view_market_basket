from utility_functions.benchmark import timer
from pyspark.sql.functions import col, lit, countDistinct

@timer
def market_basket_proc(data):
    coupons = data.select('coupon').distinct().toPandas()


@timer
def market_basket_sql(data):
    total_basket_count = data.agg(countDistinct('basket_key'))
    total_basket_count = int(total_basket_count.toPandas()['count(DISTINCT basket_key)'][0])
    df = data.select('coupon_key', 'basket_key').\
        groupBy('coupon_key').\
        agg(countDistinct('basket_key').alias('basket_count'))

    df1 = data.alias('df1').\
        withColumnRenamed('coupon_key', 'coupon_key_X').\
        withColumnRenamed('coupon', 'coupon_X')

    df2 = data.alias('df2').\
        withColumnRenamed('coupon_key', 'coupon_key_Y').\
        withColumnRenamed('coupon', 'coupon_Y')

    matrix = df1.join(df2, (df1.coupon_key_X < df2.coupon_key_Y) & (df1.basket_key == df2.basket_key), how = 'inner'). \
        select('coupon_key_X', 'coupon_X', 'coupon_key_Y', 'coupon_Y', 'df1.basket_key').\
        withColumnRenamed('df1.basket_key', 'basket_key').\
        groupBy('coupon_key_X', 'coupon_X', 'coupon_key_Y', 'coupon_Y'). \
        agg(countDistinct('basket_key').alias('basket_count_XY')). \
        select('coupon_key_X', 'coupon_X', 'coupon_key_Y', 'coupon_Y', 'basket_count_XY')

    matrix = matrix.join(df, matrix.coupon_key_X == df.coupon_key, how = 'left').drop(df.coupon_key).\
        withColumnRenamed('basket_count', 'basket_count_X')

    matrix = matrix.join(df, matrix.coupon_key_Y == df.coupon_key, how = 'left').drop(df.coupon_key). \
        withColumnRenamed('basket_count', 'basket_count_Y')

    matrix = matrix.\
        withColumn('confidence', col('basket_count_XY')/col('basket_count_X'))

    matrix = matrix.\
        withColumn('lift', col('confidence')/col('basket_count_Y'))

    matrix = matrix.\
        withColumn('support', col('basket_count_XY')/lit(total_basket_count))

    return total_basket_count, df, matrix


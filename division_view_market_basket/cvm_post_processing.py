from utility_functions.date_period import date_period, bound_date_check
from utility_functions.benchmark import timer
from connect2Databricks.read2Databricks import redshift_cdw_read, redshift_ccg_read
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col, ltrim, rtrim, coalesce, countDistinct, desc, dense_rank, concat_ws, lit
from pyspark.sql.window import Window
from utility_functions.databricks_uf import rdd_to_df
import pyspark.sql.functions as func
from utility_functions.custom_errors import *
import logging
module_logger = logging.getLogger('CVM.cvm_pre_processing')


class CVMPostProcessing:
    def __init__(self, sales, data, matrix, coup_views,
                 ):
        self.sales = sales
        self.matrix = matrix
        self.data = data
        self.prod_views = data.\
            groupBy('prod_id').\
            agg(countDistinct('basket_key')).\
            withColumnRenamed('count(DISTINCT basket_key)', 'sku_basket_count')
        self.coup_views = coup_views

    def coupon_to_sku(self):
        sku_matrix = self.matrix.\
            join(self.sales, self.matrix.coupon_X == self.sales.coupon, how = 'left').\
            withColumnRenamed('sales', 'sku_sales_X').\
            withColumnRenamed('prod_id', 'sku_X').\
            join(self.sales, self.matrix.coupon_Y == self.sales.coupon, how = 'left'). \
            withColumnRenamed('sales', 'sku_sales_Y').\
            withColumnRenamed('prod_id', 'sku_Y')

        sku_matrix = sku_matrix.\
            join(self.prod_views, self.matrix.sku_X == self.prod_views.prod_id, how = 'left').\
            withColumnRenamed('sku_basket_count', 'sku_basket_count_X').\
            join(self.prod_views, self.matrix.sku_Y == self.prod_views.prod_id, how = 'left'). \
            withColumnRenamed('sku_basket_count', 'sku_basket_count_Y')

        sku_matrix = sku_matrix.\
            withColumn('cvm_score', col('sku_basket_count_Y')*col('confidence'))

        return sku_matrix

    @staticmethod
    def filtering_by_stats(sku_matrix):
        recs = sku_matrix.\
            filter(col('confidence') >= 0.2)
        return recs

    @staticmethod
    def lsg_filtering(recs):
        cvm_rank_window = Window.partitionBy('sku_X').orderBy(desc('cvm_score'), desc('sku_sales_X'))
        recs = recs.\
            withColumn('cvm_rank', dense_rank().over(cvm_rank_window)).\
            filter(col('cvm_rank') <= 35)
        return recs

    @staticmethod
    def ccg_filtering(recs):
        recs = recs
        return recs

    @staticmethod
    def lsg_formatting(recs):
        formatted_recs = recs.select('sku_X', 'sku_Y').\
            withColumn('sku_Y', concat_ws('|', 'sku_Y'))

        return formatted_recs

    @staticmethod
    def ccg_formatting(recs):
        formatted_recs = recs
        return formatted_recs


def cvm_post_processing(
        sales,
        matrix,
        data,
        coupon_views,
        division: str = 'LSG',
):
    cvm_post = CVMPostProcessing(sales = sales, matrix = matrix, data = data, coup_views = coupon_views)

    sku_mb = cvm_post.coupon_to_sku()
    cvm_post.filtering_by_stats(sku_mb)

    if division == 'LSG':
        sku_mb_filtered = cvm_post.lsg_filtering(sku_mb)
        sku_mb_formatted = cvm_post.lsg_formatting(sku_mb_filtered)
    elif division == 'CCG':
        sku_mb_filtered = cvm_post.ccg_filtering(sku_mb)
        sku_mb_formatted = cvm_post.ccg_formatting(sku_mb_filtered)
    else:
        raise InputNotValidError(
            f'Post-processing for Division {division} is not defined.'
        )
        sku_mb_formatted = []

    return sku_mb, sku_mb_filtered, sku_mb_formatted


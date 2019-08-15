from utility_functions.date_period import date_period, bound_date_check
from utility_functions.benchmark import timer
from connect2Databricks.read2Databricks import redshift_cdw_read, redshift_ccg_read
from pyspark.sql.functions import split, explode
from utility_functions.databricks_uf import clear_cache
from pyspark.sql.functions import col, ltrim, rtrim, coalesce, countDistinct, desc, dense_rank, concat_ws, lit, \
    row_number, broadcast
from pyspark.sql.window import Window
from utility_functions.databricks_uf import rdd_to_df
import pyspark.sql.functions as func
from utility_functions.custom_errors import *
from connect2Databricks.spark_init import spark_init
import logging
module_logger = logging.getLogger('CVM.cvm_pre_processing')

if 'spark' not in locals():
    print('Environment: Databricks-Connect')
    spark, sqlContext, setting = spark_init()


class CVMPostProcessing:
    def __init__(self,
                 sales,
                 data,
                 matrix,
                 coupon_views,
                 division:str = 'LSG',
                 debug: bool = True,
                 ):
        clear_cache()
        self.sales = sales
        self.matrix = matrix
        self.data = data
        self.prod_views = data.\
            groupBy('prod_id', 'coupon_key').\
            agg(countDistinct('basket_key')).\
            withColumnRenamed('count(DISTINCT basket_key)', 'sku_basket_count')
        self.coup_views = coupon_views
        self.division = division
        self.debug = debug

    def coupon_to_sku(self):
        # if self.debug:
        #     print(f'matrix row_counts: {self.matrix.count()}')
        sku_matrix = self.matrix. \
            join(self.prod_views, self.matrix.coupon_key_X == self.prod_views.coupon_key, how = 'left'). \
            drop('coupon_key').\
            withColumnRenamed('sku_basket_count', 'sku_basket_count_X'). \
            withColumnRenamed('prod_id', 'sku_X'). \
            fillna({'sku_basket_count_X': 0})

        sku_matrix = sku_matrix.\
            join(self.prod_views, sku_matrix.coupon_key_Y == self.prod_views.coupon_key, how = 'left'). \
            drop('coupon_key'). \
            withColumnRenamed('sku_basket_count', 'sku_basket_count_Y'). \
            withColumnRenamed('prod_id', 'sku_Y'). \
            fillna({'sku_basket_count_Y': 0})

        if self.debug:
            print(f'sku_matrix row_counts: {sku_matrix.count()}')
            sku_matrix.show()
            self.sales.show()

        sku_matrix = sku_matrix.\
            join(self.sales, sku_matrix.sku_X == self.sales.prod_id, how = 'left').\
            drop('coupon', 'prod_id', 'coupon_key').\
            withColumnRenamed('sales', 'sku_sales_X').\
            fillna({'sku_sales_X': 0})

        if self.debug:
            print(f'sku_matrix row_counts: {sku_matrix.count()}')
            sku_matrix.show()

        sku_matrix = spark.createDataFrame(sku_matrix.rdd, sku_matrix.schema)

        sku_matrix = sku_matrix.\
            join(self.sales, sku_matrix.sku_Y == self.sales.prod_id, how = 'left'). \
            drop('coupon', 'prod_id', 'coupon_key').\
            withColumnRenamed('sales', 'sku_sales_Y').\
            fillna({'sku_sales_Y': 0})

        sku_matrix = sku_matrix.\
            withColumn('cvm_score', col('sku_basket_count_Y')*col('confidence'))

        sku_matrix.show()

        if self.debug:
            print(f'sku_matrix row_counts: {sku_matrix.count()}')

        return sku_matrix

    @staticmethod
    def filtering_by_stats(sku_matrix):
        recs = sku_matrix.\
            filter(col('confidence') >= 0.2)
        return recs

    @staticmethod
    def lsg_filtering(recs):
        cvm_rank_window = Window.partitionBy('sku_X').orderBy(desc('cvm_score'), desc('sku_basket_count_Y'), desc(
            'sku_sales_Y'))
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


@timer
def cvm_post_processing(
        sales,
        matrix,
        data,
        coupon_views,
        division: str = 'LSG',
        debug: bool = False,
):
    cvm_post = CVMPostProcessing(sales = sales,
                                 matrix = matrix,
                                 data = data,
                                 coupon_views = coupon_views,
                                 division = division,
                                 debug = debug)

    sku_mb = cvm_post.coupon_to_sku()
    sku_mb = cvm_post.filtering_by_stats(sku_mb)

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

    return sku_mb, sku_mb_filtered, sku_mb_formatted


def cvm_formatting_cdw(
        recs,
        model_master_name,
        model_name: str,
        model_type: str,
        start_date: str,
        time_prd_val: int = 7,
        env: str = 'TST',

):
    model_master = redshift_ccg_read(f'SELECT TOP 1 * FROM {model_master_name} ORDER BY master_id DESC',
                                     database = 'CDWCMMO',
                                     env = env).toPandas()

    master_id = model_master['master_id'][0].item() + 1
    start_date, end_date = date_period(-time_prd_val, start_date)
    w = Window().orderBy('sku_X', 'cvm_rank')
    output = recs. \
        withColumn('master_id', lit(master_id)). \
        withColumn('record_id', row_number().over(w)). \
        withColumn('time_prd_val', lit(time_prd_val)). \
        withColumn('model_type', lit(model_type)). \
        withColumn('model_name', lit(model_name)). \
        withColumn('start_date', lit(start_date)). \
        withColumn('end_date', lit(end_date))

    # Order the columns
    output = output. \
        select('master_id',
               'record_id',
               'time_prd_val',
               'model_type',
               'model_name',
               'sku_X',
               'coupon_X',
               'sku_Y',
               'coupon_Y',
               'basket_count_XY',
               'sku_basket_count_X',
               'sku_basket_count_Y',
               'sku_sales_X',
               'sku_sales_Y',
               'confidence',
               'cvm_rank',
               'start_date',
               'end_date',

               )

    return master_id, output

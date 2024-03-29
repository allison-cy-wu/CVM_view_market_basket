from utility_functions.date_period import date_period, bound_date_check
from utility_functions.benchmark import timer
from connect2Databricks.read2Databricks import oracle_cdw_read
from utility_functions.databricks_uf import clear_cache
from pyspark.sql.functions import col, ltrim, rtrim, coalesce, countDistinct, desc, dense_rank, concat_ws, lit, \
    row_number, broadcast, collect_list
from pyspark.sql.window import Window
from utility_functions.databricks_uf import clone
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
                 coupon_sales,
                 division: str = 'LSG',
                 debug: bool = True,
                 ):
        self.sales = clone(sales.select('prod_id', 'sales').distinct())
        self.matrix = matrix
        self.data = data
        self.prod_views = data.\
            groupBy('prod_id', 'coupon_key').\
            agg(countDistinct('basket_key')).\
            withColumnRenamed('count(DISTINCT basket_key)', 'sku_basket_count')
        self.prod_views = clone(self.prod_views)
        self.coup_views = clone(coupon_views)
        self.coup_sales = clone(coupon_sales.select('coupon_key'))
        self.division = division
        self.debug = debug

    def filtering_by_stats(self):
        module_logger.info('===== cvm_post_processing.filtering_by_stats : START ======')
        matrix_filtered = self.matrix.\
            filter(col('confidence') >= 0.2)
        module_logger.info(f'===== cvm_post_processing.filtering_by_stats : total rec count = {matrix_filtered.count()}======')
        module_logger.info('===== cvm_post_processing.filtering_by_stats : END======')
        return matrix_filtered

    def coupon_to_sku(self, matrix_filtered):
        module_logger.info('===== cvm_post_processing.coupon_to_sku : START ======')
        # if self.debug:
        #     print(f'matrix row_counts: {self.matrix.count()}')

        # remove coupons with no sales
        coup_sales_X = self.coup_sales.alias('coup_sales_X')
        coup_sales_Y = self.coup_sales.alias('coup_sales_Y')

        matrix_filtered = matrix_filtered. \
            join(broadcast(coup_sales_X), matrix_filtered.coupon_key_X == coup_sales_X.coupon_key,
                 how = 'inner').\
            drop('coupon_sales').\
            distinct()
        matrix_filtered = clone(matrix_filtered).cache()
        matrix_filtered = matrix_filtered. \
            join(broadcast(coup_sales_Y), matrix_filtered.coupon_key_Y == coup_sales_Y.coupon_key,
                 how = 'inner'). \
            drop('coupon_sales'). \
            distinct()

        matrix_filtered = clone(matrix_filtered).cache()

        # Append SKU basket counts
        sku_matrix = matrix_filtered. \
            join(broadcast(self.prod_views), matrix_filtered.coupon_key_X == self.prod_views.coupon_key,
                 how = 'left'). \
            drop('coupon_key').\
            withColumnRenamed('sku_basket_count', 'sku_basket_count_X'). \
            withColumnRenamed('prod_id', 'sku_X'). \
            fillna({'sku_basket_count_X': 0})

        sku_matrix = sku_matrix.\
            join(broadcast(self.prod_views), sku_matrix.coupon_key_Y == self.prod_views.coupon_key, how = 'left'). \
            drop('coupon_key'). \
            withColumnRenamed('sku_basket_count', 'sku_basket_count_Y'). \
            withColumnRenamed('prod_id', 'sku_Y'). \
            fillna({'sku_basket_count_Y': 0})

        sku_matrix = clone(sku_matrix).cache()

        if self.debug:
            print(f'sku_matrix row_counts: {sku_matrix.count()}')
            sku_matrix.show()
            self.sales.show()

        # Append SKU sales
        sku_matrix = sku_matrix.\
            join(self.sales, sku_matrix.sku_X == self.sales.prod_id, how = 'left').\
            drop('prod_id').\
            withColumnRenamed('sales', 'sku_sales_X').\
            fillna({'sku_sales_X': 0})

        if self.debug:
            print(f'sku_matrix row_counts: {sku_matrix.count()}')
            sku_matrix.show()

        sku_matrix = sku_matrix.\
            join(self.sales, sku_matrix.sku_Y == self.sales.prod_id, how = 'left'). \
            drop('prod_id'). \
            withColumnRenamed('sales', 'sku_sales_Y').\
            fillna({'sku_sales_Y': 0})

        sku_matrix = clone(sku_matrix)

        sku_matrix = sku_matrix.\
            withColumn('cvm_score', col('sku_basket_count_Y')*col('confidence'))

        if self.debug:
            print(f'sku_matrix row_counts: {sku_matrix.count()}')

        module_logger.info('===== cvm_post_processing.coupon_to_sku : END======')
        return sku_matrix

    @staticmethod
    def lsg_filtering(recs):
        module_logger.info('===== cvm_post_processing.lsg_filtering : START ======')
        cvm_rank_window = Window.partitionBy('sku_X').orderBy(desc('cvm_score'), desc('sku_basket_count_Y'), desc(
            'sku_sales_Y'))
        recs = recs.\
            withColumn('cvm_rank', dense_rank().over(cvm_rank_window)).\
            filter(col('cvm_rank') <= 35)
        module_logger.info('===== cvm_post_processing.lsg_filtering : END ======')
        return recs

    @staticmethod
    def ccg_filtering(recs):
        module_logger.info('===== cvm_post_processing.ccg_filtering : START ======')
        recs = recs
        module_logger.info('===== cvm_post_processing.ccg_filtering : END ======')
        return recs

    @staticmethod
    def lsg_formatting(recs):
        module_logger.info('===== cvm_post_processing.lsg_formatting : START ======')

        formatted_recs = recs.select('sku_X', 'sku_Y').\
            groupby('sku_X').\
            agg(concat_ws('|', collect_list(recs.sku_Y))).\
            withColumnRenamed('concat_ws(|, collect_list(sku_Y))', 'recs')

        module_logger.info('===== cvm_post_processing.lsg_formatting : END ======')
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
        coupon_sales,
        division: str = 'LSG',
        debug: bool = False,
):
    module_logger.info('===== cvm_post_processing : START ======')
    cvm_post = CVMPostProcessing(sales = sales,
                                 matrix = matrix,
                                 data = data,
                                 coupon_views = coupon_views,
                                 coupon_sales = coupon_sales,
                                 division = division,
                                 debug = debug)

    matrix_filtered = cvm_post.filtering_by_stats()
    sku_mb = cvm_post.coupon_to_sku(matrix_filtered)

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
    module_logger.info('===== cvm_post_processing : END ======')
    return sku_mb, sku_mb_filtered, sku_mb_formatted


def cvm_formatting_cdw(
        recs,
        model_master_name,
        model_name: str,
        model_type: str,
        start_date: str,
        time_prd_val: int = -7,
        env: str = 'TST',

):
    model_master = oracle_cdw_read(f'SELECT MASTER_ID FROM {model_master_name} WHERE ROWNUM = 1'
                                   f'ORDER BY master_id DESC',
                                   database = 'CDWCMMO',
                                   env = env,
                                   db_type = 'Oracle').toPandas()
    master_id = model_master['MASTER_ID'][0] + 1
    start_date, end_date = date_period(time_prd_val, start_date)
    w = Window().orderBy('sku_X', 'cvm_rank')
    output = recs. \
        withColumn('MASTER_ID', lit(master_id)). \
        withColumn('RECORD_ID', row_number().over(w)). \
        withColumn('TIME_PRD_VAL', lit(time_prd_val)). \
        withColumn('MODEL_TYPE', lit(model_type)). \
        withColumn('MODEL_NAME', lit(model_name)). \
        withColumn('START_DATE', lit(start_date)). \
        withColumn('END_DATE', lit(end_date))

    # Order the columns
    output = output. \
        select('MASTER_ID',
               'RECORD_ID',
               'TIME_PRD_VAL',
               'MODEL_TYPE',
               'MODEL_NAME',
               'SKU_X',
               'COUPON_X',
               'SKU_Y',
               'COUPON_Y',
               'BASKET_COUNT_XY',
               'BASKET_COUNT_X',
               'BASKET_COUNT_Y',
               'SKU_BASKET_COUNT_X',
               'SKU_BASKET_COUNT_Y',
               'SKU_SALES_X',
               'SKU_SALES_Y',
               'CONFIDENCE',
               'CVM_RANK',
               'START_DATE',
               'END_DATE',

               )

    return master_id, output

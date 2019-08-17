from utility_functions.date_period import date_period, bound_date_check
from utility_functions.benchmark import timer
from utility_functions.databricks_uf import clone
from utility_functions.custom_errors import *
from connect2Databricks.read2Databricks import redshift_cdw_read
from pyspark.sql.functions import split, explode, col, ltrim, rtrim, coalesce, countDistinct, broadcast
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from connect2Databricks.spark_init import spark_init
import logging

module_logger = logging.getLogger('CVM.cvm_pre_processing')

if 'spark' not in locals():
    print('Environment: Databricks-Connect')
    spark, sqlContext, setting = spark_init()


class MarketBasketPullHistory:
    def __init__(self,
                 start_date: str,
                 period: int,
                 env: str,
                 debug: bool = False,
                 ):
        self.start_date = start_date
        self.period = period
        self.env = env
        self.debug = debug

    def __repr__(self):
        return f'Pull history for {self.period} days from the {self.start_date} in {self.env} instance.'

    @timer
    def lsg_omni(self):
        start_date, end_date = date_period(self.period, self.start_date)

        table_name = 'datalake_omni.omni_hit_data'
        dt_col_name = 'hit_time_gmt_dt_key'
        _, bound_end_date = date_period(-1, end_date)
        bound_date_check(table_name, dt_col_name, start_date, bound_end_date, self.env, 'YYYYMMDD', 'LSG')

        query = 'SELECT ' \
                'VS.visit_session_key AS session_key, ' \
                'HIT.post_visid_combined AS visit_id, ' \
                'HIT.visit_return_count AS visit_number, ' \
                'UPPER(TRIM(prod_list)) AS prod_list ' \
                'FROM datalake_omni.omni_hit_data HIT ' \
                'LEFT JOIN CDWDS.D_OMNI_VISIT_SESSION VS ON ' \
                '  VS.VISIT_RETURN_COUNT=HIT.VISIT_RETURN_COUNT AND VS.POST_VISID_COMBINED=HIT.POST_VISID_COMBINED ' \
                f'WHERE HIT.hit_time_gmt_dt_key<{start_date} AND HIT.hit_time_gmt_dt_key>={end_date} ' \
                'AND HIT.post_visid_combined IS NOT NULL ' \
                "AND prod_list IS NOT NULL AND prod_list NOT LIKE '%shipping-handling%'"

        df = redshift_cdw_read(query, db_type = 'RS', database = 'CDWDS', env = self.env). \
            withColumn('prod_id_untrimmed', explode(split('prod_list', ','))). \
            withColumn('prod_id', ltrim(rtrim(col('prod_id_untrimmed')))). \
            drop('prod_id_untrimmed'). \
            drop('prod_list'). \
            distinct()

        if self.debug:
            print(f'row count for df = {df.count()}')

        query = 'SELECT UPPER(sku_nbr) AS prod_id, size_grp AS coupon ' \
                'FROM cdwds.f_web_prod_feature ' \
                'WHERE size_grp IS NOT NULL ' \
                'GROUP BY sku_nbr, size_grp'

        coupons = redshift_cdw_read(query, db_type = 'RS', database = 'CDWDS', env = self.env)

        if coupons.count() == 0:
            raise DataValidityError('No coupon information.  Please check the validity of size_grp column '
                                    'on cdwds.f_web_prod_feature.')

        if self.debug:
            print(f'row count for coupons = {coupons.count()}')

        coupons = coupons.\
            withColumn("coupon_key", func.dense_rank().over(Window.orderBy('coupon')))

        df = df.join(broadcast(coupons), ['prod_id'], how = 'left').\
            withColumn('coupon', coalesce('coupon', 'prod_id')).\
            withColumn("coupon_key", func.dense_rank().over(Window.orderBy('coupon')))

        # TODO: write test to check the integrity of df

        prod_list = df.select('prod_id').distinct()
        coupons = df.select('prod_id', 'coupon', 'coupon_key').distinct()

        return df, prod_list, coupons

    @timer
    def ccg_omni(self):
        start_date = self.start_date
        period = self.period
        env = self.env
        df = []
        prod_list = df.select('prod_id').distinct()
        coupons = []
        return df, prod_list, coupons

    @timer
    def lsg_sales(self, prod_list, coupons):
        start_date, end_date = date_period(self.period, self.start_date)
        # Check bound date
        table_name = 'cdwds.lsg_f_sls_invc'
        dt_col_name = 'invc_dt_key'
        _, bound_end_date = date_period(-1, end_date)
        bound_date_check(table_name, dt_col_name, start_date, bound_end_date, self.env, 'YYYYMMDD', 'LSG')

        query = 'SELECT '\
                'UPPER(prod_prc_ref_sku) AS prod_id, sum(ext_net_sls_pmar_amt) AS sales ' \
                'FROM cdwds.lsg_f_sls_invc ' \
                f'WHERE invc_dt_key<{start_date} AND invc_dt_key>={end_date} ' \
                'AND UPPER(prod_prc_ref_sku) IS NOT NULL ' \
                f'GROUP BY UPPER(prod_prc_ref_sku)'

        sales = redshift_cdw_read(query, db_type = 'RS', database = 'CDWDS', env = self.env)
        if prod_list:
            print(f'There are {prod_list.count()} products.')
            sales = sales.\
                join(broadcast(prod_list), ['prod_id'], how='inner')
        else:
            print('Product list is not defined for pulling sales.')

        if coupons:
            print(f'There are {coupons.count()} rows in coupons table.')
            sales = sales. \
                join(broadcast(coupons), ['prod_id'], how = 'left'). \
                withColumn('coupon', coalesce('coupon', 'prod_id'))
        else:
            print('Coupons is not defined for pulling sales.')

        print(f'sales count: {sales.count()}')
        return sales

    @timer
    def ccg_sales(self, prod_list, coupons):
        start_date, end_date = date_period(self.period, self.start_date)
        # Check bound date
        table_name = 'cdwds.lsg_f_sls_invc'
        dt_col_name = 'invc_dt_key'
        _, bound_end_date = date_period(-1, end_date)
        bound_date_check(table_name, dt_col_name, start_date, bound_end_date, self.env, 'YYYYMMDD', 'LSG')

        query = 'SELECT '\
                'UPPER(prod_prc_ref_sku) AS prod_id, sum(ext_net_sls_pmar_amt) AS sales ' \
                'FROM cdwds.lsg_f_sls_invc ' \
                f'WHERE invc_dt_key<{start_date} AND invc_dt_key>={end_date} ' \
                f'and prod_prc_ref_sku IS NOT NULL ' \
                f'GROUP BY UPPER(prod_prc_ref_sku)'

        sales = redshift_cdw_read(query, db_type = 'RS', database = 'CDWDS', env = self.env)
        if prod_list:
            print(f'There are {prod_list.count()} products.')
            sales = sales.\
                join(broadcast(prod_list), ['prod_id'], how='inner')
        else:
            print('Product list is not defined for pulling sales.')

        if coupons:
            print(f'There are {coupons.count()} rows in coupons table.')
            sales = sales. \
                join(broadcast(coupons), ['prod_id'], how = 'left'). \
                withColumn('coupon', coalesce('coupon', 'prod_id'))
        else:
            print('Coupons is not defined for pulling sales.')

        print(f'sales count: {sales.count()}')
        return sales


@timer
def cvm_pre_processing(
        start_date: str,
        period: int,
        env: str,
        division: str = 'LSG',
        debug: bool = False,
):
    module_logger.info(f'===== cvm_pre_processing for {division} in {env} : START ======')
    module_logger.info(f'===== cvm_pre_processing : start_date = {start_date} ======')
    module_logger.info(f'===== cvm_pre_processing : period = {period} days ======')
    pull_history = MarketBasketPullHistory(start_date, period, env, debug = debug)
    if division == 'LSG':
        df, prod_list, coupons = pull_history.lsg_omni()
        sales = pull_history.lsg_sales(prod_list, coupons)
    else:
        df, prod_list, coupons = pull_history.ccg_omni()
        sales = pull_history.ccg_sales(prod_list, coupons)

    # find scraper sessions: sessions with more than 30 clicks
    if df:
        session_prod_counts = df.groupBy('session_key').\
            agg(countDistinct(col('prod_id'))).\
            withColumnRenamed('count(DISTINCT prod_id)', 'prod_count')

        session_coup_counts = df.groupBy('session_key').\
            agg(countDistinct(col('coupon'))).\
            withColumnRenamed('count(DISTINCT coupon)', 'coupon_count')

        sessions_that_matter = session_prod_counts.\
            join(session_coup_counts, ['session_key'], how='inner')

        sessions_that_matter = sessions_that_matter. \
            filter(col('coupon_count') <= 30). \
            filter(col('coupon_count') > 1). \
            filter(col('prod_count') > 1)

    sessions_that_matter = clone(sessions_that_matter)
    df = df.join(broadcast(sessions_that_matter), ['session_key'], how = 'inner').\
        withColumnRenamed('session_key', 'basket_key')

    module_logger.info('===== cvm_pre_processing : caching the pre-processed data ======')

    # df = collect_and_cache(df)
    # sales = collect_and_cache(sales)

    sales_count = sales.count()
    row_count = df.count()

    module_logger.info(f'===== cvm_pre_processing : total_row_count for df = {row_count} ======')
    module_logger.info(f'===== cvm_pre_processing : number of SKUs with sales = {sales_count} ======')
    module_logger.info('===== cvm_pre_processing : END ======')
    return sessions_that_matter, sales, df



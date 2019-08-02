from utility_functions.date_period import date_period, bound_date_check
from utility_functions.benchmark import timer
from connect2Databricks.read2Databricks import redshift_cdw_read
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col, ltrim, rtrim, coalesce, countDistinct
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from utility_functions.custom_errors import *
import logging
module_logger = logging.getLogger('CVM.cvm_pre_processing')


class PullOmniHistory:
    def __init__(self,
                 start_date: str,
                 period: int,
                 env: str = 'TST',
                 ):
        self.start_date = start_date
        self.period = period
        self.env = env

    def __repr__(self):
        return f'Pull Omni history for {self.period} days from the {self.start_date} in {self.env} instance.'

    @timer
    def lsg_omni(self):
        start_date, end_date = date_period(self.period, self.start_date)

        table_name = 'datalake_omni.omni_hit_data'
        dt_col_name = 'hit_time_gmt_dt_key'
        bound_date_check(table_name, dt_col_name, start_date, str(int(end_date) - 1), self.env, 'YYYYMMDD', 'LSG')

        query = 'SELECT ' \
                'VS.visit_session_key AS session_key, ' \
                'HIT.post_visid_combined AS visit_id, ' \
                'HIT.visit_return_count AS visit_number, ' \
                'UPPER(TRIM(prod_list)) AS prod_list ' \
                'FROM datalake_omni.omni_hit_data HIT ' \
                'LEFT JOIN CDWDS.D_OMNI_VISIT_SESSION VS ON ' \
                '  VS.VISIT_RETURN_COUNT=HIT.VISIT_RETURN_COUNT AND VS.POST_VISID_COMBINED=HIT.POST_VISID_COMBINED ' \
            f'WHERE HIT.hit_time_gmt_dt_key<{start_date} AND HIT.hit_time_gmt_dt_key>={end_date} ' \
                "AND prod_list IS NOT NULL AND prod_list NOT LIKE '%shipping-handling%'"

        df = redshift_cdw_read(query, db_type = 'RS', database = 'CDWDS', env = self.env). \
            withColumn('prod_id_untrimmed', explode(split('prod_list', ','))). \
            withColumn('prod_id', ltrim(rtrim(col('prod_id_untrimmed')))). \
            drop('prod_id_untrimmed'). \
            drop('prod_list'). \
            distinct()

        query = 'SELECT UPPER(sku_nbr) AS prod_id, size_grp AS coupon ' \
                'FROM cdwds.f_web_prod_feature ' \
                'GROUP BY sku_nbr, size_grp'

        coupons = redshift_cdw_read(query, db_type = 'RS', database = 'CDWDS', env = self.env)

        # if coupons.filter(col('coupon').isNotNull()).count() == 0:
        #     raise DataValidityError('No coupon information.  Check the validity of f_web_prod_feature.')

        df = df.join(coupons, ['prod_id'], how = 'left').withColumn('coupon', coalesce('coupon', 'prod_id'))

        df = df. \
            withColumn("coupon_key", func.dense_rank().over(Window.orderBy('coupon')))

        # TODO: write test to check the integrity of df

        return df

    @timer
    def ccg_omni(self):
        start_date = self.start_date
        period = self.period
        env = self.env
        df = []
        return df


@timer
def cvm_pre_processing(
        start_date: str,
        period: int,
        env: str = 'TST',
        division: str = 'LSG',
):
    module_logger.info('===== cvm_pre_processing : START ======')
    pull_history = PullOmniHistory(start_date, period, env)
    if division == 'LSG':
        df = pull_history.lsg_omni()
    else:
        df = pull_history.ccg_omni()

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

    df = df.join(sessions_that_matter, ['session_key'], how = 'inner')
    module_logger.info('===== cvm_pre_processing : END ======')
    return sessions_that_matter, df



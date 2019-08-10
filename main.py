from division_view_market_basket.logger_config import configure_logger
from connect2Databricks.update_model_master import update_model_master
from division_view_market_basket.cvm_pre_processing import cvm_pre_processing
from division_view_market_basket.cvm_post_processing import cvm_post_processing
from market_basket.mb_run import market_basket_sql
import logging
import pickle
from connect2Databricks.spark_init import spark_init
if 'spark' not in locals():
    print('Environment: Databricks-Connect')
    spark, sqlContext, setting = spark_init()

# create logger
logger = logging.getLogger('CVM')
logger = configure_logger(logger)

start_date = '20190710'


def cvm_pipeline(
        table_name:str,
        start_date: str,
        period: int = -1,
        division: str = 'LSG',
        env: str = 'TST',
):
    logger.info('===== cvm_pipeline : START ======')
    _, sales, df = cvm_pre_processing(
        start_date = start_date,
        period = period,
        env = env,
        division = division
    )

    total_basket_count, coupon_views, matrix = market_basket_sql(df)
    # with open('post_processing_test_data.pkl', 'wb') as f:
    #     pickle.dump([df, sales, matrix, coupon_views, total_basket_count], f)

    sku_mb, sku_mb_filtered, sku_mb_formatted = cvm_post_processing(
        sales = sales,
        matrix = matrix,
        data = df,
        coupon_views = coupon_views,
        division = division
    )

    sku_mb.show()
    sku_mb_filtered()
    sku_mb_formatted.show()

    # # update model master
    # update_model_master(
    #     recs = recs,
    #     master_id = master_id,
    #     model_master_name = 'cdwcmmo.model_master',
    #     model_type = 'CVM',
    #     time_prd_val = 7,
    #     env = env,
    #     division = 'LSG',
    #     modelsubtype = m,
    #     tablename = table_name.split('.')[1],
    #     train_period = 90,
    #     predict_period = 30,
    #     start_predict_date = start_date,
    # )

    # Write tables to CDWCMMO

    # Write to S3 Archive

    # write formatted outputs to S3 buckets
    logger.info('===== cvm_pipeline : END ======')


if __name__ == '__main__':
    logger.info('==== CVM : START ====')
    cvm_pipeline(env = 'TST',
                 table_name = 'cdwcmmo.cvm_databricks_test',
                 start_date = start_date,
                 division = 'LSG', )
    logger.info('==== CVM : END ====')

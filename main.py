from division_view_market_basket.logger_config import configure_logger
from connect2Databricks.update_model_master import update_model_master
from connect2Databricks.write2Databricks import oracle_cdw_write
from connect2Databricks.write2S3 import s3_tfs_bucket_write
from division_view_market_basket.cvm_pre_processing import cvm_pre_processing
from division_view_market_basket.cvm_post_processing import cvm_post_processing,  cvm_formatting_cdw
from connect2Databricks.read2Databricks import oracle_cdw_read
from connect2Databricks.write2S3 import s3_lsg_archive_write
from utility_functions.date_period import date_period
from market_basket.mb_run import market_basket_sql
import logging
from connect2Databricks.spark_init import spark_init
if 'spark' not in locals():
    print('Environment: Databricks-Connect')
    spark, sqlContext, setting = spark_init()

sc = spark.sparkContext

# create logger
logger = logging.getLogger('CVM')
logger = configure_logger(logger)


def cvm_pipeline(
        table_name: str,
        start_date: str,
        period: int = -1,
        division: str = 'LSG',
        env: str = 'TST',
        debug: bool = False,
):
    write_env = 'TST'
    logger.info('===== cvm_pipeline : START ======')
    start_date, end_date = date_period(period, start_date)
    _, sales, coupon_sales, df = cvm_pre_processing(
        start_date = start_date,
        period = period,
        env = env,
        division = division,
        debug = debug,
    )

    total_basket_count, coupon_views, matrix = market_basket_sql(df, debug)

    sku_mb, sku_mb_filtered, sku_mb_formatted = cvm_post_processing(
        sales = sales,
        matrix = matrix,
        data = df,
        coupon_views = coupon_views,
        coupon_sales = coupon_sales,
        division = division,
        debug = debug,
    )

    # sku_mb_filtered = sc.pickleFile('view_mb_sku_mb_filtered.pkl').toDF()
    # sku_mb_formatted = sc.pickleFile('view_mb_sku_mb_formatted.pkl').toDF()

    master_id, sku_mb_filtered = cvm_formatting_cdw(sku_mb_filtered,
                                                    model_name = 'CVM_v1.0',
                                                    model_type = 'CVM',
                                                    model_master_name = 'cdwcmmo.model_master',
                                                    start_date = start_date,
                                                    time_prd_val = period,
                                                    env = write_env,
                                                    )
    # # update model master
    update_model_master(
        recs = sku_mb_filtered,
        master_id = master_id,
        model_master_name = 'cdwcmmo.model_master',
        model_type = 'CVM',
        time_prd_val = period,
        env = write_env,
        division = 'LSG',
        modelsubtype = 'CVM_v1.0',
        tablename = table_name.split('.')[1],
        train_period = 180,
        predict_period = 14,
        start_predict_date = start_date,
    )

    # Check if table with same name exists. If exists, archive the table first.
    # Copy the old table to archive
    try:
        old_recs = oracle_cdw_read(
            table_name,
            database = 'cdwcmmo',
            env = write_env,
            db_type = 'Oracle',
        )
        last_master_id = old_recs.agg({'master_id': 'max'}).toPandas()['max(master_id)'][0]
        s3_lsg_archive_write(old_recs, 'market_basket_cvm', f'master_id={last_master_id}')
    except Exception as e:
        print(Exception)

    # Write tables to CDWCMMO
    oracle_cdw_write(
        sku_mb_filtered,
        env = write_env,
        table = table_name,
        schema = None,
        write_mode = 'overwrite',
    )

    # write formatted outputs to S3 buckets
    file_name = f'CSV_Pipes_{start_date}.csv'
    s3_tfs_bucket_write(sku_mb_formatted, file_name, env = 'QA')
    s3_tfs_bucket_write(sku_mb_formatted, file_name, env = 'DEV')

    if write_env == 'PRD':
        s3_tfs_bucket_write(sku_mb_formatted, file_name, env = 'PRD')

    logger.info('===== cvm_pipeline : END ======')


if __name__ == '__main__':
    logger.info('==== CVM : START ====')
    cvm_pipeline(env = 'PRD',
                 table_name = 'cdwcmmo.cvm_databricks_test',
                 start_date = '',
                 period = -1,
                 division = 'LSG',
                 debug = True,)
    logger.info('==== CVM : END ====')

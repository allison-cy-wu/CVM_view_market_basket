from lsg_view_market_basket.logger_config import configure_logger
from connect2Databricks.update_model_master import update_model_master
import logging

# create logger
logger = logging.getLogger('CVM')
logger = configure_logger(logger)

start_date = '20190710'


def cvm_pipeline(
        table_name:str,
        start_date:str,
        env: str = 'TST',
):
    data = cvm_pre_processing(start_date, env)
    output = run_market_basket(data)
    master_id,  recs, formatted_recs = cvm_post_processing(output)

    # update model master
    update_model_master(
        recs = recs,
        master_id = master_id,
        model_master_name = 'cdwdsmo.model_master',
        model_type = 'CVM',
        time_prd_val = 7,
        env = env,
        division = 'LSG',
        modelsubtype = m,
        tablename = table_name.split('.')[1],
        train_period = 90,
        predict_period = 30,
        start_predict_date = start_date,
    )

    # write tables to S3 buckets


if __name__ == '__main__':
    logger.info('==== CVM : START ====')
    cvm_pipeline(env = 'TST',
                 table_name = 'ccgdsmo.cvm_databricks_test',
                 start_date = start_date)
    logger.info('==== CVM : END ====')

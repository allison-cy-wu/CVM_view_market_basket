from market_basket.mb_run import market_basket_sql
from utility_functions.stats_functions import permute_columns
import numpy as np


def mb_permutation_test(
        data,
        t: int = 20,
        sig_level: float = 0.05,
        perm_times: int = 1,
):
    metrics_null_array = []

    for i in range(perm_times):
        perm_data = permute_columns(data,
                                    columns_to_permute = ['prod_id', 'coupon_key', 'coupon'],
                                    column_to_order = 'basket_key',
                                    ind_permute = False)

        perm_data = perm_data. \
            select('basket_key', 'rand_prod_id', 'rand_coupon_key', 'rand_coupon'). \
            withColumnRenamed('rand_prod_id', 'prod_id'). \
            withColumnRenamed('rand_coupon_key', 'coupon_key'). \
            withColumnRenamed('rand_coupon', 'coupon').cache()

        _, _, perm_matrix = market_basket_sql(perm_data, t = t)
        metrics_null_array = metrics_null_array + [row.confidence for row in perm_matrix.select('confidence').collect()]

    sig_threshold = np.percentile(metrics_null_array, 100 - sig_level * 100)

    return metrics_null_array, sig_threshold

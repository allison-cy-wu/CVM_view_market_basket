from unittest import TestCase
from utility_functions.databricks_uf import rdd_to_df
from market_basket.mb_run import market_basket_sql
from division_view_market_basket.cvm_pre_processing import cvm_pre_processing
import pickle


class TestMarketBasketSql(TestCase):
    def test_market_basket_sql(self):
        start_date = ''
        period = -30
        env = 'TST'
        _, data = cvm_pre_processing(start_date, period, env)
        test_data = data.take(5000)
        with open('mb_run_testData.pkl', 'wb') as f:
            pickle.dump(test_data, f)

        with open('mb_run_testData.pkl', 'rb') as f:
            data = pickle.load(f)

        data = rdd_to_df(data)
        data.show(10)

        total_basket_count, df, matrix = market_basket_sql(data)
        matrix.show(10)
        df.show(10)
        print(total_basket_count)

        self.assertEqual(0, 0)


if __name__ == '__main__':
    test_CVM.main()



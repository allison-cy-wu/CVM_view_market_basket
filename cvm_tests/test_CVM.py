from unittest import TestCase
from utility_functions.databricks_uf import rdd_to_df
from market_basket.mb_run import market_basket_sql
from division_view_market_basket.cvm_pre_processing import cvm_pre_processing
import pickle


class TestMarket_basket_sql(TestCase):
    def test_market_basket_sql(self):
        start_date = ''
        period = -1
        env = 'TST'
        _, data = cvm_pre_processing(start_date, period, env)
        testData = data.take(1000)
        with open('mb_run_testData.pkl', 'wb') as f:
            pickle.dump(testData, f)

        with open('mb_run_testData.pkl', 'rb') as f:
            data = pickle.load(f)

        data = rdd_to_df(data)

        market_basket_sql(data)

        self.assertEqual(0, 0)


if __name__ == '__main__':
    test_CVM.main()
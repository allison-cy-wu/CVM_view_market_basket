from unittest import TestCase
from utility_functions.databricks_uf import rdd_to_df
from market_basket.mb_run import market_basket_sql, market_basket_stats
from division_view_market_basket.cvm_pre_processing import MarketBasketPullHistory
from pyspark.sql.functions import col, ltrim, rtrim, desc, countDistinct
import pickle


class TestMarketBasketSql(TestCase):

    def setUp(self):
        with open('mb_run_testData.pkl', 'rb') as f:
            self.data = pickle.load(f)

        self.data = rdd_to_df(self.data)
        self.data.show()

        self.total_basket_count, self.df, self.matrix = market_basket_sql(self.data)
        self.matrix.orderBy(desc('basket_count_XY'), desc('coupon_key_X'), desc('coupon_key_Y')).show()

    def test_basket_count(self):
        baskets_1 = self.data.filter(col('coupon_key') == 56533).select('basket_key').distinct()
        baskets_2 = self.data.filter(col('coupon_key') == 56527).select('basket_key').distinct()
        basket_check_1 = baskets_1.count()
        basket_check_2 = baskets_2.count()
        basket_check_3 = baskets_1.join(baskets_2, ['basket_key'], how='inner').count()

        self.assertEqual(basket_check_1, 27)
        self.assertEqual(basket_check_2, 14)
        self.assertEqual(basket_check_3, 12)

    def test_conf(self):
        conf_check_1 = self.matrix.filter(col('confidence') > 1.0).count()
        conf_check_2 = self.matrix.filter(col('confidence') < 0).count()
        self.assertEqual(conf_check_1, 0)
        self.assertEqual(conf_check_2, 0)


class TestMarketBasketStats(TestCase):
    def setUp(self):
        with open('mb_stats_testData.pkl', 'rb') as f:
            self.matrix, self.df, self.total_basket_count = pickle.load(f)

        with open('mb_run_testData.pkl', 'rb') as f:
            self.data = pickle.load(f)

        self.matrix_with_stats = market_basket_stats(self.data, self.matrix)

    # with open('mb_stats_testData.pkl', 'wb') as f:
    #     pickle.dump([self.matrix, self.df, self.total_basket_count], f)

    def test_market_basket_stats(self):
        self.matrix_with_stats.show()

        self.assertEqual(0, 0)


class TestMarketBasketPullHistory(TestCase):
    def setUp(self):
        self.start_date = '20190710'
        self.period = -1
        self.env = 'PRD'
        self.pull_history = MarketBasketPullHistory(self.start_date, self.period, self.env)

    def test_lsg_omni(self):
        df = self.pull_history()
        df.show()
        self.assertEqual(0,0)

    def test_lsg_sales(self):
        sales = self.pull_history.lsg_sales()
        # sales.orderBy(desc('sales')).show()
        sales_check_1 = sales.filter(col('prod_id') == 'RA2B2').select('sales').toPandas()['sales'][0]
        sales_check_2 = sales.count()

        self.assertEqual(sales_check_1, 638580)
        self.assertEqual(sales_check_2, 7697)


if __name__ == '__main__':
    test_CVM.main()



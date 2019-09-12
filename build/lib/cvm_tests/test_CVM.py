from unittest import TestCase
import unittest
from utility_functions.databricks_uf import rdd_to_df
from market_basket.mb_run import market_basket_sql, market_basket_stats
from division_view_market_basket.cvm_pre_processing import MarketBasketPullHistory
from division_view_market_basket.cvm_post_processing import CVMPostProcessing
from decimal import *
from pyspark.sql.functions import col, ltrim, rtrim, desc, countDistinct
from connect2Databricks.spark_init import spark_init
import pickle


class TestMarketBasketSql(TestCase):

    def setUp(self):
        with open('mb_run_testData.pkl', 'rb') as f:
            self.data = pickle.load(f)

        self.data = rdd_to_df(self.data)
        self.data.show()
        self.total_basket_count, self.df, self.matrix = market_basket_sql(self.data, debug = True)
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
        with open('prod_list_test.pkl', 'rb') as f:
            [self.prod_list, self.coupons] = pickle.load(f)

        self.prod_list = rdd_to_df(self.prod_list)
        self.coupons = rdd_to_df(self.coupons)
        self.pull_history = MarketBasketPullHistory(self.start_date, self.period, self.env)

    def test_lsg_omni(self):
        df, _, _ = self.pull_history.lsg_omni()
        count_check = df.count()
        self.assertLess(150000, count_check)

    def test_lsg_sales(self):
        sales, coupon_sales = self.pull_history.lsg_sales(self.prod_list, self.coupons)
        # sales.orderBy(desc('sales')).show()
        sales_check_1 = sales.filter(col('prod_id') == '292805').select('sales').toPandas()['sales'][0]
        sales_check_2 = sales.count()
        coupon_sales_check_1 = coupon_sales.filter(col('coupon_sales') == 0).count()

        self.assertEqual(Decimal('476829.60'), sales_check_1)
        self.assertEqual(6097, sales_check_2)
        self.assertEqual(0, coupon_sales_check_1)


class TestCVMPostProcessing(TestCase):
    def setUp(self):
        if 'spark' not in locals():
            print('Environment: Databricks-Connect')
            spark, sqlContext, setting = spark_init()

        sc = spark.sparkContext

        self.start_date = '20190710'
        self.period = -1
        self.env = 'TST'
        self.df = sc.pickleFile('post_processing_test_data_df.pkl').toDF()
        self.sales = sc.pickleFile('post_processing_test_data_sales.pkl').toDF()
        self.total_basket_count, self.coupon_views, self.matrix = market_basket_sql(self.df)

        self.cvm_post = CVMPostProcessing(
            sales = self.sales,
            matrix = self.matrix,
            data = self.df,
            coupon_views = self.coupon_views,
            division = 'LSG')

    def test_coupon_to_sku(self):
        # self.sales.show()
        # self.matrix.show()
        # self.df.show()
        # self.coupon_views.show()
        sku_mb = self.cvm_post.coupon_to_sku()
        sku_mb.show()
        self.assertEqual(0, 0)

    def test_filtering_by_stats(self):
        sku_mb = self.cvm_post.coupon_to_sku()
        sku_mb_filtered = self.cvm_post.filtering_by_stats(sku_mb)
        sku_mb_filtered.show()
        self.assertEqual(0, 0)

    def test_lsg_filtering(self):
        sku_mb = self.cvm_post.coupon_to_sku()
        sku_mb_filtered = self.cvm_post.filtering_by_stats(sku_mb)
        sku_mb_filtered = self.cvm_post.lsg_filtering(sku_mb_filtered)
        max_check = sku_mb_filtered.agg({'cvm_rank': 'max'}).collect()[0]['max(cvm_rank)']
        self.assertLessEqual(max_check, 35)

    def test_ccg_filtering(self):
        self.assertEqual(0, 0)

    def test_lsg_formatting(self):
        sku_mb = self.cvm_post.coupon_to_sku()
        sku_mb_filtered = self.cvm_post.filtering_by_stats(sku_mb)
        sku_mb_filtered = self.cvm_post.lsg_filtering(sku_mb_filtered)
        sku_mb_formatted = self.cvm_post.lsg_formatting(sku_mb_filtered)
        sku_mb_formatted.show()
        count_check = sku_mb_formatted.groupby('sku_X').count().agg({'count': 'max'}).collect()[0]['max(count)']
        self.assertLessEqual(count_check, 1)

    def test_ccg_formatting(self):
        self.fail()


if __name__ == '__main__':
    unittest.main()



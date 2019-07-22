from utility_functions.benchmark import timer

@timer
def market_basket_proc(data):
    coupons = data.select('coupon').distinct().toPandas()
    mb_matrix =
    for m in coupons:
        for n in coupons:
            if m != n:



def market_basket_sql(data):
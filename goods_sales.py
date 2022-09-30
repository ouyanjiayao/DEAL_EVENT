from biz.goods_sales_total import *
import time
import threading
import time

class GetGoodsSale(threading.Thread):

    def __init__(self):
        self.get_goods_sales = GetGoodsSales()
        threading.Thread.__init__(self)

    def run(self):
        self.get_goods_sales.execute()
        

print('start get　goods sales')
get_goods_sale = GetGoodsSale()
get_goods_sale.start()

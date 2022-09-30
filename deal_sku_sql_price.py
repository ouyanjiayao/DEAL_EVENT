from dsku.deal_sku_sql import *
from libs.youzan import *
import threading
import sys

class SkuPriceThread(threading.Thread):

    def __init__(self):
        self.deal_pri = DealSku()
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                self.deal_pri.deal_price(100)
            except Exception as e:
                syn_logger.exception(e)
            time.sleep(10)

sku_price_thread = SkuPriceThread()
sku_price_thread.start()
print('start deal sku price')


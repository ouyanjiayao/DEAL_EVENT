from dsku.deal_sku_sql import *
from libs.youzan import *
import threading
import sys

class SkuDetailThread(threading.Thread):

    def __init__(self):
        self.deal_det = DealSku()
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                self.deal_det.deal_detail(100)
            except Exception as e:
                syn_logger.exception(e)
            time.sleep(10)


sku_detail_thread = SkuDetailThread()
sku_detail_thread.start()
print('start deal sku detail')


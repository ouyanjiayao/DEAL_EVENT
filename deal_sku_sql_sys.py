from dsku.deal_sku_sql import *
from libs.youzan import *
import threading
import sys

class SkuSystemThread(threading.Thread):

    def __init__(self):
        self.deal_sys = DealSku()
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                self.deal_sys.deal_system(100)
            except Exception as e:
                syn_logger.exception(e)
            time.sleep(10)


sku_sys_thread = SkuSystemThread()
sku_sys_thread.start()
print('start deal sku system')

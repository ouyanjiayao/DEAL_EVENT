from dsku.deal_sku_syn import *
from libs.youzan import *
import threading

class SkuThread():

    def __init__(self):
        self.sku_syn = SkuSyn()

    def run(self):
        try:
            self.sku_syn.execute_to(1000)
        except Exception as e:
            syn_logger.exception(e)

print('start deal sku')
sku_thread = SkuThread()
sku_thread.run()
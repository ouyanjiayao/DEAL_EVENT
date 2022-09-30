from biz.order_detail_excel import *
import threading

class OrderGoodsDetailThread():
    def __init__(self):
        self.detail_excel = GoodsDetail()
        threading.Thread.__init__(self)
    def runTotal(self):
        try:
            type ='total'
            self.detail_excel.execute_to(type)
        except Exception as e:
            syn_logger.exception(e)

    def runMorning(self):
        try:
            type ='morning'
            self.detail_excel.execute_to(type)
        except Exception as e:
            syn_logger.exception(e)

    def runAfternoon(self):
        try:
            type ='afternoon'
            self.detail_excel.execute_to(type)
        except Exception as e:
            syn_logger.exception(e)

goods_detail_thread = GoodsDetailThread()
goods_detail_thread.runTotal()
goods_detail_thread.runMorning()
goods_detail_thread.runAfternoon()
print('start goods_detail to excel')
from biz.order_detail_excel import *
import threading

class OrderGoodsDetailThread():
    def __init__(self):
        self.order_detail_excel = OrderGoodsDetail()
        threading.Thread.__init__(self)
    def runTotal(self):
        try:
            type ='total'
            self.order_detail_excel.execute_to(type)
        except Exception as e:
            syn_logger.exception(e)

    def runMorning(self):
        try:
            type ='mon'
            self.order_detail_excel.execute_to(type)
        except Exception as e:
            syn_logger.exception(e)

    def runAfternoon(self):
        try:
            type ='noon'
            self.order_detail_excel.execute_to(type)
        except Exception as e:
            syn_logger.exception(e)

order_goods_detail_thread = OrderGoodsDetailThread()
order_goods_detail_thread.runMorning()
print('start order_goods_detail to excel')
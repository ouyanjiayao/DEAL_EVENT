from biz.order_detail_excel_diff import *
import threading

class OrderGoodsDetailNextThread():
    def __init__(self):
        self.order_detail_excel_next = OrderGoodsDetail()
        threading.Thread.__init__(self)
    def runTotal(self):
        try:
            type ='total'
            self.order_detail_excel_next.execute_to(type)
        except Exception as e:
            syn_logger.exception(e)

    def runMorning(self):
        try:
            type ='morning'
            self.order_detail_excel_next.execute_to(type)
        except Exception as e:
            syn_logger.exception(e)

    def runAfternoon(self):
        try:
            type ='afternoon'
            self.order_detail_excel_next.execute_to(type)
        except Exception as e:
            syn_logger.exception(e)

order_goods_detail_thread = OrderGoodsDetailNextThread()
order_goods_detail_thread.runTotal()
order_goods_detail_thread.runMorning()
order_goods_detail_thread.runAfternoon()
print('start order_goods_detail to excel')
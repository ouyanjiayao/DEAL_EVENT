from biz.order_delivery import *
import threading

class OrderDeliveryThread():
    def __init__(self):
        self.order_delivery = DealOrder()
        threading.Thread.__init__(self)
    def runTotal(self):
        try:
            type ='total'
            self.order_delivery.execute_to(type)
        except Exception as e:
            syn_logger.exception(e)

    def runMorning(self):
        try:
            type ='morning'
            self.order_delivery.execute_to(type)
        except Exception as e:
            syn_logger.exception(e)

    def runAfternoon(self):
        try:
            type ='noon'
            self.order_delivery.execute_to(type)
        except Exception as e:
            syn_logger.exception(e)

print('start order_goods_detail to excel')
order_delivery_thread = OrderDeliveryThread()
order_delivery_thread.runMorning()
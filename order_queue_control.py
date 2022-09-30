from biz.order_control import *
from biz.order_pur_control import *
from biz.order_queue import *
import time
import threading

class OrderThread(threading.Thread):
    def __init__(self):
        self.order_print = OrderPrint()
        threading.Thread.__init__(self)
    def run(self):
        while (1):
            try:
                self.order_print.execute(10)
            except Exception as e:
                order_control_logger.exception(e)
            time.sleep(5)

class OrderPrintThread(threading.Thread):
    def __init__(self):
        self.get_order = getQueuePrint()
        threading.Thread.__init__(self)
    def run(self):
        while (1):
            try:
                self.get_order.executePush()
            except Exception as e:
                order_control_logger.exception(e)
            time.sleep(5)

class OrderPurPrintThread(threading.Thread):

    def __init__(self):
        self.pur_print = PurPrint()
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                self.pur_print.execute(20)
            except Exception as e:
                order_control_logger.exception(e)
            time.sleep(5)

print('start youzan order queue control')
order_queue_print_thread = OrderPrintThread()
order_queue_print_thread.start()
#
# print('start youzan order control')
# order_print_thread = OrderThread()
# order_print_thread.start()
#
# print('start youzan order pur  control')
# order_pur_print_thread = OrderPurPrintThread()
# order_pur_print_thread.start()



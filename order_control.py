from biz.order_control import *
from biz.fix_order import *
from biz.order_pur_control import *
import time
import threading

class OrderPrintThread(threading.Thread):

    def __init__(self):
        self.pur_print = PurPrint()
        self.order_print = OrderPrint()
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                self.pur_print.execute(5)
                time.sleep(5)
                self.order_print.execute(10)
            except Exception as e:
                order_control_logger.exception(e)
            time.sleep(5)

print('start youzan order sy  control')
order_sy_zt_print_thread = OrderPrintThread()
order_sy_zt_print_thread.start()

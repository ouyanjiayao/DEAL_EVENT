from biz.order_pur_control import *
import time
import threading

class OrderPrintPurThread(threading.Thread):

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

print('start youzan order pur control')
order_pur_print_thread = OrderPrintPurThread()
order_pur_print_thread.start()

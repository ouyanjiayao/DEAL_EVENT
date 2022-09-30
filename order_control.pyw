from biz.order_control import *
import time

class OrderControlThread(threading.Thread):

    def __init__(self):
        self.order_control = OrderControl()
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                self.order_control.execute(50)
            except Exception as e:
                order_control_logger.exception(e)
            time.sleep(5)


class OrderPrintThread(threading.Thread):

    def __init__(self,type):
        self.order_print = OrderPrint(type)
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                self.order_print.execute(50)
            except Exception as e:
                order_control_logger.exception(e)
            time.sleep(5)

print('start youzan order control')
order_control_thread = OrderControlThread()
order_control_thread.start()
# order_zt_print_thread = OrderPrintThread(1)
# order_zt_print_thread.start()
# order_fk_print_thread = OrderPrintThread(2)
# order_fk_print_thread.start()
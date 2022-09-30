from biz.fix_order import *
import time
import threading


class OrderFixThread(threading.Thread):

    def __init__(self):
        self.order_fix = OrderFix()
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                self.order_fix.execute(20)
            except Exception as e:
                order_control_logger.exception(e)
            time.sleep(5)


print('start youzan order fix')
order_fix_thread = OrderFixThread()
order_fix_thread.start()

from biz.order_push import *
import time
import threading

class OrderPushThread(threading.Thread):

    def __init__(self):
        self.order_push = YzPush()
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                self.order_push.execute_to()
            except Exception as e:
                syn_logger.exception(e)
            time.sleep(5)

print('start youzan order push control')
order_purchase_thread = OrderPushThread()
order_purchase_thread.start()

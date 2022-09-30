from biz.normal_bill import *
import threading
import time
import sys

class BillToImageThread(threading.Thread):
    def __init__(self):
        self.normal_bill = NormalBill()
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                res = self.normal_bill.execute_to('total',1)
                if res==0:
                    sys.exit()
            except Exception as e:
                syn_logger.exception(e)
            time.sleep(100)

print('start bill to image')
bill_image_thread = BillToImageThread()
bill_image_thread.start()

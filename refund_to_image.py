from biz.refund_bill import *
import threading

class ReToImageThread():
    def __init__(self):
        self.refund_bill = RefundBill()
        threading.Thread.__init__(self)

    def run(self):
        try:
            self.refund_bill.execute_to('total')
        except Exception as e:
            syn_logger.exception(e)


refund_image_thread = ReToImageThread()
refund_image_thread.run()
print('start refund to image')

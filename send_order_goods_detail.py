from biz.send_order_detail import *
import threading
class SendGoodsDetailThread():
    def __init__(self):
        self.send_order_detail = OrderGoodsDetail()
        threading.Thread.__init__(self)
    def run(self):
        try:
            self.send_order_detail.execute_to()
        except Exception as e:
            syn_logger.exception(e)

send_goods_detail_thread = SendGoodsDetailThread()
send_goods_detail_thread.run()
print('start send_order_goods_detail')
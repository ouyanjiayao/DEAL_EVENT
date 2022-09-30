from biz.deal_order_data import *
import threading
class SendGoodsDataThread():
    def __init__(self):
        self.send_order_data = OrderGoodsData()
        threading.Thread.__init__(self)
    def run(self):
        try:
            self.send_order_data.execute_to()
        except Exception as e:
            syn_logger.exception(e)

send_goods_data_thread = SendGoodsDataThread()
send_goods_data_thread.run()
print('start send_order_goods_data')
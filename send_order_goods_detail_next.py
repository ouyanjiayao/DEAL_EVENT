from biz.send_order_detail_next import *
import threading
class SendGoodsDetailNextThread():
    def __init__(self):
        self.send_order_detail_next = OrderGoodsDetailNext()
        threading.Thread.__init__(self)
    def run(self):
        try:
            self.send_order_detail_next.execute_to()
        except Exception as e:
            syn_logger.exception(e)
        exit(0)

print('start send_order_goods_detail')
send_goods_detail_next_thread = SendGoodsDetailNextThread()
send_goods_detail_next_thread.run()
print('start send_order_goods_detail')
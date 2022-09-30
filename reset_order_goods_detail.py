from biz.reset_order_goods_detail import *
import threading
class ResetGoodsDetailThread():
    def __init__(self):
        self.reset_order_goods_detail = ResetOrderGoodsDetail()
        threading.Thread.__init__(self)
    def run(self):
        try:
            self.reset_order_goods_detail.execute_to()
        except Exception as e:
            print(e)
            syn_logger.exception(e)

reset_goods_detail_thread = ResetGoodsDetailThread()
reset_goods_detail_thread.run()
print('start reset_goods_detail')
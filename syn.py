from biz.goods_group_syn import *
from biz.upload_image_syn import *
from biz.goods_syn import *
from libs.youzan import *
import threading

class GoodsGroupThread(threading.Thread):

    def __init__(self):
        self.goods_group_syn = GoodsGroupSyn()
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                self.goods_group_syn.execute_to(10)
                self.goods_group_syn.execute_delete(10)
            except Exception as e:
                syn_logger.exception(e)
            time.sleep(5)

class UploadImageThread(threading.Thread):

    def __init__(self):
        self.upload_image_syn = UploadImageSyn()
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                self.upload_image_syn.execute_to(10)
                self.upload_image_syn.execute_delete()
            except Exception as e:
                syn_logger.exception(e)
            time.sleep(5)

class GoodsThread(threading.Thread):

    def __init__(self):
        self.goods_syn = GoodsSyn()
        threading.Thread.__init__(self)

    def run(self):
        while (1):
            try:
                self.goods_syn.execute_to(10)
                self.goods_syn.execute_delete(10)
            except Exception as e:
                syn_logger.exception(e)
            time.sleep(5)

print('start youzan syn')
goods_group_thread = GoodsGroupThread()
goods_group_thread.start()
upload_image_thread = UploadImageThread()
upload_image_thread.start()
goods_thread = GoodsThread()
goods_thread.start()
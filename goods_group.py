from gdt.get_goods_group import *
from libs.youzan import *
import threading

class GetGoodsGroupThread():

    def __init__(self):
        self.get_goods_group = GetGoodsGroupSyn()
        threading.Thread.__init__(self)

    def run(self):
          try:
              self.get_goods_group.execute_to()
          except Exception as e:
              tags_logger.exception(e)

goods_group_thread = GetGoodsGroupThread()
goods_group_thread.run()
print('start youzan get goods group')

from libs.helper import *
from custar.get_cus_tag import UpdataCustomerTag
from custar.get_expand_tag import UpdataExpandTag
import threading
import time

class GenerateCusTag(threading.Thread):

    def __init__(self):
        self.get_cus = UpdataCustomerTag()
        self.get_expand = UpdataExpandTag()
        threading.Thread.__init__(self)

    def run(self):

        # 拓展扫码标签
        try:
            self.get_expand.execute_to(10)
        except Exception as e:
            syn_logger.exception(e)

        # 其余标签
        while True: 
            try:
                self.get_cus.execute_to(10)
            except Exception as e:
                syn_logger.exception(e)
            time.sleep(1)


print('start reset cus tag')
cus_tags_thread = GenerateCusTag()
cus_tags_thread.start()


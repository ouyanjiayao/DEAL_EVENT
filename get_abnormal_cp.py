# from biz.get_customer import *
from biz.get_abnormalCp import AbnormalCp
from libs.helper import syn_logger
from libs.youzan import *
import threading
import time


class GetAbnormalCp(threading.Thread):

    def __init__(self):
        self.get_cp = AbnormalCp()
        threading.Thread.__init__(self)

    def run(self):

        while True:

            # 礼包表和组合表
            self.get_cp.execute_to("goods_cp_config")
            print("完")

            # except Exception as e:
            #     syn_logger.exception(e)

            time.sleep(1)


tags_thread = GetAbnormalCp()
tags_thread.start()
print('start get abnormal cp')

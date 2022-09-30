from libs.helper import *
from custar.get_cus_star import *
import threading
import time

class GetStarThread(threading.Thread):
    def __init__(self):
        self.get_star = UpdateCusStar()
        threading.Thread.__init__(self)

    def run(self):
        while True:
            try:
                # 重新写入星级
                self.get_star.execute_to(10)
            except Exception as e:
                syn_logger.exception(e)
            time.sleep(1)
            
print('start reset cus star')
cus_star_thread = GetStarThread()
cus_star_thread.start()

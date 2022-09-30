from custar.reset_syn_star import ResetSynStar
from custar.empty_cus_star import EmptyCusStar
from libs.helper import *
import time

class CusStarConfig:
    def __init__(self):
        self.reset_syn = ResetSynStar()
        self.empty_star = EmptyCusStar()

    def run(self):
        try:
            # 重置customer表标志
            self.reset_syn.execute_to()
            # 清空customer_star表
            self.empty_star.execute_to()

        except Exception as e:
            syn_logger.exception(e)
        

print('start star config')
star_config = CusStarConfig()
star_config.run()

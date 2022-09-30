from libs.helper import *

from custar.reset_no_offend import *
from custar.no_offend_cache import *
from custar.get_no_offend import *
from custar.empty_cus_tag import *
from custar.reset_syn_tag import *
import time

# 不能得罪客户
class DealNoOffend():

    def __init__(self):
        self.reset_offend = CleanNoOffend()
        self.generate_cache = NoOffendCache()

    def run(self):
        try:
            # 清空中间表
            self.reset_offend.execute_to()
            time.sleep(5)
            # 生成缓存，获取不能惹用户标签写入中间表
            self.generate_cache.execute_to()

        except Exception as e:
            syn_logger.exception(e)

class CleanTagAff():

    def __init__(self):
        self.reset_tag_aff = ResetTagAff()

    def run(self):
        try:
            self.reset_tag_aff.execute_to()
        except Exception as e:
            syn_logger.exception(e)

class GenNoOffend():

    def __init__(self):
        self.syn_tag = ResetSynTag()
        self.no_offend = GetNoOffend()

    def run(self):
        try:
            # 重置customer表标志
            self.syn_tag.execute_to()
            # 不能惹用户标签写入
            self.no_offend.execute_to()
        except Exception as e:
            syn_logger.exception(e)

print('start reset no offend')
no_offend_thread = DealNoOffend()
no_offend_thread.run()


print('start clean tag aff')
clean_tag_aff_thread = CleanTagAff()
clean_tag_aff_thread.run()


print('start write no offend')
set_no_offend_thread = GenNoOffend()
set_no_offend_thread.run()




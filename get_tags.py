from ggd.get_tags import *
from libs.youzan import *
import threading

class GetTagsThread():

    def __init__(self):
        self.get_tags = GetTags()
        threading.Thread.__init__(self)

    def run(self):
        try:
            self.get_tags.execute_to()
        except Exception as e:
            tags_logger.exception(e)
        time.sleep(5)

tags_thread = GetTagsThread()
tags_thread.run()
print('start get goods tags syn')

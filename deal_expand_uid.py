from biz.expand_queue import *
import time
import threading

class ExpandThread(threading.Thread):
    def __init__(self):
        self.ex_queue = ExpandQueue()
        threading.Thread.__init__(self)
    def run(self):
        while (1):
            try:
                self.ex_queue.expand()
            except Exception as e:
                syn_logger.exception(e)
            time.sleep(10)

print('start expand queue control')
expand_queue_thread = ExpandThread()
expand_queue_thread.start()



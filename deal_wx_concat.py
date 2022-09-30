from biz.concat_to_excel import *
import threading
class SendConcatThread():
    def __init__(self):
        self.concat_excel = ConcatToExcel()
        threading.Thread.__init__(self)
    def run(self):
        try:
            self.concat_excel.execute_to()
        except Exception as e:
            syn_logger.exception(e)

send_goods_data_thread = SendConcatThread()
send_goods_data_thread.run()
print('start send concat')
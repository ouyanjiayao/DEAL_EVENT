from libs.youzan import *
from syn.syn_list import *
from syn.syn_news import *
from syn.syn_sku import *
from ggd.deal_tag import GetTags
import threading

class SynThread(threading.Thread):

    def __init__(self):
        self.get_list = synList()
        self.syn_news = synNews()
        self.syn_skus = synSku()
        self.syn_tags = GetTags()
        threading.Thread.__init__(self)

    def run(self):
        try:
            print('获取更新列表...')
            syn_items,now = self.get_list.deal_id()
            print('同步基本信息...')
            self.syn_news.getItem(syn_items,now)
            print('同步规格信息...')
            self.syn_skus.synSku(syn_items)
            print('同步分组...')
            self.syn_tags.execute_to(syn_items)
        except Exception as e:
            print('Error...')
            syn_logger.exception(e)

syn_thread = SynThread()
syn_thread.run()
print('start get goods syn')

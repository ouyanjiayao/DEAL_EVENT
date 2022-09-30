import sys
from libs.youzan import ApiClient
from libs.helper import *
from ugd.get_offline_info import *
import time
import os
import json

class UpdateDelivery:
    def __init__(self):
        self.db_helper = DBHelper()
        self.api = ApiClient()
        self.offline_ids = GetOfflineInfo()

    def update_goods_delivery(self, goods_ids, cursor):

        log_inserts = []
        offline_ids = self.offline_ids.get_offline_ids()

        api_name = 'youzan.multistore.goods.delivery.update'
        if goods_ids:
            for row in goods_ids:
                try:
                    syn_state = 1
                    for i, info in enumerate(offline_ids):
                        response = self.api.invoke(api_name, '3.0.0', {
                            'settings': json.dumps({row: {"express": 0, "local_delivery": 1, "self_fetch": 0}}),
                            'offline_id': str(info, encoding='gbk')
                        })

                    if not response:
                        response_content = '接口调用失败'
                    else:
                        response_content = json.dumps(response, ensure_ascii=False)
                        if not response['response']['is_success']:
                            syn_state = 2
                        else:
                            syn_state = 3

                    log_inserts.append((response_content, time.time(), 4, row, syn_state, api_name))
                    time.sleep(1)

                except Exception as e:
                    syn_logger.exception(e)
                if len(log_inserts) > 0:
                    cursor.executemany("insert into syn_log(response_content,created_time,type,syn_id,syn_state,api_name) values(%s,%s,%s,%s,%s,%s)",log_inserts)
                   

import sys
from libs.youzan import ApiClient
from libs.helper import *
import time
import os
import json

class GetOfflineInfo:
    def __init__(self):
        self.db_helper = DBHelper()
        self.api = ApiClient()
        self.redis_helper = RedisHelper()

    def get_offline_ids(self):
        redisInfo = self.redis_helper.getConnect()
        get_offline_ids = redisInfo.lrange('offline_ids', 0, -1)
        if not get_offline_ids:
            api_name = 'youzan.multistore.offline.search'
            offline_info = self.api.invoke(api_name, '3.0.0')
            try:
                if not offline_info:
                    conn = self.db_helper.getConnect()
                    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
                    cursor.execute("SELECT distinct(offline_id) FROM `print_conf` order by id desc")
                    rows = cursor.fetchall()
                    for i, line in enumerate(rows):
                        redisInfo.lpush('offline_ids', line['offline_id'])
                else:
                    for index, offline in enumerate(offline_info['response']['list']):
                        redisInfo.lpush('offline_ids', offline['id'])
            except Exception as e:
                syn_logger.exception(e)
        return get_offline_ids


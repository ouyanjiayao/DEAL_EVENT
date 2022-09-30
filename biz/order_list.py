from libs.helper import *
import time
import json
import sys
import re

class getListPrint:
    #type=1TOTAL打印机 type=2BRANCH打印机
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.redis_helper = RedisHelper()

    def executePush(self, limit):
        all_list = []
        pur_list = redis.lpop("pur_list")
        normal_list = redis.lpop("normal_list")
        all_list.append(pur_list)
        for i in normal_list:
            if i not in all_list:
                all_list.append(i)
        redis.rpush("print_list", json.dumps(all_list))

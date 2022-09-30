from libs.helper import *
import time
import json
import sys
import re

class getQueuePrint:
    #type=1TOTAL打印机 type=2BRANCH打印机
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.redis_helper = RedisHelper()
        self.redis = self.redis_helper.getConnect()
        self.purchase_list = []
        self.nor_list = []
        self.backend = []

    def executePush(self):
        if not self.redis:
            self.redis = self.redis_helper.getConnect()
        all_list = []
        pur_list = self.redis.lpop("pur_list")
        normal_list = self.redis.lpop("normal_list")
        backend_list = self.redis.lpop("backend")
        if pur_list:
            self.purchase_list = eval(self.redis.decode('UTF-8'))
            for i in self.purchase_list:
                all_list.append(i)
        if backend_list:
            self.backend = eval(backend_list.decode('UTF-8'))
            all_list.append(self.backend)
        if normal_list:
            self.nor_list = eval(normal_list.decode('UTF-8'))
            all_list.append(self.nor_list)

        if all_list:
            redis.rpush("print_list", json.dumps(all_list))

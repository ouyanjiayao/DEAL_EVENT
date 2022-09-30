from libs.helper import *
import requests
import time
import json

class YzPush:
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.redis_helper = RedisHelper()
        self.redis = self.redis_helper.getConnect()
        self.conn = self.db_helper.getConnect()
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)

    def execute_to(self):
        if not self.redis:
            self.redis = self.redis_helper.getConnect()
        yz_receive = self.redis.lpop("yz_push_receive")
        if yz_receive:
            receive_con = json.loads(yz_receive)
            syn_logger.exception(receive_con)
            try:
                res = ''
                url = self.default_config['youzan']['push_url']
                headers={'content-type':'application/x-www-form-urlencoded','Connection':'close'}
                r = requests.put(url, data=yz_receive, headers=headers,verify=False)
                res = r.status_code
                syn_logger.exception(res)
                if res != 200:
                    self.cursor.execute("insert into order_error(msg,tid,msg_type,updated_time) values(%s,%s,%s,%s)",(yz_receive, receive_con.get('id'), receive_con.get('status'), time.time()))
                    self.conn.commit()
            except Exception as e:
                self.cursor.execute("insert into order_error(msg,tid,msg_type,updated_time) values(%s,%s,%s,%s)",(yz_receive, receive_con.get('id'), receive_con.get('status'), time.time()))
                self.conn.commit()
                syn_logger.exception(e)
        time.sleep(3)
        self.cursor.close()
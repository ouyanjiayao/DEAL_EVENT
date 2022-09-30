from libs.helper import *
import json
class NormalQueuePrint:
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.conn = self.db_helper.getConnect()
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.redis_helper = RedisHelper()
        self.print_rows = []
    def executeNormal(self,limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute('select switch_state from syn_switch where switch_name=%s', 'print_syn')
        is_run = cursor.fetchone()
        if not is_run['switch_state']:
            pass
        else:
            redis = self.redis_helper.getConnect()
            cursor.execute('select * from order where state = 0 and order_state>1 and order_num is not null and (zt_print_state=0 or fk_print_state=0) and delivery_start_time BETWEEN UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE)) AND UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE) + INTERVAL 1 DAY) order by id asc limit %s',limit)
            rows = cursor.fetchall()
            self.print_rows = [i['id'] for i in rows]
            if len(self.print_rows) > 0:
                redis.rpush("normal_list", json.dumps(self.print_rows))
        if len(self.print_rows) > 0:
            print('normal:')
            print(self.print_rows)
            items = "update order set state = 1 where id in (%s)" % ','.join(['%s'] * len(self.print_rows))
            cursor.execute(items, self.print_rows)



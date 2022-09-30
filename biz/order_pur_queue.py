from libs.print import *
from libs.helper import *
import json
class PurQueuePrint:
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.redis_helper = RedisHelper()
        self.conn = self.db_helper.getConnect()
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.pur_rows = []
        self.pur_ids = []

    def executePurOrder(self, limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute('select switch_state from syn_switch where switch_name=%s','print_syn')
        is_run = cursor.fetchone()
        if not is_run['switch_state']:
            pass
        else:
            cursor.execute('select id,order_id from order_purchase where state=0 and inner_state in (0,2) and user_id!=0 and delivery_time BETWEEN UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE)) AND UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE) + INTERVAL 1 DAY) order by id desc limit %s',(limit))
            rows = cursor.fetchall()
            if len(rows) > 0:
                redis = self.redis_helper.getConnect()
                self.pur_rows = [i['order_id'] for i in rows]
                self.pur_ids = [i['id'] for i in rows]
                try:
                    if len(self.pur_rows) > 0:
                        redis.rpush("pur_list", json.dumps(self.pur_rows))
                except Exception as e:
                    order_control_logger.exception(e)
            if len(self.pur_ids) > 0:
                print('purchase:')
                print(self.pur_rows)
                items = "update order_purchase set state = 1 where id in (%s)" % ','.join(['%s'] * len(self.pur_ids))
                cursor.execute(items, self.pur_ids)
                conn.commit()
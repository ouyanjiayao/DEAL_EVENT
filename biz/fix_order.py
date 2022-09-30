from libs.print import *
from libs.helper import *
from libs.youzan import ApiClient
import time
import json
import sys
import re

class OrderFix:

    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.conn = self.db_helper.getConnect()
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.api = ApiClient()
        
    def execute(self, limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute('select switch_state from syn_switch where switch_name=%s','print_syn')
        is_run = cursor.fetchone()
        if is_run['switch_state']:
            #订单补漏
            cursor.execute('select created_time,delivery_start_time,tid from order where order_state=1 and (zt_print_state=0 or fk_print_state=0) order by id asc limit %s', (limit))
            created_row = cursor.fetchall()
            if created_row:
                now = int(time.time())
                for i in created_row:
                    dead_line = i['delivery_start_time'] - i['created_time']
                    if dead_line > 0 and dead_line <= 5400:
                        wait_time = 120
                    elif dead_line <= 9000:
                        wait_time = 300
                    elif dead_line <= 12600:
                        wait_time = 600
                    elif dead_line > 12600:
                        wait_time = 1200
                    if (now - i['created_time']) % wait_time < 6:
                        data = self.api.invoke('youzan.trade.get','4.0.0',{
                            'tid':i['tid']
                        })
                        order_status = data['data']['full_order_info']['order_info']['status']
                        if order_status == 'WAIT_SELLER_SEND_GOODS':
                            cursor.execute("update order set order_state=2 where tid=%s",i['tid'])
                            conn.commit()
                        elif order_status == 'WAIT_BUYER_CONFIRM_GOODS':
                            cursor.execute("update order set order_state=3 where tid=%s",i['tid'])
                            conn.commit()
                        elif order_status == 'TRADE_SUCCESS':
                            cursor.execute("update order set order_state=4 where tid=%s",i['tid'])
                            conn.commit()
                        elif order_status == 'TRADE_CLOSED':
                            cursor.execute("update order set order_state=0 where tid=%s",i['tid'])
                            conn.commit()
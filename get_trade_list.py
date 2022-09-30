from requests import api
from libs.helper import *
from libs.youzan import ApiClient
import math
import time
import json
import urllib
import urllib.parse
import re

class GetTrade:
    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None
        self.default_config = ConfigHelper.getDefault()
        self.api = ApiClient()
        self.redis_helper = RedisHelper()

    def get_trade_list(self,start_date,end_date,pageno=1):
        try:
            api_detail = 'youzan.trades.sold.get'
            page_size = 2
            data = {
                # 'start_created': "2022-07-" + start_date + " 23:00:00",
                # 'end_created': "2022-07-" + end_date + " 00:00:00",
                # 'delivery_start_time': "2022-07-" + start_date + " 00:00:00",
                # 'delivery_end_time': "2022-07-" + end_date + " 00:00:00",
                'delivery_start_time' : time.strftime('%Y-%m-%d',time.localtime(time.time()))+" 00:00:00",
                'delivery_end_time': time.strftime('%Y-%m-%d',time.localtime(time.time()+86400))+" 00:00:00",
                # 'tid': "E20220903235102020506157",
                # "express_type": "SELF_FETCH",
                'page_no' : pageno,
                'page_size' : page_size
            }
            item_get = self.api.invoke(api_detail, '4.0.2', data)
            total_results = item_get['data']['total_results']
            if not item_get['data']:
                self.get_next_page(total_results, page_size, pageno, start_date, end_date)
            data_list = item_get['data']['full_order_info_list']
            if not data_list:
                self.get_next_page(total_results, page_size, pageno, start_date, end_date)

            conn = self.db_helper.getConnect()
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            self.cursor = cursor
            inserts = []
            for j in data_list:
                try:
                    items = self.filter_emoji(json.dumps(j))
                    fans_nickname = self.filter_emoji(j['full_order_info']['buyer_info']['fans_nickname'])
                    order_info = j['full_order_info']['order_info']
                    salesman_nick_name = ''
                    buyer_name = ''
                    if order_info:
                        order_extra = order_info.get('order_extra')
                        if order_extra:
                            salesman = order_extra.get('salesman')
                            buyername = order_extra.get('buyer_name')
                            if salesman:
                                salenick_name = salesman.get('salesman_nick_name')
                                salesman_nick_name = self.filter_emoji(salenick_name)
                            if buyername:
                                buyer_name = self.filter_emoji(buyername)
                    j['full_order_info']['buyer_info']['fans_nickname'] = fans_nickname
                    if buyer_name:
                        j['full_order_info']['order_info']['order_extra']['buyer_name'] = buyer_name
                    if salesman_nick_name:
                        j['full_order_info']['order_info']['order_extra']['salesman']['salesman_nick_name'] = salesman_nick_name

                    res = True
                    tid = j['full_order_info']['order_info']['tid']
                    status = j['full_order_info']['order_info']['status']
                    if status != 'TRADE_CLOSED':
                        res = self.exist_order_data(self.cursor, tid)
                    syn_logger.exception(tid)
                    syn_logger.exception(res)
                    if res == False:
                        con = self.wrap_con(items, tid, status)
                        syn_logger.exception(con)
                        if con:
                            self.push_redis(con)
                    inserts.append((items, j['full_order_info']['address_info']['delivery_start_time'], tid, status, j['full_order_info']['pay_info']['payment']))
                except Exception as e:
                    syn_logger.exception(e)
            if len(inserts) > 0:
                try:
                    self.cursor.executemany("insert into trade_list(content,delivery_time,tid,status,payment) values(%s,%s,%s,%s,%s)", inserts)
                    conn.commit()
                except Exception as e:
                    syn_logger.exception(e)
            self.get_next_page(total_results, page_size, pageno, start_date, end_date)
        except Exception as e:
            syn_logger.exception(e)

    # print(item_get['data']['full_order_info_list'][0]['full_order_info'])
    # 日期range(25, 28):25日到27日数据
    def get_date(self):
        for i in range(21, 22):
            n = str(i)
            n2 = str(i+1)
            s = n.zfill(2)
            s2 = n2.zfill(2)
            self.get_trade_list(s, s2, 1)
            time.sleep(5)

    # 下一页
    def get_next_page(self,total_results,page_size,pageno,start_date,end_date):
        total_page = math.ceil(total_results / page_size)
        if pageno < total_page:
            page_no = pageno + 1
            self.get_trade_list(start_date, end_date, page_no)
            time.sleep(5)

    # 是否存在order
    def exist_order_data(self, cursor, tid):
        cursor.execute('select * from order where tid = %s', tid)
        order_row = cursor.fetchone()
        res = False
        if order_row:
            res = True
        return res


    # 封装数据格式
    def wrap_con(self,items, tid, status):
        item = urllib.parse.quote_plus(items)
        wrap_data = {
            'msg': item,
            "kdt_name": "",
            "test": False,
            "sign": "4a415f681e2404a49e1651d64111afa2",
            "type": "trade_TradePaid",
            "sendCount": 1,
            "version": 2141,
            "client_id": "7a8ea6840e2b0d160c",
            "mode": 1,
            "kdt_id": 1414,
            "id": tid,
            "msg_id": "d5a89d96-c145-4dfc-add5-789bb05c6d88",
            "root_kdt_id": 2141,
            "status" : status
        }
        return json.dumps(wrap_data)

    # 存入redis
    def push_redis(self, con):
        redisInfo = self.redis_helper.getConnect()
        redisInfo.lpush('yz_push_receive', con)

    def filter_emoji(self, desstr, restr=''):
        try:
          co = re.compile(u'[\U00010000-\U0010ffff]')
        except re.error:
          co = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]')
        return co.sub(restr, desstr)
        # co = re.compile(u'[\U00010000-\U0010ffff\uD800-\uDBFF\uDC00-\uDFFF]')
   

bill_image_thread = GetTrade()
bill_image_thread.get_date()




from libs.helper import *
import re

class DealSku:

    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None

    def deal_system(self,total):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute('select switch_state from syn_switch where switch_name=\"sku_syn\"')
        is_run = cursor.fetchone()
        if not is_run['switch_state']:
            pass
        else:
            o = []
            cursor.execute("SELECT id FROM `goods_attr_copy`")
            attr_ids = cursor.fetchall()
            try:
                for i in attr_ids:
                    o.append(i['id'])
                cursor.execute("SELECT id,sku_id FROM `goods_sku_system` where sku_status = 0 order by id desc limit %s",(total))
                sys = cursor.fetchall()
                self.deal_sku(sys, cursor, 'goods_sku_system', o)
            except Exception as e:
                syn_logger.exception(e)

    def deal_detail(self,total):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute('select switch_state from syn_switch where switch_name=\"sku_syn\"')
        is_run = cursor.fetchone()

        if not is_run['switch_state']:
            pass
        else:
            o = []
            cursor.execute("SELECT id FROM `goods_attr_copy`")
            attr_ids = cursor.fetchall()
            try:
                for i in attr_ids:
                    o.append(i['id'])
                cursor.execute("SELECT id,sku_id FROM `goods_sku_detail` where sku_status = 0 order by id desc limit %s",(total))
                details = cursor.fetchall()
                self.deal_sku(details, cursor, 'goods_sku_detail', o)

            except Exception as e:
                syn_logger.exception(e)

    def deal_price(self,total):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute('select switch_state from syn_switch where switch_name=\"sku_syn\"')
        is_run = cursor.fetchone()

        if not is_run['switch_state']:
            pass
        else:
            o = []
            cursor.execute("SELECT id FROM `goods_attr_copy`")
            attr_ids = cursor.fetchall()
            try:
                for i in attr_ids:
                    o.append(i['id'])

                cursor.execute("SELECT id,sku_id FROM `goods_sku_price` where sku_status = 0 order by id desc limit %s",(total))
                rows = cursor.fetchall()
                self.deal_sku(rows, cursor, 'goods_sku_price', o)

            except Exception as e:
                syn_logger.exception(e)

    def deal_sku(self, details, cursor, table_name,o):
        updates = []
        d = []
        str = []
        status = 0
        try:
            for i in details:
                d = []
                str = []
                a = re.split(':', i['sku_id'])
                for j in a:
                    b = re.split('_', j)
                    if int(b[0]) not in o:
                        d.append(b[0] + '_' + b[1])
                    # if int(b[0]) in o and len(a) < 3:
                    #     d = []
                    #     pass
                str.append(':'.join(d))
                status = 1
                updates.append((str, status, i['id']))
            cursor.executemany("update `" + table_name + "` set no_sku_id = %s , sku_status = %s where id=%s", updates)
        except Exception as e:
            syn_logger.exception(e)
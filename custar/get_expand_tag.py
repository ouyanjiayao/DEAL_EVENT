from libs.helper import *
import pymysql
import time

class UpdataExpandTag:
    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None
        self.default_config = ConfigHelper.getDefault()
        self.tag_list = self.default_config['cus_tag']

    def execute_to(self,limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor


        while True:
            yz_ids = []
            yz_id_list = []
            # 先从扫码表拿出十个没有写入标签的
            cursor.execute("SELECT yz_open_id FROM customer WHERE union_id IN "
                           "( SELECT union_id FROM wechat_customer WHERE code_id <> '' AND created_time >= 1631872522 ) "
                           "AND syn_tag <2 limit %s",(limit))
            yz_ids = cursor.fetchall()

            print(len(yz_ids))
            # 判断获取的客户是否为空
            if len(yz_ids) == 0:
                break

            # 非空再对用户进行标签判断
            if len(yz_ids) > 0:
                for i in yz_ids:
                    yz_id_list.append(i['yz_open_id'])

            # 修改客户标签标记为2
            if len(yz_id_list) > 0:
                cursor.execute('update customer set syn_tag = 2 where yz_open_id IN %s', ((yz_id_list),))
                cursor.executemany('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(14,%s)',yz_id_list)
                conn.commit()
            time.sleep(5)

        cursor.close()
        conn.close()
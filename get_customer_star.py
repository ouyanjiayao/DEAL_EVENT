# 商品规格复制之后全部写入商品第一个规格值的商品配置

# from biz.get_customer import *
import pymysql

from app.console.biz.get_customer_star import CustomerStar
from app.console.libs.helper import syn_logger, DBHelper
from libs.youzan import *
import threading
import time


class GetThread(threading.Thread):
    def __init__(self):
        self.get_cus = CustomerStar()
        threading.Thread.__init__(self)
        self.db_helper = DBHelper()
        self.cursor = None



    def run(self):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

        # 清空star表
        cursor.execute('TRUNCATE customer_star')
        # 客户表星级标签归0
        cursor.execute('update customer set syn_star = 0')
        cursor.close()
        while True:
            try:

                self.get_cus.execute_to()


            except Exception as e:
                syn_logger.exception(e)
            time.sleep(1)


tags_thread = GetThread()
tags_thread.start()
print('start get customer')

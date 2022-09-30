from libs.helper import *
import pymysql

class NoOffendCache:
    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None

    def execute_to(self):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor

        # 获取不能惹的客户标签
        offend_sql = 'INSERT INTO cus_no_offend(tag_id,yz_open_id) select tag_id,yz_open_id from customer_tag_affiliate where tag_id = 10000000'
        cursor.execute(offend_sql)
        conn.commit()        
        cursor.close()
        conn.close()









from libs.helper import *
import pymysql

class CleanNoOffend:
    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None

    def execute_to(self):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        # 清空不能惹的客户标签
        cursor.execute("TRUNCATE TABLE cus_no_offend") 
        cursor.close()
        conn.close()
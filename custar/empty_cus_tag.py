from libs.helper import *
import pymysql

class ResetTagAff:
    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None

    def execute_to(self):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute("TRUNCATE TABLE customer_tag_affiliate")
        cursor.close()
        conn.close()











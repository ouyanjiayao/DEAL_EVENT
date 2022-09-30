from libs.helper import *
import pymysql

class ResetSynTag:
    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None

    def execute_to(self):

        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor

        cursor.execute("update customer set syn_tag = 0")
        conn.commit()   
        cursor.close()
        conn.close()
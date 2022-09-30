import pymysql
from libs.helper import *
import time
import json

class ResetSynStar:
    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None

    def execute_to(self):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute("update customer set syn_star = 0")
        conn.commit()   
        cursor.close()
        conn.close()


import pymysql
from libs.helper import *

class EmptyCusStar:
    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None

    def execute_to(self):

        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute("TRUNCATE customer_star")
        cursor.close()
        conn.close()











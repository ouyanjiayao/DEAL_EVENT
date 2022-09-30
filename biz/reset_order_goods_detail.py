from libs.helper import *
import time
class ResetOrderGoodsDetail:
    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None
        self.default_config = ConfigHelper.getDefault()
    def execute_to(self):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        sql = "TRUNCATE TABLE  `bill_dp_on`;"
        try:
            cursor.execute(sql)
            cursor.close()
        except pymysql.Error as e:
            print(e)


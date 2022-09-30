from libs.helper import *
import pymysql

class GetNoOffend:
    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None

    def execute_to(self):

        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor

        # 获取不能惹的客户标签
        cursor.execute('insert into customer_tag_affiliate(tag_id,yz_open_id)'
                       'select tag_id,yz_open_id from cus_no_offend')
        conn.commit()   
        cursor.close()
        conn.close()









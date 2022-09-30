# from biz.get_customer import *
import pymysql
from app.console.biz.get_customer import GetCustomer
from app.console.libs.helper import syn_logger, DBHelper
from libs.youzan import *
import threading
import time
import pandas as pd

class CommunityBuyCount:

    def __init__(self):
        self.get_cus = GetCustomer()
        self.db_helper = DBHelper()
        self.cursor = None

    def execute_to(self,begin,end):

        print(begin,end)
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor

        village = ['尚海阳光花园','阳光海岸','金禧花园','香域春天','金紫世家','星湖城','星光华庭','龙腾嘉园','世贸花园','龙腾熙园','中信海滨花园','万豪南湾','锦泰花园','星湖豪景','金丰花园','佰悦春天','春江花园','天华美地','中海花园','御海阳光花园','御海天禧','山海豪庭','柏嘉半岛','壹品湾','御海禧园','天合名轩']
        count = []

        for i in village:
            print(i)
            cursor.execute("select IF(count(1) IS NULL,0,count(1)) as num from customer a "
                           "INNER JOIN customer_order b on a.yz_open_id = b.yz_open_id "
                           "where a.address REGEXP %s and b.created_time BETWEEN UNIX_TIMESTAMP(%s) and UNIX_TIMESTAMP(%s)", (i,begin,end))
            buy_count = cursor.fetchone()
            count.append(buy_count['num'])

        dataframe=pd.DataFrame({'小区':village,'购买数量':count})
        dataframe.to_csv('e:/Users/Administrator.WIN-HSSOP2S8HAG/Desktop/test.csv',index=True,sep=',',encoding='utf_8_sig')

communityBuyCount = CommunityBuyCount()
communityBuyCount.execute_to('2022-2-1','2022-3-1')
print('小区购买数')

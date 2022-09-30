import pymysql
from libs.helper import *
from libs.youzan import ApiClient
from libs.helper import ConfigHelper
import time
import json
import gc


class CustomerStar:
    def __init__(self):
        self.api = ApiClient()
        self.db_helper = DBHelper()
        self.cursor = None

    def execute_to(self):
        t = time.time()
        print(int(t))
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor

        # 从客户表和客户标签关联表拿出十个没同步的用户id

        cursor.execute("select t1.yz_open_id from (select yz_open_id from customer_tag_affiliate GROUP BY yz_open_id) t1 "
                       "left join(select yz_open_id,syn_star from customer where syn_star != 1) t2  "
                       "on t1.yz_open_id = t2.yz_open_id where t2.syn_star!=1 limit 5")
        yz_ids = cursor.fetchall()

        conn.close()
        # 判断获取的客户是否为空
        if len(yz_ids) == 0:
            print("星级写入完毕")
            exit()

        # 非空再对用户进行标签判断
        else:
            print(yz_ids)


        diction = []
        syn_list = []
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        for i in yz_ids:
            syn_list.append(i['yz_open_id'])
            cursor.execute("select tag_id from customer_tag_affiliate where yz_open_id = %s",(i['yz_open_id']))
            tag_ids = cursor.fetchall()

            num = 0b00000000000000      #标记该用户所拥有的标签
            star = 0
            for j in tag_ids:
                tag_id = j['tag_id']

                if tag_id == 1:         #高价值用户      8192
                    num += 2 ** 13
                elif tag_id == 2:       #中价值用户      4096
                    num += 2 ** 12
                elif tag_id == 3:       #价值用户       2048
                    num += 2 ** 11
                elif tag_id == 4:       #高度活跃用户     1024
                    num += 2 ** 10
                elif tag_id == 5:       #中度活跃用户     512
                    num += 2 ** 9
                elif tag_id == 6:       #轻度活跃用户     256
                    num += 2 ** 8
                elif tag_id == 7:       #喜好组合       128
                    num += 2 ** 7
                elif tag_id == 8:       #新用户          64
                    num += 2 ** 6
                elif tag_id == 9:       #轻度流失用户     32
                    num += 2 ** 5
                elif tag_id == 10:      #流失用户       16
                    num += 2 ** 4
                elif tag_id == 11:      #高度流失用户     8
                    num += 2 ** 3
                elif tag_id == 12:      #未激活用户      4
                    num += 2 ** 2
                elif tag_id == 10000000:#不能得罪用户     2
                    num += 2 ** 1
                elif tag_id == 10000001:#巨划算用户      1
                    num += 2 ** 0

            # 根据拥有的标签判断用户星级
            if num in [9345,8833,8577,9344,9217,9216,8832,8576,8705,8449]:
                star = 5
            elif num in [2,8192,5249,4737,4481,5248,4736,5120]:
                star = 4
            elif num in [4096,4608,4352]:
                star = 3
            elif num in [2048,1]:
                star = 2

            # 用户id和星级写入数组
            if star != 0:
                diction.append((i['yz_open_id'],star))
            gc.collect()
        conn.close()

        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.executemany("INSERT INTO customer_star (`yz_open_id`, `star`) VALUES (%s, %s)",diction)
        conn.commit()
        # 最后修改星级标签标记为1
        cursor.execute('update customer set syn_star = 1 where yz_open_id IN %s',((syn_list),))
        conn.commit()
        conn.close()







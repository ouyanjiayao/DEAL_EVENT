from libs.helper import *
import time
import json

class UpdateCusStar:
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.tag_list = self.default_config['cus_tag']
        self.cursor = None
        self.conn = None

    def execute_to(self, limit):
        self.conn = self.db_helper.getConnect()
        cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        yz_ids = []
        # 从客户表和客户标签关联表拿出十个没同步的用户id
        self.cursor.execute("select t1.yz_open_id from (select yz_open_id from customer_tag_affiliate GROUP BY yz_open_id) t1 left join(select yz_open_id,syn_star from customer where syn_star != 1) t2  on t1.yz_open_id = t2.yz_open_id where t2.syn_star!=1 limit %s",(limit))
        yz_ids = self.cursor.fetchall()


        # 判断获取的客户是否为空
        if len(yz_ids) == 0:
            exit()

        # 非空再对用户进行标签判断
        syn_list = []
        if len(yz_ids) > 0:
            syn_list = [i['yz_open_id'] for i in yz_ids]
            # 修改星级标签标记为1
            self.cursor.execute('update customer set syn_star = 1 where yz_open_id IN %s', ((syn_list),))
            self.conn.commit()   
            try:
                self.deal_cus_star(self.cursor,syn_list)
            except Exception as e:
                order_control_logger.exception(e)
            time.sleep(5)

        self.cursor.close()
        self.conn.close()

    def deal_cus_star(self,cursor,id_list):
        diction = []
        for i in id_list:
            # 获取每个客户的所有tag
            cursor.execute("select tag_id from customer_tag_affiliate where yz_open_id = %s",(i))
            tag_ids = cursor.fetchall()
        
            
            num = 0                     #标记该用户所拥有的标签
            star = 0

            # 分析每个标签
            for j in tag_ids:
                tag_id = j['tag_id']

                if tag_id == int(self.tag_list['tag_highWorth']):             #高价值用户      8192
                    num += 2 ** 13
                elif tag_id == int(self.tag_list['tag_modWorth']):            #中价值用户      4096
                    num += 2 ** 12
                elif tag_id == int(self.tag_list['tag_liteWorth']):           #价值用户       2048
                    num += 2 ** 11
                elif tag_id == int(self.tag_list['tag_highAct']):             #高度活跃用户     1024
                    num += 2 ** 10
                elif tag_id == int(self.tag_list['tag_modAct']):               #中度活跃用户     512
                    num += 2 ** 9
                elif tag_id == int(self.tag_list['tag_liteAct']):              #轻度活跃用户     256
                    num += 2 ** 8
                elif tag_id == int(self.tag_list['tag_cp']):                   #喜好组合       128
                    num += 2 ** 7
                elif tag_id == int(self.tag_list['tag_buyOnce']):              #新用户          64
                    num += 2 ** 6
                elif tag_id == int(self.tag_list['tag_liteLeave']):            #轻度流失用户     32
                    num += 2 ** 5
                elif tag_id == int(self.tag_list['tag_modLeave']):             #流失用户       16
                    num += 2 ** 4
                elif tag_id == int(self.tag_list['tag_highLeave']):            #高度流失用户     8
                    num += 2 ** 3
                # elif tag_id == 12:      #未激活用户      4
                #     num += 2 ** 2
                elif tag_id == int(self.tag_list['tag_noOffend']):             #不能得罪用户     2
                    num += 2 ** 1
                elif tag_id == int(self.tag_list['tag_bigWorth']):             #巨划算用户      1
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
                try:
                    cursor.execute("insert into customer_star(yz_open_id, star) values(%s, %s)", (i, star))
                    self.conn.commit()   
                except Exception as e:
                    order_control_logger.exception(e)









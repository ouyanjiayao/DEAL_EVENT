import pymysql
from libs.helper import *
from libs.youzan import ApiClient
from libs.helper import ConfigHelper
import time
import json
from biz.send_email import *
from biz.deal_wx_concat import *


class AbnormalCp:
    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None
        self.dealWxConcat = DealWxConcat()
        self.sendMail = SendMail()


    def execute_to(self, table):
        print("查找商品配置不符的组合")

        now = time.strftime("%Y-%m-%d", time.localtime())

        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor



        sql = "SELECT a.goods_id,a.dp_goods_id,a.content FROM "\
              +table+" a INNER JOIN goods c on a.goods_id = c.id " \
                     "where c.is_delete =0 ORDER BY a.goods_id DESC"

        cursor.execute(sql)
        a = cursor.fetchall()




        for i in a:
            goods_id = i['goods_id']
            dp_id = i["dp_goods_id"]
            if (i['content'] is None) or (i['content'] == ""):
                print("商品ID\t", goods_id, "\t里面的", dp_id, "配置为空")
                continue
            else:
                content = json.loads(i['content'])
            # unit = content['unit']


            if content['attr'] is not None:

                attr_length = len(content['attr'])
                attr_id1 = content['attr'][0]['attr_id']
                item_id1 = content['attr'][0]['item_id']

                # 目前遍历的第一个商品的规格值
                attr_config1 = attr_id1 + "_" + item_id1

                # 规格值列表
                attrlist = []

                # 规格值错误的商品名称
                goods_name = ""

                # 规格值名称
                item_name = ""


                # 如果该商品只有一个规格值
                if attr_length == 1:
                    cursor.execute("select attr_id,item_id from goods_attr_item_assign  where goods_id = %s", dp_id)

                    # 获取该商品所有规格值
                    list = cursor.fetchall()
                    if len(list) > 0:
                        attrlist = [str(j['attr_id']) + "_" + str(j['item_id']) for j in list]

                    # 如果规格值有误
                    if len(attrlist) > 0:
                        if attr_config1 not in attrlist:

                            # 查找规格值有误的商品名称
                            cursor.execute("select `name` from goods where id = %s", dp_id)
                            goods_one = cursor.fetchone()
                            if goods_one:
                                goods_name = str(goods_one['name'])

                            # 查找该商品的规格值名称
                            cursor.execute("select `name` from goods_attr_item where id = %s", item_id1)
                            item_one = cursor.fetchone()
                            if item_one:
                                item_name = item_one['name']

                            # 拼接邮件字符串
                            print("商品ID\t", goods_id, "\t里面的",dp_id,goods_name,"\t规格值①错误：", attr_config1,item_name)






                elif attr_length == 2:
                    attr_id2 = content['attr'][1]['attr_id']
                    item_id2 = content['attr'][1]['item_id']
                    attr_config2 = attr_id2 + "_" + item_id2

                    cursor.execute("select * from goods_attr_item_assign where goods_id = %s",dp_id)

                    # 获取该商品所有规格值
                    list = cursor.fetchall()
                    if len(list) > 0:
                        attrlist = [str(j['attr_id']) + "_" + str(j['item_id']) for j in list]

                        # 如果规格值有误
                        if len(attrlist) > 0:
                            if attr_config1 not in attrlist:
                                # 查找规格值有误的商品名称
                                cursor.execute("select `name` from goods where id = %s", dp_id)
                                goods_one = cursor.fetchone()
                                if goods_one:
                                    goods_name = str(goods_one['name'])

                                # 查找该商品的规格值名称
                                cursor.execute("select `name` from goods_attr_item where id = %s", item_id1)
                                item_one = cursor.fetchone()
                                if item_one:
                                    item_name = item_one['name']
                                    # 拼接邮件字符串
                                print("商品ID\t", goods_id, "\t里面的", dp_id, goods_name, "\t规格值①错误：", attr_config1, item_name)


                            if attr_config2 not in attrlist:
                                # 查找规格值有误的商品名称
                                cursor.execute("select `name` from goods where id = %s", dp_id)
                                goods_one = cursor.fetchone()
                                if goods_one:
                                    goods_name = str(goods_one['name'])

                                # 查找该商品的规格值名称
                                cursor.execute("select `name` from goods_attr_item where id = %s", item_id2)
                                item_one = cursor.fetchone()
                                if item_one:
                                    item_name = item_one['name']
                                print("商品ID\t", goods_id, "\t里面的", dp_id, goods_name, "\t规格值②错误：", attr_config2, item_name)





                elif attr_length == 3:

                    attr_id2 = content['attr'][1]['attr_id']
                    item_id2 = content['attr'][1]['item_id']
                    attr_config2 = attr_id2 + "_" + item_id2


                    attr_id3 = content['attr'][2]['attr_id']
                    item_id3 = content['attr'][2]['item_id']
                    attr_config3 = attr_id3 + "_" + item_id3

                    cursor.execute("select * from goods_attr_item_assign where goods_id = %s",dp_id)
                    b = cursor.fetchall()
                    attrlist = [str(j['attr_id'])+"_"+str(j['item_id']) for j in b]

                    # if attr_config1 not in attrlist:
                    #     print("商品ID：",goods_id, ",包含商品ID：", dp_id, ",第一个规格", attr_config1)
                    # if attr_config2 not in attrlist:
                    #     print("商品ID：",goods_id, ",包含商品ID：", dp_id, ",第二个规格", attr_config2)
                    # if attr_config3 not in attrlist:
                    #     print("商品ID：",goods_id, ",包含商品ID：", dp_id, ",第三个规格", attr_config3)

        cursor.close()
        conn.close()

      


# coding=utf-8
import sys
from libs.youzan import ApiClient
from libs.helper import *
import random
import threading
import time
import os
import re
import json
import operator
import urllib
import pandas as pd
import xlwt
api = ApiClient()
db = DBHelper()
default_config = ConfigHelper.getDefault()
conn = db.getConnect()
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

file_path = default_config['toexcel']['Compare_Goods_Attr']+'/Compare_Goods_Attr_'+str(int(time.time()))+'.xls'
df = pd.DataFrame(columns=('id','组合id','商品id','商品名'))
cursor.execute("select * from goods where type=2")
cp_list = cursor.fetchall()
for x in cp_list:
    cursor.execute("select * from goods_cp_config where goods_id=%s",x['id'])
    cp_config = cursor.fetchall()
    for i in cp_config:
        dp_skus = []
        cursor.execute("select * from goods_sku_detail where goods_id=%s",i['dp_goods_id'])
        dp_sku = cursor.fetchall()
        for j in dp_sku:
            dp_skus.append(j['sku_id'])
        cp_dp_content = json.loads(i['content'])
        if cp_dp_content and cp_dp_content['attr']:
            for j in cp_dp_content['attr']:
                if j['attr_id'] not in ['1','2']:
                    cp_dp_sku = j['attr_id']+'_'+j['item_id']
                else:
                    cp_dp_sku = ''
            dp_skus = ','.join(dp_skus)
            if cp_dp_sku and cp_dp_sku not in dp_skus:
                cursor.execute("select * from goods where id = %s",i['dp_goods_id'])
                dp_name = cursor.fetchone()
                df = df.append(pd.DataFrame({'id':i['id'],'组合id':i['goods_id'],'商品id':i['dp_goods_id'],'商品名':dp_name['name']},index=[0]),ignore_index=True)
with pd.ExcelWriter(file_path,engine='xlsxwriter') as writer:
    df.to_excel(writer, index=False, encoding='utf_8_sig')

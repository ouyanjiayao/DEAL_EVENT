# -*- coding:utf-8 -*-
import sys
from libs.helper import *
default_config = ConfigHelper.getDefault()
db_helper = DBHelper()
conn = db_helper.getConnect()
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)


cursor.execute('select id,goods_id,sku_id,sale_price from goods_sku_detail where type = 1')
sku_ids = cursor.fetchall()
for i in sku_ids:
    print(i['goods_id'])
    #添加 no_sku_id
    skus = i['sku_id'].split(':')
    no_handle = []
    no_weight = []
    has_custom = 0
    has_weight = 0
    for j in skus:
        if j.split('_')[0] != '2':
            no_handle.append(j)
            if j.split('_')[0] != '1':
                has_custom = 1
    no_handle_id = ':'.join(no_handle)
    cursor.execute("update goods_sku_detail set no_sku_id='%s' where id = %s"%(no_handle_id,i['id']))
    if no_handle_id:
        #添加 no_handle_price
        cursor.execute("select auto_create_sale_price,price_count_type from goods_attr_config where goods_id=%s "%i['goods_id'])
        attr_config = cursor.fetchone()
        if attr_config['price_count_type'] == 2:
            has_weight = 1
        if has_custom and not has_weight:
            cursor.execute("select cost,sale_scale,sale_price from goods_sku_price where sku_id = '%s' and  goods_id=%s "%(no_handle_id,i['goods_id']))
            sku_price = cursor.fetchone()
            if attr_config['auto_create_sale_price']:
                price = sku_price['cost'] * sku_price['sale_scale']
            else:
                price = sku_price['sale_price']
        if has_weight:
            cursor.execute("select sale_price from goods_sku_system where sku_id = '%s' and  goods_id=%s "%(no_handle_id,i['goods_id']))
            sku_system = cursor.fetchone()
            price = sku_system['sale_price']
        cursor.execute("update goods_sku_detail set no_handle_price='%s' where id = %s"%(price,i['id']))

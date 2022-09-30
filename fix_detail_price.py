import sys
import re
from libs.youzan import ApiClient
from libs.helper import *
from biz.goods_syn import *
from ggd.deal_tag import GetTags
from itertools import combinations

get_tag = GetTags()
default_config = ConfigHelper.getDefault()
db = DBHelper()
api = ApiClient()
conn = db.getConnect()
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

# ['data']['item']['skus']
cursor.execute("select id from `goods` order by id asc")
goods_ids = cursor.fetchall()
for i in goods_ids:
    if i['id']!=1127:
        goods_id = i['id']
    else:
        continue
    cursor.execute("select * from `goods_attr_config` where goods_id=%s", goods_id)
    attr_config = cursor.fetchone()
    cursor.execute("select * from `goods_sku_system` where goods_id=%s", goods_id)
    sku_system = cursor.fetchall()
    cursor.execute("select * from `goods_sku_detail` where goods_id=%s", goods_id)
    sku_detail = cursor.fetchall()

    is_auto = attr_config['auto_create_sale_price']
    is_weight = attr_config['sys_attr_weight']
    is_handle = attr_config['sys_attr_handle']
    is_custom = attr_config['has_custom_attr']
    for sku_ids in sku_detail:
        system_id = []
        sku_id = sku_ids['sku_id']
        attrs = sku_id.split(':')
        weight = 500
        if is_custom:
            if is_weight or is_handle:
                if is_weight:
                    for i in attrs:
                        attr = i.split('_')
                        if attr[0] == '1':
                            cursor.execute("select name from `goods_attr_item` where id=%s",(attr[1]))
                            weight_name = cursor.fetchone()
                            weight = int(re.findall(r'[0-9]+(?=g)',weight_name['name'])[0])
                cursor.execute("select * from `goods_sku_price` where goods_id=%s and sku_id=%s",(goods_id,attrs[0]))
                sku_price = cursor.fetchone()
                cost = sku_price['cost']/500*weight
                for i in combinations(attrs,2):
                    system_id.append(':'.join(i))
                sum_price = 0
                sum_handle = 0
                for i in system_id:
                    cursor.execute("select sale_price,handle_price from `goods_sku_system` where sku_id=%s and goods_id=%s",(i,goods_id))
                    is_sku_id = cursor.fetchone()
                    if is_sku_id:
                        handle_price = is_sku_id['handle_price'] if is_sku_id['handle_price'] else 0
                        sale_price = is_sku_id['sale_price'] if is_sku_id['sale_price'] else 0
                        sum_price += float(sale_price) + float(handle_price)/500*weight
                        sum_handle += float(handle_price)/500*weight
                if not is_weight:
                    sale_price = sku_price['sale_price'] if sku_price['sale_price'] else 0
                    sum_price = float(sale_price) + float(sum_handle)
            else:
                cursor.execute("select sale_price,cost from `goods_sku_price` where sku_id=%s and goods_id=%s",(sku_id,goods_id))
                is_sku_id = cursor.fetchone()
                sum_price = is_sku_id['sale_price']
                cost = is_sku_id['cost']
        else:
            if is_weight or is_handle:
                if is_weight:
                    for i in attrs:
                        attr = i.split('_')
                        if attr[0] == '1':
                            cursor.execute("select name from `goods_attr_item` where id=%s",(attr[1]))
                            weight_name = cursor.fetchone()
                            weight = int(re.findall(r'[0-9]+(?=g)',weight_name['name'])[0])
                cost = attr_config['cost']/500*weight
                for i in combinations(attrs,1):
                    system_id.append(':'.join(i))
                sum_price = 0
                sum_handle = 0
                for i in system_id:
                    cursor.execute("select sale_price,handle_price from `goods_sku_system` where sku_id=%s and goods_id=%s",(i,goods_id))
                    is_sku_id = cursor.fetchone()
                    if is_sku_id:
                        handle_price = is_sku_id['handle_price'] if is_sku_id['handle_price'] else 0
                        sale_price = is_sku_id['sale_price'] if is_sku_id['sale_price'] else 0
                        sum_price += float(sale_price) + float(handle_price)/500*weight
                        sum_handle += float(handle_price)/500*weight
                if not is_weight:
                    sale_price = attr_config['sale_price'] if attr_config['sale_price'] else 0
                    sum_price = float(sale_price)/500*weight + float(sum_handle)
        cursor.execute("update `goods_sku_detail` set cost=%s,sale_price=%s where goods_id=%s and sku_id=%s",('%.2f' % cost,'%.2f' % sum_price,goods_id,sku_id))
        print(goods_id,'%.2f' % cost,'%.2f' % sum_price)

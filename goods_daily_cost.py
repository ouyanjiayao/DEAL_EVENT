from libs.helper import *
import time

db = DBHelper()
conn = db.getConnect()
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

now = int(time.time())
cursor.execute("INSERT INTO goods_daily_cost ( goods_id, goods_name, sku_id, goods_cost ) SELECT goods.id,goods.NAME,goods_sku_price.sku_id,goods_sku_price.cost FROM goods JOIN goods_sku_price WHERE goods_sku_price.goods_id = goods.id AND goods.type = 1 and goods.is_delete = 0")
cursor.execute("INSERT INTO goods_daily_cost ( goods_id, goods_name, goods_cost ) SELECT goods.id,goods.NAME,goods_attr_config.cost FROM goods JOIN goods_attr_config WHERE goods_attr_config.goods_id = goods.id AND goods_attr_config.cost IS NOT NULL AND goods_attr_config.cost != 0 AND goods.type = 1 and goods.is_delete = 0")
cursor.execute("UPDATE goods_daily_cost SET price_count_type = ( SELECT price_count_type FROM goods_attr_config WHERE goods_id = goods_daily_cost.goods_id ),update_time=%s",now)
cursor.execute("select id,sku_id from goods_daily_cost where sku_id is not null")
skulist = cursor.fetchall()
for i in skulist:
    ilist = i['sku_id'].split(':')
    sku_name = ''
    for j in ilist:
        sku = j.split('_')
        attr = sku[0]
        item = sku[1]
        cursor.execute("select name from goods_attr where id=%s",attr)
        attr_name = cursor.fetchone()['name']
        cursor.execute("select name from goods_attr_item where id=%s",item)
        item_name = cursor.fetchone()['name']
        sku_name += attr_name + ':' + item_name + ' '
    cursor.execute("update goods_daily_cost set spec_name=%s where id=%s",(sku_name,i['id']))
        
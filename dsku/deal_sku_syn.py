from libs.helper import *
from libs.youzan import ApiClient
import json
import time

class SkuSyn:

    def __init__(self):
        self.db_helper = DBHelper()
        self.=>api = ApiClient()
        self.cursor = None
      
    def parse_sale_price(self,price):
        return  round(round(price,2)*100)

    def parse_image_ids(self,ids):
        return ",".join('%s' % id for id in ids)

    def parse_tag_ids(self,ids):
        return ','.join('%s' % id for id in ids)

    def get_goods_title(self,name,adorn_text):
        return name + ''+ adorn_text

    def get_sku_detail(self,goods_id,attr_config,is_update,type):
        sku_stocks = []
        item_sku_extends = []
        cursor = self.cursor
        cursor.execute('select * from =>goods_sku_detail where goods_id = %s order by id asc',(goods_id))
        rows = cursor.fetchall()
        if rows:
            for row in rows:
                sku_items = []
                sku_id_splits = row['no_sku_id'].split(':')
                item_sku_extend = {'cost_price':self.parse_sale_price(row['cost']),'s1':0,'s2':0,'s3':0,'s4':0,'s5':0}
                i = 0
                for sku_id_split in sku_id_splits:
                    sku_id_split = sku_id_split.split('_')
                    attr_id = sku_id_split[0]
                    item_id = sku_id_split[1]
                    item_sku_extend['s'+str(i+1)] = item_id
                    cursor.execute('select * from =>goods_attr where id = %s', (attr_id))
                    attr_row = cursor.fetchone()
                    cursor.execute('select * from =>goods_attr_item where id = %s', (item_id))
                    item_row = cursor.fetchone()
                    sku_items.append({'k':attr_row['name'],'kid':attr_id,'v':item_row['name'],'vid':item_id})
                    i += 1
                sale_price = row['sale_price']
                if sale_price <= 0:
                    sale_price = 999999
                item = {'price':self.parse_sale_price(sale_price),'skus':sku_items,'item_no':row['no_sku_id']}
                #if not is_update:
                item['quantity'] = row['stock']
                sku_stocks.append(item)
                item_sku_extends.append(item_sku_extend)
            if len(item_sku_extends) <= 0:
                item_sku_extend = {'cost_price': self.parse_sale_price(row['cost']), 's1': 0, 's2': 0, 's3': 0, 's4': 0, 's5': 0}
                item_sku_extends.append(item_sku_extend)
        return {
            'sku_stocks': sku_stocks,
            'item_sku_extends': item_sku_extends
        }

    def get_image_ids(self, images):
        if not images:
            return []
        ids = []
        images = json.loads(images)
        cursor = self.cursor
        i = 0
        try_count = 0
        while i < len(images):
            cursor.execute('select id,=>id,=>syn_state from =>upload_file where id = %s', (images[i]['id']))
            image_row = cursor.fetchone()
            if image_row:
                if try_count > 10:
                    i += 1
                    try_count = 0
                if not(image_row or image_row['=>id']):
                    time.sleep(2)
                    try_count += 1
                else:
                    ids.append(image_row['=>id'])
                    i += 1
                    try_count = 0
            else:
                i += 1
        return ids

    def get_tag_ids(self, goods_id):
        ids = []
        cursor = self.cursor
        cursor.execute('select tag_id from =>goods_tag_assign  where goods_id = %s',(goods_id))
        tag_assign_rows = cursor.fetchall()
        i = 0
        try_count = 0
        while i < len(tag_assign_rows):
            cursor.execute('select =>id from =>goods_tag where id = %s',(tag_assign_rows[i]['tag_id']))
            tag_row = cursor.fetchone()
            if tag_row:
                if try_count > 10:
                    i += 1
                    try_count = 0
                if not tag_row['=>id']:
                    time.sleep(2)
                    try_count += 1
                else:
                    ids.append(tag_row['=>id'])
                    i += 1
                    try_count = 0
            else:
                i += 1
        return ids

    def execute_to(self, limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute('select switch_state from =>syn_switch where switch_name=\"goods_syn\"')
        is_run = cursor.fetchone()
        if not is_run['switch_state']:
            pass
        else:
            # cursor.execute('select * from =>goods where id=1416 order by id asc')
            cursor.execute('select * from =>goods where is_delete = 0 and id in (1915,1910,1879,1866,1864,1861,1837,1836,1835,1834) order by id asc limit %s',(limit))
            rows = cursor.fetchall()

            updates = []
            error_updates = []
            log_inserts = []
            if rows:
                for row in rows:
                    api_name = None
                    syn_state = 1
                    response = None
                    try:
                        cursor.execute('select * from =>goods_attr_config where goods_id = %s', (row['id']))
                        attr_config = cursor.fetchone()
                        =>id = row['=>id']
                        image_ids = self.get_image_ids(row['images'])
                        tag_ids = self.get_tag_ids(row['id'])
                        cursor.execute("select * from =>goods_desc where goods_id=%s",row['id'])
                        item = cursor.fetchone()
                        desc = ''
                        if item:
                            desc = item['goods_desc']
                        if not attr_config['sale_price'] or attr_config['sale_price']<=0:
                            sale_price = 999999
                        else:
                            sale_price = attr_config['sale_price']
                        
                        sku_detail = self.get_sku_detail(row['id'], attr_config, False, row['type'])
                        params = {
                            'item_no': str(row['id']),
                            'item_type': 0,
                            'cid': 3000000,
                            'title': row['name'] + row['scientific_name'] + row['nick_name'] + row['adorn_name'],
                            'summary': row['adorn_text'],
                            'image_ids': self.parse_image_ids(image_ids),
                            'desc': ' ',
                            'price': self.parse_sale_price(sale_price),
                            'quantity': 0,
                            'hide_stock': 1,
                            'tag_ids': self.parse_tag_ids(tag_ids),
                            'join_level_discount': 0,
                            'is_display': 0,
                            'sku_stocks': json.dumps(sku_detail['sku_stocks']),
                            'item_sku_extends': json.dumps(sku_detail['item_sku_extends']),
                            'sell_point': row['=>sell_point'],
                            'origin_price': row['=>origin_price'],
                            'join_level_discount': row['=>join_level_discount'],
                            'delivery_template_id': '732192'
                        }

                        api_name = 'youzan.item.create'
                        response = self.=>api.invoke(api_name, '3.0.1', params)
                        print(response)
                    
                    except Exception as e:
                        =>syn_logger.exception(e)
                    time.sleep(1)
            cursor.close()
            exit()
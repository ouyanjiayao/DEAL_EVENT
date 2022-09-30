from libs.helper import *
from libs.youzan import ApiClient
import json
import time


def get_tc_config_price(cursor,goods_id,sku_detail_id,attr_config):
    cost = 0
    cursor.execute('select * from goods_tc_config where goods_id = %s and sku_detail_id = %s order by id asc',
                   (goods_id, sku_detail_id))
    s_tc_configs = cursor.fetchall()
    for s_tc_config in s_tc_configs:
        cp_config_content = json.loads(s_tc_config['content'])
        if not cp_config_content['attr']:
            cost += 0
        else:
            s_sku_id = []
            for cp_config_content_attr in cp_config_content['attr']:
                cursor.execute('select * from goods_attr where id = %s',
                               (cp_config_content_attr['attr_id']))
                attr_t = cursor.fetchone()
                if attr_t and attr_t['type'] == 1:
                    s_sku_id.append(cp_config_content_attr['attr_id'] + '_' + cp_config_content_attr['item_id'])
                else:
                    s_sku_id = []
            s_sku_id = ':'.join(s_sku_id)
            cursor.execute('select * from goods_attr_config where goods_id = %s', (s_tc_config['dp_goods_id']))
            s_attr_config = cursor.fetchone()
            cursor.execute('select * from goods_sku_detail where goods_id = %s and sku_id = %s',
                           (s_tc_config['dp_goods_id'], s_sku_id))
            s_sku_detail = cursor.fetchone()
            if s_sku_detail:
                s_price = get_cp_config_price(cursor, s_tc_config['dp_goods_id'], s_sku_detail['id'], s_attr_config)
            else:
                cursor.execute("select cost from goods_sku_detail where goods_id=%s",s_tc_config['dp_goods_id'])
                s_price = cursor.fetchone()
            if not s_price:
                cost += 0
            else:
                cost += float(s_price['cost'])
    if attr_config['auto_create_sale_price']==1:
        return {'cost': float('%.2f'%cost), 'sale_price': float('%.2f'%(cost * float(attr_config['sale_scale'])))}
    else:
        cursor.execute('select * from goods_sku_detail where goods_id = %s and id=%s', (goods_id,sku_detail_id))
        cp_sale_price = cursor.fetchone()
        return {'cost': float('%.2f'%cost), 'sale_price': float('%.2f'%float(cp_sale_price['sale_price']))}

def get_cp_config_price(cursor,goods_id,sku_detail_id,attr_config):
    cost = 0
    h_costs = 0
    handle_sku_price = []
    cursor.execute('select * from goods_cp_config where goods_id = %s and sku_detail_id = %s order by id asc',
                   (goods_id, sku_detail_id))
    cp_config_rows = cursor.fetchall()
    if cp_config_rows:
        for cp_config_row in cp_config_rows:
            cp_config_content = json.loads(cp_config_row['content'])
            cursor.execute('select * from goods_attr_config where goods_id = %s', (cp_config_row['dp_goods_id']))
            cp_config_row_attr_config = cursor.fetchone()
            d_cost = 0
            h_cost = 0
            if not cp_config_row_attr_config['cost']:
                cp_config_row_attr_config['cost'] = 0
            if not cp_config_content['attr']:
                d_cost = float(cp_config_row_attr_config['cost'])
            else:
                s_sku_id = []
                sku_jg_ids = []
                for cp_config_content_attr in cp_config_content['attr']:
                    sku_jg_id = []
                    cursor.execute('select * from goods_attr where id = %s', (cp_config_content_attr['attr_id']))
                    attr_t = cursor.fetchone()
                    if attr_t and attr_t['type'] == 1:
                        s_sku_id.append(cp_config_content_attr['attr_id'] + '_' + cp_config_content_attr['item_id'])
                    sku_jg_id.append(cp_config_content_attr['attr_id'] + '_' + cp_config_content_attr['item_id'])
                    sku_jg_ids = s_sku_id+sku_jg_id
                s_sku_id = ':'.join(s_sku_id)
                cursor.execute('select * from goods_sku_price where goods_id = %s and sku_id = %s',
                               (cp_config_row['dp_goods_id'], s_sku_id))
                s_sku_price = cursor.fetchone()
                sku_jg_id = ':'.join(sku_jg_ids)
                cursor.execute('select * from goods_sku_system where goods_id = %s and sku_id = %s and type=2',
                               (cp_config_row['dp_goods_id'], sku_jg_id))
                handle_sku_price = cursor.fetchone()
                if not handle_sku_price:
                    h_cost = 0
                else:
                    h_cost = float(handle_sku_price['handle_price'])
                if not s_sku_price:
                    d_cost = float(cp_config_row_attr_config['cost'])
                else:
                    d_cost = float(s_sku_price['cost'])
            if cp_config_row_attr_config['price_count_type'] == 1:
                cost += d_cost * float(cp_config_content['unit'])
                h_costs += h_cost * float(cp_config_content['unit'])
            elif cp_config_row_attr_config['price_count_type'] == 2:
                cost += (d_cost / 500) * float(cp_config_content['unit'])
                h_costs += (h_cost / 500) * float(cp_config_content['unit'])
    if attr_config['auto_create_sale_price']==1:
        return {'cost': float('%.2f'%cost), 'sale_price': float('%.2f'%(cost * float(attr_config['sale_scale']) + h_costs))}
    else:
        cursor.execute('select * from goods_sku_detail where goods_id = %s and id=%s', (goods_id,sku_detail_id))
        cp_sale_price = cursor.fetchone()
        return {'cost': float('%.2f'%cost), 'sale_price': float('%.2f'%float(cp_sale_price['sale_price']))}

class GoodsSyn:

    def __init__(self):
        self.db_helper = DBHelper()
        self.api = ApiClient()
        self.cursor = None
      

    def parse_sale_price(self,price):
        return  round(round(price,2)*100)

    def parse_image_ids(self,ids):
        return ",".join('%s' % id for id in ids)

    def parse_tag_ids(self,ids):
        return ','.join('%s' % id for id in ids)

    def get_goods_title(self,name,scientific_name,nick_name,adorn_name):
        return name + ''+scientific_name + ''+nick_name+''+adorn_name

    def get_sku_detail(self,goods_id,attr_config,is_update,type):
        sku_stocks = []
        item_sku_extends = []
        cursor = self.cursor
        cursor.execute('select * from goods_sku_detail where goods_id = %s and no_sku_id <> "" order by id asc',(goods_id))
        rows = cursor.fetchall()
        if rows:
            dup = {}
            for row in rows:
                if row['no_sku_id'] not in dup.keys():
                    dup[row['no_sku_id']] = row['stock']
                else:
                    dup[row['no_sku_id']] += row['stock']
                    continue
                if type == 2:
                    config_price = get_cp_config_price(cursor,goods_id,row['id'],attr_config)
                    row['cost'] = config_price['cost']
                    if attr_config['auto_create_sale_price'] == 1:
                        row['sale_price'] = config_price['sale_price']
                if type == 3:
                    config_price = get_tc_config_price(cursor, goods_id, row['id'], attr_config)
                    row['cost'] = config_price['cost']
                    if attr_config['auto_create_sale_price'] == 1:
                        row['sale_price'] = config_price['sale_price']
                sku_items = []
                sku_id_splits = row['no_sku_id'].split(':')
                item_sku_extend = {'cost_price':self.parse_sale_price(row['cost']),'s1':0,'s2':0,'s3':0,'s4':0,'s5':0}
                i = 0
                for sku_id_split in sku_id_splits:
                    sku_id_split = sku_id_split.split('_')
                    attr_id = sku_id_split[0]
                    item_id = sku_id_split[1]
                    item_sku_extend['s'+str(i+1)] = item_id
                    cursor.execute('select * from goods_attr where id = %s', (attr_id))
                    attr_row = cursor.fetchone()
                    cursor.execute('select * from goods_attr_item where id = %s', (item_id))
                    item_row = cursor.fetchone()
                    sku_items.append({'k':attr_row['name'],'kid':attr_id,'v':item_row['name'],'vid':item_id})
                    i += 1
                sale_price = row['no_handle_price']
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
            cursor.execute('select id,id,syn_state from upload_file where id = %s', (images[i]['id']))
            image_row = cursor.fetchone()
            if image_row:
                if try_count > 10:
                    i += 1
                    try_count = 0
                if not(image_row or image_row['id']):
                    time.sleep(2)
                    try_count += 1
                else:
                    ids.append(image_row['id'])
                    i += 1
                    try_count = 0
            else:
                i += 1
        return ids

    def get_tag_ids(self, goods_id):
        ids = []
        cursor = self.cursor
        cursor.execute('select tag_id from goods_tag_assign  where goods_id = %s',(goods_id))
        tag_assign_rows = cursor.fetchall()
        i = 0
        try_count = 0
        while i < len(tag_assign_rows):
            cursor.execute('select id from goods_tag where id = %s',(tag_assign_rows[i]['tag_id']))
            tag_row = cursor.fetchone()
            if tag_row:
                if try_count > 10:
                    i += 1
                    try_count = 0
                if not tag_row['id']:
                    time.sleep(2)
                    try_count += 1
                else:
                    ids.append(tag_row['id'])
                    i += 1
                    try_count = 0
            else:
                i += 1
        return ids

    def execute_to(self, limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute('select switch_state from syn_switch where switch_name=\"goods_syn\"')
        is_run = cursor.fetchone()
        is_mirror = True
        if not is_run['switch_state']:
            pass
        else:
            # cursor.execute('select * from goods where id=1416 order by id asc')
            cursor.execute('select * from goods where is_delete = 0 and (version is null or version!=version) and auto_syn=1 and syn = 1 and type = 1 order by id asc limit %s',(limit))
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
                        cursor.execute('select * from goods_attr_config where goods_id = %s', (row['id']))
                        attr_config = cursor.fetchone()
                        id = row['id']
                        image_ids = self.get_image_ids(row['images'])
                        tag_ids = self.get_tag_ids(row['id'])
                        cursor.execute("select * from goods_desc where goods_id=%s",row['id'])
                        item = cursor.fetchone()
                        desc = '<p></p>'
                        if item:
                            desc = item['goods_desc']
                        if not attr_config['sale_price'] or attr_config['sale_price']<=0:
                            sale_price = 999999
                        else:
                            sale_price = attr_config['sale_price']
                        if not row['id']:
                            sku_detail = self.get_sku_detail(row['id'], attr_config, False, row['type'])
                            params = {
                                'item_no': str(row['id']),
                                'item_type': 0,
                                'cid': 3000000,
                                'title': row['name'] + row.get('scientific_name','') + row.get('nick_name','') + row.get('adorn_name',''),
                                'summary': row['adorn_text'],
                                'image_ids': self.parse_image_ids(image_ids),
                                'desc': desc,
                                'price': self.parse_sale_price(sale_price),
                                'quantity': attr_config['stock'],
                                'hide_stock': 1,
                                'tag_ids': self.parse_tag_ids(tag_ids),
                                'is_display': row['state'],
                                'sku_stocks': json.dumps(sku_detail['sku_stocks']),
                                'item_sku_extends': json.dumps(sku_detail['item_sku_extends']),
                                'sell_point': row['sell_point'],
                                'origin_price': row['origin_price'],
                                'join_level_discount': row['join_level_discount'],
                                'delivery_template_id': '732192'
                            }
                            api_name = 'youzan.item.create'
                            response = self.api.invoke(api_name, '3.0.1', params,mirror=is_mirror)
                            if not (response['code'] == 200):
                                syn_state = 2
                            else:
                                id = response['data']['item']['item_id']
                                syn_state = 3
                              
                        else:
                            sku_detail = self.get_sku_detail(row['id'], attr_config, True, row['type'])
                            data = self.api.invoke('youzan.item.get','3.0.0',{
                                'item_id':row['id']
                            },mirror=is_mirror)
                            data_imgs = [i['id'] for i in data['data']['item']['item_imgs']]
                            row['image_ids'] = str(data_imgs)[1:-1].replace(" ","")
                            params = {
                                'item_id': row['id'],
                                'item_no': str(row['id']),
                                'title': row['name'] + row.get('scientific_name', '') + row.get('nick_name','')+ row.get('adorn_name', ''),
                                'summary': row['adorn_text'],
                                'image_ids': self.parse_image_ids(image_ids),
                                'desc': desc,
                                'quantity':attr_config['stock'],
                                'price': self.parse_sale_price(sale_price),
                                'tag_ids': self.parse_tag_ids(tag_ids),
                                'is_display': row['state'],
                                'remove_image_ids': row['image_ids'],
                                'sku_stocks': json.dumps(sku_detail['sku_stocks']),
                                'item_sku_extends': json.dumps(sku_detail['item_sku_extends']),
                                'sell_point': row['sell_point'],
                                'origin_price': row['origin_price'],
                                'join_level_discount': row['join_level_discount']
                            }
                            api_name = 'youzan.item.update'
                            response = self.api.invoke(api_name, '3.0.1', params,mirror=is_mirror)
                            if not (response['code'] == 200):
                                syn_state = 2
                            else:
                                syn_state = 3
                        updates.append((id, params['image_ids'], row['version'], time.time(), syn_state, row['id']))
                    except Exception as e:
                        syn_logger.exception(e)

                    if syn_state == 1:
                        error_updates.append((time.time(), syn_state, row['id']))
                    if not response:
                        response_content = '接口调用失败'
                    else:
                        response_content = json.dumps(response, ensure_ascii=False)
                    if not row['updated_id']:
                        row['updated_id'] = 0
                    log_inserts.append((response_content,time.time(),1,row['id'],syn_state,api_name,row['updated_id']))
                    time.sleep(1)
                
                if len(updates) > 0:
                    cursor.executemany("update goods set id = %s,image_ids = %s,version = %s,syn_time = %s,syn_state = %s where id = %s",updates)
                if len(error_updates) > 0:
                    cursor.executemany("update goods set syn_time = %s, syn_state = %s where id = %s",error_updates)
                if len(log_inserts) > 0:
                    cursor.executemany("insert into syn_log(response_content,created_time,type,syn_id,syn_state,api_name,updated_id) values(%s,%s,%s,%s,%s,%s,%s)",log_inserts)
                cursor.close()

    def execute_delete(self,limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute('select switch_state from syn_switch where switch_name=\"delete_goods_syn\"')
        is_run = cursor.fetchone()
        if not is_run['switch_state']:
            pass
        else:
            cursor.execute('select id,id,updated_id from goods where is_delete = 1 and auto_syn=1 order by id asc limit %s',(limit))
            rows = cursor.fetchall()
            deletes = []
            log_inserts = []
            if rows:
                for row in rows:
                    api_name = None
                    response = None
                    syn_state = 1
                    deletes = []
                    response_content = None
                    try:
                        if not row['id']:
                            deletes.append((row['id']))
                        else:
                            api_name = 'youzan.item.delete'
                            response = self.api.invoke(api_name, '3.0.0', {
                                'item_id': row['id']
                            })
                            if not (response['code'] == 200):
                                syn_state = 2
                            else:
                                deletes.append((row['id']))
                                syn_state = 3
                    except Exception as e:
                        syn_logger.exception(e)
                    if not response:
                        response_content = '接口调用失败'
                    else:
                        response_content = json.dumps(response, ensure_ascii=False)
                    log_inserts.append((response_content, time.time(), 1, row['id'], syn_state, api_name, row['updated_id']))
                cursor.executemany('delete from goods where id = %s', deletes)
                if len(log_inserts) > 0:
                    cursor.executemany(
                        "insert into syn_log(response_content,created_time,type,syn_id,syn_state,api_name,updated_id) values(%s,%s,%s,%s,%s,%s,%s)",
                        log_inserts)
                cursor.close()

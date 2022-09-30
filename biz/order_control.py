from libs.print import *
from libs.helper import *
import time
import json
import sys
import re

class OrderPrint:

    #type=1TOTAL打印机 type=2BRANCH打印机
    def __init__(self,type):
        self.db_helper = DBHelper()
        default_config = ConfigHelper.getDefault()
        device_id = None
        if type == 1:
            device_id = default_config['print']['zt_device_id']
        elif type == 2:
            device_id = default_config['print']['fk_device_id']
        self.print_device = PrintDevice(default_config['print']['id'],default_config['print']['secret'],device_id)
        self.type = type

    def execute(self, limit):
        print_state_column = ''
        print_time_column = ''
        if self.type == 1:
            print_state_column = 'zt_print_state'
            print_time_column = 'zt_print_time'
        elif self.type == 2:
            print_state_column = 'fk_print_state'
            print_time_column = 'fk_print_time'
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute('select * from order where '+print_state_column+' = 0 and order_state > 1 and order_num is not null order by id asc limit %s', (limit))
        rows = cursor.fetchall()
        updates = []
        for row in rows:
            state = 2
            try:
                order_data = self.generate_order_data(row)
                print_content = self.generate_print_content(self.type,order_data,row)
                self.print_device.printContent(print_content,row['id'])
                if self.type == 2:
                    print_content = self.generate_print_content(3, order_data, row)
                    self.print_device.printContent(print_content, row['id'])
                    print_content = self.generate_print_content(4, order_data, row)
                    self.print_device.printContent(print_content, row['id'])
            except Exception as e:
                state = 1
                order_control_logger.exception(e)
            updates.append((state,time.time(),row['id']))
            time.sleep(1)
        if len(updates) > 0:
            cursor.executemany('update order set '+print_state_column+' = %s,'+print_time_column+' = %s where id = %s', updates)


    def generate_print_content(self,type,order_data,row):
        title = None
        if type == 1:
            title = 'TOTAL'
        elif type == 2:
            title = 'BRANCH单'
        elif type == 3:
            title = 'Delivery'
        elif type == 4:
            title = 'PURCHASE'
        params = order_data.copy()
        params['title'] = title
        content = ''
        content += '   <FB> TITLE</FB>\r\n\r\n<FB>'
        content += '             {title}</FB>\r\n\r\n'
        content += '<FS><FB>ORDERNUM:{order_num}</FB></FS>\r\n\r\n'
        if 'delivery_time' in params.keys():
            content += '<FS><FB>{delivery_time_label}:\r\n{delivery_time}</FB></FS>\r\n\r\n'
        content += '<FS><FB>买家留言:\r\n{buyer_message}</FB></FS>\r\n\r\n'
        content += 'ORDERNUM:{tid}\r\n'
        content += '联系人:{receiver_name}\r\n'
        content += '昵称:{nickname}\r\n'
        content += '联系方式:{receiver_tel}\r\n'
        content += '下单时间:{created_time}\r\n'
        content += '收货方式:{express_type_label}\r\n'
        content += ':{receiver_address_detail}\r\n\r\n\r\n'
        content += '*********** <FB>商品</FB> ***********\r\n\r\n'
        content += '<table>'
        content += '<tr><td>商品</td><td> </td><td>小计</td></tr><tr><td> </td><td> </td><td></td></tr>'
        for detail in order_data['details']:
            detail_desc = ''
            if 'sku_properties_name' in detail.keys():
                detail_desc = []
                for sku_properties_name in detail['sku_properties_name']:
                    detail_desc.append(sku_properties_name['k']+':'+sku_properties_name['v'])
                detail_desc = ','.join(detail_desc)
            if detail['print_config']:
                detail_desc += '\r\n '+ detail['print_config']
            detail['detail_desc'] = detail_desc
            content += '<tr><td>{title} <FS>x{num}</FS></td><td> </td><td>￥{price}</td></tr><tr><td>{detail_desc}</td></tr>'.format(**detail)
            if not detail['detail_desc']:
                content += '<tr><td> </td><td> </td><td></td></tr>'
        content += '</table>'
        if len(order_data['cp_config'])>0:
            content += '\r\n\r\n'
            content += '*********** <FB>组合</FB> ***********\r\n\r\n'
            cp_item_contents = []
            for cp_config in order_data['cp_config']:
                c_dp_item_contents = []
                c_content = ''
                c_content += '<FB>'+cp_config['goods_name']+'</FB>\r\n'+cp_config['g_detail_name']+'\r\n\r\n'
                for c_dp_config in cp_config['dp_config']:
                    c_dp_item_content = '- '+c_dp_config['dp_name']+' x'+str(c_dp_config['count']) +'\r\n'
                    c_dp_item_content += c_dp_config['desc']
                    c_dp_item_contents.append(c_dp_item_content)
                c_content += '\r\n\r\n'.join(c_dp_item_contents)
                cp_item_contents.append(c_content)
            cp_item_contents = '\r\n\r\n--------------------------------\r\n\r\n'.join(cp_item_contents)
            content += cp_item_contents
            content += '\r\n'
        if len(order_data['tc_config']) > 0:
            content += '\r\n\r\n'
            content += '*********** <FB>礼包</FB> ***********\r\n\r\n'
            tc_item_contents = []
            for tc_config in order_data['tc_config']:
                cp_item_contents = []
                tc_item_content = ''
                tc_item_content += '<FS>{tc_name}</FS>\r\n'+tc_config['g_detail_name']+'\r\n\r\n'
                for tc_config_content in tc_config['tc_config']:
                    cp_item_content = '<FB>{cp_name}</FB>\r\n\r\n'
                    dp_item_contents = []
                    for dp_config_content in tc_config_content['dp_config']:
                        if 'sku_id' in dp_config_content.keys() and dp_config_content['sku_id'] != tc_config[
                            'tc_sku_id']:
                            continue
                        dp_item_content = '- {dp_name} x{count}'
                        if dp_config_content['desc']:
                            dp_item_content += '\r\n{dp_desc}'
                        dp_item_contents.append(dp_item_content.format(
                            **{'dp_name': dp_config_content['dp_name'], 'count': dp_config_content['count'],
                               'dp_desc': dp_config_content['desc']}))
                    dp_item_contents = '\r\n\r\n'.join(dp_item_contents)
                    cp_item_content += dp_item_contents
                    cp_item_contents.append(cp_item_content.format(
                        **{'cp_name': tc_config_content['cp_name'], 'count': tc_config_content['count']}))
                cp_item_contents = '\r\n\r\n--------------------------------\r\n\r\n'.join(cp_item_contents)
                tc_item_content += cp_item_contents
                tc_item_contents.append(tc_item_content.format(**{'tc_name': tc_config['tc_name']}))
            tc_item_contents = '\r\n\r\n--------------------------------\r\n\r\n'.join(tc_item_contents)
            content += tc_item_contents
            content += '\r\n'
        content += '\r\n********************************\r\n\r\n'
        content += '<FS><FB>总计:￥{total_fee}</FB></FS>\r\n\r\n'
        content += '配送费:￥{post_fee}\r\n'
        content += '优惠金额:￥{order_discount_fee}\r\n\r\n'
        content += '<FS><FB>实付:￥{payment}</FB></FS>'
        content += '\r\n\r\n\r\n\r\n\r\n\r\n\r\n'
        if type == 2:
            content += '<center>扫码发货↓</center><QR>{url}</QR>\r\n\r\n'.format(
                **{'url': 'https://vc.sto2c.com/youzan/api/logistics-confirm?id=' + str(row['id'])})
        return content.format(**params)

    def generate_order_data(self,row):
        cursor = self.cursor
        order_data = {}
        order_data['order_num'] = row['order_num']
        push_content = json.loads(row['push_content'])
        express_type_label = None
        delivery_time = None
        if push_content['full_order_info']['order_info']['express_type'] == 0:
            express_type_label = '快递发货'
        elif push_content['full_order_info']['order_info']['express_type'] == 1:
            express_type_label = '到店自提'
            dbtime = time.strftime('%Y-%m-%d %H:%M',time.strptime(push_content['full_order_info']['address_info']['delivery_start_time'],'%Y-%m-%d %H:%M:%S'))
            betime = time.strftime('%H:%M',time.strptime(push_content['full_order_info']['address_info']['delivery_end_time'],'%Y-%m-%d %H:%M:%S'))
            delivery_time = dbtime + '-' + betime
            order_data['delivery_time_label'] = '自提时间'
        elif push_content['full_order_info']['order_info']['express_type'] == 2:
            express_type_label = '同城配送'
            dbtime = time.strftime('%Y-%m-%d %H:%M', time.strptime(push_content['full_order_info']['address_info']['delivery_start_time'], '%Y-%m-%d %H:%M:%S'))
            betime = time.strftime('%H:%M', time.strptime(push_content['full_order_info']['address_info']['delivery_end_time'], '%Y-%m-%d %H:%M:%S'))
            delivery_time = dbtime + '-' + betime
            order_data['delivery_time_label'] = '配送时间'
        order_data['express_type_label'] = express_type_label
        if delivery_time:
            order_data['delivery_time'] = delivery_time
        order_data['tid'] = push_content['full_order_info']['order_info']['tid']
        order_data['receiver_name'] = push_content['full_order_info']['address_info']['receiver_name']
        order_data['nickname'] = push_content['full_order_info']['buyer_info']['fans_nickname']
        order_data['receiver_tel'] = push_content['full_order_info']['address_info']['receiver_tel']
        order_data['receiver_address_detail'] = push_content['full_order_info']['address_info']['delivery_province'] + \
                          push_content['full_order_info']['address_info']['delivery_city'] + \
                          push_content['full_order_info']['address_info']['delivery_district'] + ' ' + \
                          push_content['full_order_info']['address_info']['delivery_address']
        order_data['buyer_message'] = push_content['full_order_info']['remark_info']['buyer_message']
        order_data['order_discount_fee'] = 0
        if 'item_discount_fee' in push_content['order_promotion'].keys():
            order_data['order_discount_fee'] += float(push_content['order_promotion']['item_discount_fee'])
        if 'order_discount_fee' in push_content['order_promotion'].keys():
            order_data['order_discount_fee'] += float(push_content['order_promotion']['order_discount_fee'])
        order_data['post_fee'] = push_content['full_order_info']['pay_info']['post_fee']
        order_data['total_fee'] = push_content['full_order_info']['pay_info']['total_fee']
        order_data['payment'] = push_content['full_order_info']['pay_info']['payment']
        order_data['created_time'] = time.strftime('%Y-%m-%d %H:%M', time.localtime(
            time.mktime(time.strptime(push_content['full_order_info']['order_info']['created'], "%Y-%m-%d %H:%M:%S"))))
        details = []
        order_data['cp_config'] = []
        order_data['tc_config'] = []
        tc_config_code_ids = []
        cp_config_code_ids = []
        for order in push_content['full_order_info']['orders']:
            cursor.execute('select * from goods where id = %s',(order['outer_item_id']))
            goods_row = cursor.fetchone()
            #cursor.execute('select * from goods_tc_config where code_id = %s',(order['outer_item_id']))
            #tc_row = cursor.fetchone()
            detail = {}
            detail['title'] = order['title']
            detail['print_config'] = ''
            if goods_row:
                detail['title'] = goods_row['name']
                detail['print_config'] = goods_row['print_config']
            #if tc_row and tc_row['id'] not in tc_config_code_ids:
            #    tc_config_code_ids.append(tc_row['id'])
            #    tc_row['content'] =  json.loads(tc_row['content'])
            #    order_data['tc_config'].append({'tc_name':detail['title'],'tc_config':tc_row['content'],'tc_sku_id':order['outer_sku_id']})
            detail['num'] = order['num']
            detail['price'] = order['num'] * float(order['price'])
            #detail['price'] = order['payment']
            sku_properties_name = []
            cursor.execute('select * from goods_sku_detail where goods_id = %s and sku_id = %s', (order['outer_item_id'],order['outer_sku_id']))
            sku_detail_row = cursor.fetchone()

            if order['sku_properties_name']:
                order['sku_properties_name'] = json.loads(order['sku_properties_name'])
                for sku_item in order['sku_properties_name']:
                    if sku_item['k'] == '重量':
                        sku_item['k'] = '单重'
                    sku_properties_name.append({'k': sku_item['k'], 'v': sku_item['v']})

                    if detail['num'] > 1 and sku_item['k'] == '单重':
                        sku_item['k'] = '总重'
                        sku_item['val'] = self.deal_weight(sku_item['v'])
                        sku_item['total'] = str(sku_item['val'] * order['num'])

                        sku_properties_name.append({'k': sku_item['k'], 'v': sku_item['total']})

            if len(sku_properties_name)>0:
                detail['sku_properties_name'] = sku_properties_name
            if goods_row and goods_row['type'] == 2:
                g_detail_name = []
                if sku_properties_name:
                    for sn in sku_properties_name:
                        g_detail_name.append(sn['k']+':'+sn['v'])
                g_detail_name = ','.join(g_detail_name)
                v_dp_configs = self.get_cp_dp_config(cursor,goods_row['id'],order['outer_sku_id'])
                order_data['cp_config'].append({'goods_name':goods_row['name'],'g_detail_name':g_detail_name,'dp_config':v_dp_configs})
            if goods_row and goods_row['type'] == 3:
                g_detail_name = []
                if sku_properties_name:
                    for sn in sku_properties_name:
                        g_detail_name.append(sn['k'] + ':' + sn['v'])
                g_detail_name = ','.join(g_detail_name)
                cursor.execute('select * from goods_tc_config where sku_detail_id = %s',(sku_detail_row['id']))
                tc_configs = cursor.fetchall()
                tc_row_content = []
                for tc_config in tc_configs:
                    tc_config['content'] = json.loads(tc_config['content'])
                    s_sku_id = []
                    for cp_config_content_attr in tc_config['content']['attr']:
                        cursor.execute('select * from goods_attr where id = %s',
                                       (cp_config_content_attr['attr_id']))
                        attr_t = cursor.fetchone()
                        if attr_t and attr_t['type'] == 1:
                            s_sku_id.append(
                                cp_config_content_attr['attr_id'] + '_' + cp_config_content_attr['item_id'])
                    s_sku_id = ':'.join(s_sku_id)
                    cursor.execute('select * from goods where id = %s',(tc_config['dp_goods_id']))
                    cp_goods = cursor.fetchone()
                    t_dp_configs = self.get_cp_dp_config(cursor, tc_config['dp_goods_id'], s_sku_id)
                    tc_row_content.append({"cp_name":cp_goods['name'],"count":1,"dp_config":t_dp_configs})
                order_data['tc_config'].append({'tc_name': goods_row['name'],'g_detail_name':g_detail_name, 'tc_config': tc_row_content, 'tc_sku_id': order['outer_sku_id']})
            details.append(detail)
        order_data['details'] = details
        return order_data

    def deal_weight(self, val):
        if not val:
            return 0

        mo = re.findall(r'\约(\d{1,5}(\.\d+)?)*', val)

        if not mo[0][0]:
            return 0
        return int(mo[0][0])

    def get_cp_dp_config(self,cursor,goods_id,sku_id):
        cursor.execute('select * from goods_sku_detail where goods_id = %s and sku_id = %s',
                       (goods_id, sku_id))
        s_sku_detail = cursor.fetchone()
        cursor.execute('select * from goods_cp_config where sku_detail_id = %s order by id asc',
                       ( s_sku_detail['id']))
        s_cp_configs = cursor.fetchall()
        v_dp_configs = []
        for s_cp_config in s_cp_configs:
            s_content = json.loads(s_cp_config['content'])
            cursor.execute('select * from goods where id = %s', (s_cp_config['dp_goods_id']))
            s_dp_goods = cursor.fetchone()
            cursor.execute('select * from goods_attr_config where id = %s', (s_cp_config['dp_goods_id']))
            s_dp_goods_attr_config = cursor.fetchone()
            count = 1
            desc = ''
            attr_arr = []
            if s_content['attr']:
                for sd_attr in s_content['attr']:
                    cursor.execute('select * from goods_attr where id = %s', (sd_attr['attr_id']))
                    s_attr_row = cursor.fetchone()
                    cursor.execute('select * from goods_attr_item where id = %s', (sd_attr['item_id']))
                    s_item_row = cursor.fetchone()
                    if s_attr_row and s_item_row:
                        attr_arr.append(s_attr_row['name'] + ':' + s_item_row['name'])
            if s_dp_goods_attr_config['price_count_type'] == 2:
                attr_arr.insert(0, '重量:' + s_content['unit'] + 'g')
            desc = ','.join(attr_arr)
            if s_dp_goods_attr_config['price_count_type'] == 1:
                count = int(s_content['unit'])
            v_dp_configs.append({'dp_name': s_dp_goods['name'], 'count': count, 'desc': desc})
        return v_dp_configs


class OrderControl:

    def __init__(self):
        self.db_helper = DBHelper()
        self.redis_helper = RedisHelper()
        self.cursor = None

    def create_order_num(self):
        redis = self.redis_helper.getConnect()
        key = 'order_num'
        value = redis.get(key)
        if value:
            value = int(value) + 1
        else:
            value = 1
        if value > 1000:
            value = 1
        redis.set(key, value)
        return value

    def execute(self,limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute('select id from order where order_state = 2 and order_num is null order by id asc limit %s',(limit))
        rows = cursor.fetchall()
        updates = []
        for row in rows:
            state = 2
            order_num = None
            try:
                order_num = self.create_order_num()
            except Exception as e:
                state = 1
                order_control_logger.exception(e)
            updates.append((order_num,row['id']))
        if len(updates) > 0:
            cursor.executemany("update order set order_num = %s where id = %s",updates)


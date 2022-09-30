from libs.print import *
from libs.helper import *
from libs.youzan import ApiClient
from biz.order_print import *
from biz.get_azimuth import *
import time
import json
import sys
import re

class PurPrint:
    #type=1TOTAL打印机 type=2BRANCH打印机
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.order_print = OrderPrintFirst()
        self.redis_helper = RedisHelper()
        self.redis_conn = self.redis_helper.getConnect()
        self.conn = self.db_helper.getConnect()
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.api = ApiClient()
        self.cursor.execute("select * from goods_printer")
        self.devices = []
        device = {}
        device['seller_tag'] = pow(2, int(self.default_config['print']['dis_seller_id']))
        device['print_device0'] = PrintDevice(self.default_config['print']['id'],self.default_config['print']['secret'],self.default_config['print']['dis_device_id0'])
        device['print_device1'] = PrintDevice(self.default_config['print']['id'],self.default_config['print']['secret'],self.default_config['print']['dis_device_id1'])
        device['print_device2'] = PrintDevice(self.default_config['print']['id'],self.default_config['print']['secret'],self.default_config['print']['dis_device_id2'])
        self.devices.append(device)
        self.pur_rows = []

    def execute(self, limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        cursor.execute('select switch_state from syn_switch where switch_name=%s','print_syn')
        is_run = cursor.fetchone()
        if not is_run['switch_state']:
            pass
        else:
            cursor.execute('select id,tid,order_num,order_state,inner_state,yz_open_id,distance,delivery_time,user_id,top_time,goods_num,azimuth,updated_time,created_time,order_id,finished_time,print_time,sorting_time,repurchase,tags,refund_state,state from order_purchase where inner_state in (0,2) and order_state>1 and user_id!=0 and delivery_time BETWEEN UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE)) AND UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE) + INTERVAL 1 DAY) order by id desc limit %s',(limit))
            pur_rows = cursor.fetchall()
            if len(pur_rows) > 0:
                self.pur_rows = [i['order_id'] for i in pur_rows]
                try:
                    if len(self.pur_rows) > 0:
                        self.order_print.executePrint(20,self.pur_rows)
                except Exception as e:
                    order_control_logger.exception(e)
                dict_pur = {}
                sort_row = {}
                tags_row = {}
                dis_row = {}
                angle_row = {}
                pur_row = [dict_pur.setdefault(i['order_id'], []).append(i['user_id']) for i in pur_rows]
                s_row = [sort_row.setdefault(s['order_id'], []).append(s['sorting_time']) for s in pur_rows]
                t_row = [tags_row.setdefault(t['order_id'], []).append(t['tags']) for t in pur_rows]
                d_row = [dis_row.setdefault(d['order_id'], []).append(d['distance']) for d in pur_rows]
                a_row = [angle_row.setdefault(a['order_id'], []).append(a['azimuth']) for a in pur_rows]
                
                user_ids = [i['user_id'] for i in pur_rows]
                # 获取NAME
                getName = "select * from system_user where id in (%s)" % ','.join(['%s'] * len(user_ids))
                cursor.execute(getName, user_ids)
                user_name = cursor.fetchall()
                dict_name = {}
                user_names = [dict_name.setdefault(u['id'], []).append(u['username']) for u in user_name]
                getOrder = "select * from order where id in (%s)" % ','.join(['%s'] * len(self.pur_rows))
                cursor.execute(getOrder, self.pur_rows)
                rows = cursor.fetchall()
                fk_updates = []
                for row in rows:
                    row['angle'] = int(angle_row[row['id']][0])
                    row['distance'] = int(dis_row[row['id']][0])
                    order_data = self.generate_order_data(0, row)
                    order_data['user_name'] = dict_name[dict_pur[row['id']][0]][0]
                    order_data['sorting'] = sort_row[row['id']][0]
                    order_data['tags'] = tags_row[row['id']][0]
                    order_data['distance'] = int(dis_row[row['id']][0])/1000
                    order_data['order_pur_num'] = self.get_order_pur_num()
                    state = 1
                    try:
                        for i in self.devices:
                            if i['seller_tag']:
                                if self.is_reserve == 0:
                                    print_content = ''
                                    if order_data['is_points'] != 1:
                                        device_id = self.create_device_num()
                                        print_device = i['print_device'+device_id]
                                        print_content = self.generate_print_content(3, order_data, row)
                                        print_device.printContent(print_content, row['id'])
                                        print_content = self.generate_print_content(4, order_data, row)
                                        print_device.printContent(print_content, row['id'])
                                        state = 3
                                else:
                                    state = 1
                    except Exception as e:
                        state = 1
                        order_control_logger.exception(e)
                    fk_updates.append((state,time.time(),time.time(),row['id']))
                    time.sleep(1)
                if len(fk_updates)>0:
                    cursor.executemany("update order_purchase set inner_state = %s, updated_time = %s,print_time = %s where order_id = %s",fk_updates)

        cursor.execute('select * from order_cpfr where user_id is not null and inner_print_time is null and print_time BETWEEN UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE)) AND UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE) + INTERVAL 1 DAY) order by inner_print_time asc limit %s' % (limit))
        cpfr_rows = cursor.fetchall()
        if len(cpfr_rows) > 0:
            cpfr = []
            for i in cpfr_rows:
                push_content = json.loads(i['push_content'])
                push_content['full_order_info']['orders'] = json.loads(i['text_content'])
                i['push_content'] = json.dumps(push_content)
                i['delivery_start_time'] = i['print_time']
                order_data = self.generate_order_data(0, i)
                try:
                    for j in self.devices:
                        if j['seller_tag']:
                            if self.is_reserve == 0:
                                print_content = ''
                                if order_data['is_points'] != 1:
                                    device_id = self.create_device_num()
                                    print_device = j['print_device' + device_id]
                                    print_content = self.extra_print_content(3, order_data, i)
                                    print_device.printContent(print_content, i['id'])
                                    print_content = self.extra_print_content(4, order_data, i)
                                    print_device.printContent(print_content, i['id'])
                except Exception as e:
                    order_control_logger.exception(e)
                cpfr.append((int(time.time()), i['id']))
            if cpfr:
                cursor.executemany('update order_cpfr set inner_print_time = %s where id = %s', cpfr)

    # RESTOCK
    def extra_print_content(self, type, order_data, row):
        if type == 3:
            title = 'Delivery'
        elif type == 4:
            title = 'MATCHING'
        else:
            title = ''
        self.params = order_data.copy()
        self.params['title'] = title
        self.params['extra_content'] = ''
        star_tags = self.params.get('star_tags', 0)
        route_name = self.params.get('route_name', '')
        if 'con' in self.params['text_content']:
            self.params['extra_content'] = self.params['text_content']['con']
        content = ''
        if route_name:
            content += '<FB>路线：{route_name}</FB>\r\n\r\n'
        
        content += '         <FB><FS2>RESTOCK</FS2></FB>\r\n'
        content += '   <FB> </FB>\r\n\r\n'
        if type == 3:
            content += '   <FB>TEL：0000-00000000</FB>\r\n\r\n'
        if type != 3:
            if star_tags:
                content += '<FB>' + str(star_tags) + '</FB>\r\n\r\n'
                content += '<FB>距离：{distance}KM</FB>\r\n\r\n'
        content += '<FS><FB>'
        content += '         {title}</FB></FS>\r\n\r\n'
        content += '<FS><FB>ORDERNUM:{order_num}</FB></FS>\r\n\r\n'
        if type != 4:
            content += '********************************\r\n'
            content += '昵称:{nickname}\r\n'
            if type in [1, 2, 3]:
                content += '<FS><FB>'
            if type in [1, 2, 4]:
                content += 'NAME:{receiver_name}\r\n'
            else:
                content += 'NAME:{receiver_first_name}\r\n'
            content += '联系方式:{receiver_tel}\r\n'
            content += '配送方式:{express_type_label}\r\n'
            if 'delivery_time' in self.params.keys():
                content += '{delivery_time_label}:{delivery_time}\r\n'
            content += ':{receiver_address_detail}'
            if type in [1, 2, 3]:
                content += '</FB></FS>'
        content += '\r\n********************************\r\n'
        content += '下单时间:{created_time}\r\n'
        content += 'ORDERNUM:{tid}\r\n'
        if 'con' in self.params['text_content']:
            content += '********************************\r\n'
            content += '<FB><FS>补充内容:\r\n{extra_content}</FB></FS>\r\n'
        else:
            if type != 3:
                n = 0
                for detail in order_data['details']:
                    if detail['type'] == 1:
                        n += 1
                if n > 0:
                    content += '*********** <FB>商品</FB> ***********\r\n'
                    content += '<table>'
                    content_dp = []
                    for detail in order_data['details']:
                        detail_desc = ''
                        if 'sku_properties_name' in detail.keys():
                            detail_desc = []
                            for sku_properties_name in detail['sku_properties_name']:
                                detail_desc.append(sku_properties_name['k'] + ':' + sku_properties_name['v'])
                            detail_desc = '\r\n'.join(detail_desc)
                        # 打印配置
                        # if detail['print_config']:
                        #     detail_desc += detail['print_config']
                        detail['detail_desc'] = detail_desc
                        if detail['type'] == 1:
                            if type == 1:
                                content_dp.append(
                                    '<tr><td><FB><FS>{title}</FS><FS2> x{num}</FS2></FB></td><td>\t\t<FB>{price_str}</FB></td><td><FB>{detail_desc}</FB></td></tr>'.format(
                                        **detail))
                            elif type in [2, 4]:
                                content_dp.append(
                                    '<tr><td><FB><FS>{title}</FS><FS2> x{num}</FS2></FB></td><td>\t\t<FB>{price_str}</FB></td><td><FB><FS>{detail_desc}</FS></FB></td></tr>'.format(
                                        **detail))
                            else:
                                content += '<tr><td><FB><FS>{title} x{num}</FS></FB></td><td>\t\t<FB>{price_str}</FB></td></tr>'.format(
                                    **detail)
                            if not detail['detail_desc']:
                                pass
                    if type in [1, 2, 4]:
                        content += '<tr><td>****************</td><td>*****</td><td>*****</td><td>*****</td></tr>'.join(
                            content_dp)
                    content += '</table>'
                if len(order_data['cp_config']) > 0:
                    content += '*********** <FB>组合</FB> ***********\r\n'
                    cp_item_contents = []
                    label_content = []
                    for cp_config in order_data['cp_config']:
                        c_content = ''
                        c_dp_item_contents = []
                        if type == 1:
                            c_content += '<FB><FS>%s</FS> <FS2>x%s</FS2></FB><FB><right>%s</right></FB>\r\n%s\r\n<FB>' % (
                            cp_config['goods_name'], cp_config['count'], cp_config['price_str'],
                            cp_config['g_detail_name'])
                        elif type in [2, 4]:
                            c_content += '<FB><FS>%s</FS> <FS2>x%s</FS2></FB><FB><right>%s</right></FB>\r\n%s\r\n<FS><FB>' % (
                            cp_config['goods_name'], cp_config['count'], cp_config['price_str'],
                            cp_config['g_detail_name'])
                        else:
                            c_content += '<FB><FS>%s</FS> <FS>x%s</FS></FB><FB><right>%s</right></FB>\r\n%s\r\n' % (
                            cp_config['goods_name'], cp_config['count'], cp_config['price_str'],
                            cp_config['g_detail_name'])

                        for c_dp_config in cp_config['dp_config']:
                            c_dp_item_content = '— ' + c_dp_config['dp_name'] + ' x' + str(c_dp_config['count'])
                            if type in [1, 2, 4]:
                                if c_dp_config['desc']:
                                    c_dp_item_content += '\r\t' + c_dp_config['desc'].replace(',', '\r\t')
                            c_dp_item_contents.append(c_dp_item_content)
                        # 组合标签内容
                        c_label_content = ''
                        c_label_content += '<FS2>#%s</FS2><FS>%s</FS>\r\n' % (
                        self.params['order_num'], cp_config['goods_name'])
                        c_label_content += '%s\r\n' % cp_config['adorn_text']
                        c_label_content += '%s\r\n' % '\r\n'.join(cp_config['detail_name'])
                        for _ in range(int(cp_config['count'])):
                            label_content.append(c_label_content)
                        c_content += '\r\n'.join(c_dp_item_contents)
                        cp_item_contents.append(c_content)
                    if type == 1:
                        cp_item_contents = '</FB>\r\n********************************\r\n'.join(cp_item_contents)
                    elif type in [2, 4]:
                        cp_item_contents = '</FB></FS>\r\n********************************\r\n'.join(cp_item_contents)
                    elif type == 5:
                        return label_content
                    else:
                        cp_item_contents = '\r\n********************************\r\n'.join(cp_item_contents)
                    content += cp_item_contents
                    if type == 1:
                        content += '</FB>'
                    elif type in [2, 4]:
                        content += '</FB></FS>'
                    content += '\r\n'
                elif type == 5:
                    return ''
                if len(order_data['tc_config']) > 0:
                    content += '*********** <FB>礼包</FB> ***********\r\n'
                    tc_item_contents = []
                    for tc_config in order_data['tc_config']:
                        cp_item_contents = []
                        tc_item_content = ''
                        if type in [1, 2, 4]:
                            tc_item_content += '<FB><FS>{tc_name}</FS> <FS2>x%s</FS2></FB><FB><right>%s</right></FB>\r\n%s\r\n' % (
                            tc_config['count'], tc_config['price_str'], tc_config['g_detail_name'])
                        else:
                            tc_item_content += '<FB><FS>{tc_name}</FS> <FS>x%s</FS></FB><FB><right>%s</right></FB>\r\n%s\r\n' % (
                            tc_config['count'], tc_config['price_str'], tc_config['g_detail_name'])
                        for tc_config_content in tc_config['tc_config']:
                            cp_item_content = '<FS>{cp_name}({cp_attr_info})</FS>\r\n' if tc_config_content[
                                'cp_attr_info'] else '<FS>{cp_name}</FS>\r\n'
                            if type == 1:
                                cp_item_content += '<FB>'
                            if type in [2, 4]:
                                cp_item_content += '<FS><FB>'
                            dp_item_contents = []

                            for dp_config_content in tc_config_content['dp_config']:
                                # 键名未定义，商品sku_id对比礼包sku_id意义不明
                                # if 'sku_id' in dp_config_content.keys() and dp_config_content['sku_id'] != tc_config[
                                #     'tc_sku_id']:
                                #     continue
                                dp_item_content = '— {dp_name} x{count}'
                                if type in [1, 2, 4]:
                                    if dp_config_content['desc']:
                                        dp_item_content += '\r\t{dp_desc}'
                                dp_item_contents.append(dp_item_content.format(
                                    **{'dp_name': dp_config_content['dp_name'], 'count': dp_config_content['count'],
                                       'dp_desc': dp_config_content['desc'].replace(',', '\r\t')}))
                            dp_item_contents = '\r\n'.join(dp_item_contents)
                            cp_item_content += dp_item_contents
                            cp_item_contents.append(cp_item_content.format(
                                **{'cp_name': tc_config_content['cp_name'], 'count': tc_config_content['count'],
                                   'cp_attr_info': tc_config_content['cp_attr_info']}))
                        if type == 1:
                            cp_item_contents = '</FB>\r\n********************************\r\n'.join(cp_item_contents)
                        elif type in [2, 4]:
                            cp_item_contents = '</FB></FS>\r\n********************************\r\n'.join(
                                cp_item_contents)
                        else:
                            cp_item_contents = '\r\n********************************\r\n'.join(cp_item_contents)
                        tc_item_content += cp_item_contents
                        tc_item_contents.append(tc_item_content.format(**{'tc_name': tc_config['tc_name']}))
                    if type == 1:
                        tc_item_contents = '</FB>\r\n<FB>********************************</FB>\r\n'.join(
                            tc_item_contents)
                    elif type in [2, 4]:
                        tc_item_contents = '</FB></FS>\r\n<FB>********************************</FB>\r\n'.join(
                            tc_item_contents)
                    else:
                        tc_item_contents = '\r\n<FB>********************************</FB>\r\n'.join(tc_item_contents)
                    content += tc_item_contents
                    if type == 1:
                        content += '</FB>'
                    elif type in [2, 4]:
                        content += '</FB></FS>'
                    content += '\r\n'
        if type == 3:
            content += '********************************\r\n'
            content += '<center>扫码发货↓</center><QR>{url}</QR>\r\n\r\n'.format(
                       **{'url': 'http://vc.sto2c.com/youzan/api/logistics-confirm?id=' + str(row['id'])})
        return content.format(**self.params)

    def generate_print_content(self,type,order_data,row):
        if type == 3:
            title = 'Delivery'
        elif type == 4:
            title = 'MATCHING'
        else:
            title = ''
        self.params = order_data.copy()
        self.params['title'] = title
        star_tags = self.params.get('star_tags', 0)
        route_name = self.params.get('route_name', '')
        content = ''
        if route_name:
            content += '<FB>路线：{route_name}</FB>\r\n\r\n'
        
        if row['repair_times']:
            content += '         <FB><FS2>REPEAT</FS2></FB>\r\n' 
        content += '   <FB> </FB>\r\n\r\n'
        if type == 3:
            content += '   <FB>TEL：0000-00000000</FB>\r\n\r\n'
        if type == 4:   
            if star_tags:
                content += '<FB>' + str(star_tags) + '星</FB>\r\n\r\n'
                content += '<FB>距离：{distance}公里</FB>\r\n\r\n'
        content += '<FS><FB>'        
        content += '         {title}</FB></FS>\r\n\r\n'
        if type == 3:
            content += '<FS><FB>ORDERNUM:{order_num}</FB></FS>\r\n\r\n'
        if type != 3:
            content += '<FS><FB>ORDERNUM:{order_num} {user_name} {order_pur_num}{tags}</FB></FS>\r\n\r\n'
            content += '<FS><FB>分拣时间:{sorting}分钟</FB></FS>\r\n\r\n'
            if 'delivery_time' in self.params.keys():
                content += '<FS><FB>{delivery_time_label}:{delivery_time}</FB></FS>\r\n'
        if order_data['is_points'] == 0:
            content += '<FS><FB>买家留言:\r\n{buyer_message}</FB></FS>\r\n'
        else:
            content += '<FS><FB>订单类型: 积分兑换</FB></FS>\r\n'
        if type != 4:
            content += '********************************\r\n'
            content += '昵称:{nickname}\r\n'
            if type in [1,2,3]:
                content += '<FS><FB>'
            if type in [1,2,4]:
                content += 'NAME:{receiver_name}\r\n'
            else:
                content += 'NAME:{receiver_first_name}\r\n'
            content += '联系方式:{receiver_tel}\r\n'
            content += '配送方式:{express_type_label}\r\n'
            if 'delivery_time' in self.params.keys():
                content += '{delivery_time_label}:{delivery_time}\r\n'
            content += ':{receiver_address_detail}'
            if type in [1,2,3]:
                content += '</FB></FS>'
        content += '\r\n********************************\r\n'
        content += '下单时间:{created_time}\r\n'
        content += 'ORDERNUM:{tid}\r\n'
        if type !=3:
            n = 0
            for detail in order_data['details']:
                if detail['type']==1:
                    n+=1
            if n>0:
                content += '*********** <FB>商品</FB> ***********\r\n'
                content += '<table>'
                content_dp = []
                for detail in order_data['details']:
                    detail_desc = ''
                    if 'sku_properties_name' in detail.keys():
                        detail_desc = []
                        for sku_properties_name in detail['sku_properties_name']:
                            detail_desc.append(sku_properties_name['k']+':'+sku_properties_name['v'])
                        detail_desc = '\r\n'.join(detail_desc)

                    detail['detail_desc'] = detail_desc
                    if detail['type']==1:
                        if type==1:
                            content_dp.append('<tr><td><FB><FS>{title}</FS><FS2> x{num}</FS2></FB></td><td>\t\t<FB>{price_str}</FB></td><td><FB>{detail_desc}</FB></td></tr>'.format(**detail))
                        elif type in [2,4]:
                            content_dp.append('<tr><td><FB><FS>{title}</FS><FS2> x{num}</FS2></FB></td><td>\t\t<FB>{price_str}</FB></td><td><FB><FS>{detail_desc}</FS></FB></td></tr>'.format(**detail))
                        else:
                            content += '<tr><td><FB><FS>{title} x{num}</FS></FB></td><td>\t\t<FB>{price_str}</FB></td></tr>'.format(**detail)
                        if not detail['detail_desc']:
                            pass
                if type in [1,2,4]:
                    content += '<tr><td>****************</td><td>*****</td><td>*****</td><td>*****</td></tr>'.join(content_dp)
                content += '</table>'
            if len(order_data['cp_config'])>0:
                content += '*********** <FB>组合</FB> ***********\r\n'
                cp_item_contents = []
                label_content = []
                for cp_config in order_data['cp_config']:
                    c_content = ''
                    c_dp_item_contents = []
                    if type == 1:
                        c_content += '<FB><FS>%s</FS> <FS2>x%s</FS2></FB><FB><right>%s</right></FB>\r\n%s\r\n<FB>'%(cp_config['goods_name'],cp_config['count'],cp_config['price_str'],cp_config['g_detail_name'])
                    elif type in [2,4]:
                        c_content += '<FB><FS>%s</FS> <FS2>x%s</FS2></FB><FB><right>%s</right></FB>\r\n%s\r\n<FS><FB>'%(cp_config['goods_name'],cp_config['count'],cp_config['price_str'],cp_config['g_detail_name'])
                    else:
                        c_content += '<FB><FS>%s</FS> <FS>x%s</FS></FB><FB><right>%s</right></FB>\r\n%s\r\n'%(cp_config['goods_name'],cp_config['count'],cp_config['price_str'],cp_config['g_detail_name'])
                    for c_dp_config in cp_config['dp_config']:
                        c_dp_item_content = '— '+c_dp_config['dp_name']+' x'+str(c_dp_config['count'])
                        if type in [1, 2, 4]:
                            if c_dp_config['desc']:
                                c_dp_item_content += '\r\t'+c_dp_config['desc'].replace(',','\r\t')
                        c_dp_item_contents.append(c_dp_item_content)
                    c_content += '\r\n'.join(c_dp_item_contents)
                    #组合标签内容
                    c_label_content = ''
                    c_label_content += '<FS2>#%s</FS2><FS>%s</FS>\r\n'%(self.params['order_num'],cp_config['goods_name'])
                    c_label_content += '%s\r\n'%cp_config['adorn_text']
                    c_label_content += '%s\r\n'%'\r\n'.join(cp_config['detail_name'])
                    for _ in range(int(cp_config['count'])):
                        label_content.append(c_label_content)
                    cp_item_contents.append(c_content)
                if type==1:
                    cp_item_contents = '</FB>\r\n********************************\r\n'.join(cp_item_contents)
                elif type in [2,4]:
                    cp_item_contents = '</FB></FS>\r\n********************************\r\n'.join(cp_item_contents)
                elif type == 5:
                    return label_content
                else:
                    cp_item_contents = '\r\n********************************\r\n'.join(cp_item_contents)
                content += cp_item_contents
                if type==1:
                    content += '</FB>'
                elif type in [2,4]:
                    content += '</FB></FS>'
                content += '\r\n'
            elif type == 5:
                return ''
            if len(order_data['tc_config']) > 0:
                content += '*********** <FB>礼包</FB> ***********\r\n'
                tc_item_contents = []
                for tc_config in order_data['tc_config']:
                    cp_item_contents = []
                    tc_item_content = ''
                    if type in [1,2,4]:
                        tc_item_content += '<FB><FS>{tc_name}</FS> <FS2>x%s</FS2></FB><FB><right>%s</right></FB>\r\n%s\r\n'%(tc_config['count'],tc_config['price_str'],tc_config['g_detail_name'])
                    else:
                        tc_item_content += '<FB><FS>{tc_name}</FS> <FS>x%s</FS></FB><FB><right>%s</right></FB>\r\n%s\r\n'%(tc_config['count'],tc_config['price_str'],tc_config['g_detail_name'])
                    for tc_config_content in tc_config['tc_config']:
                        cp_item_content = '<FS>{cp_name}({cp_attr_info})</FS>\r\n' if tc_config_content['cp_attr_info'] else '<FS>{cp_name}</FS>\r\n'
                        if type==1:
                            cp_item_content += '<FB>'
                        if type in [2,4]:
                            cp_item_content += '<FS><FB>'
                        dp_item_contents = []
                        for dp_config_content in tc_config_content['dp_config']:
                            # 键名未定义，商品sku_id对比礼包sku_id意义不明
                            # if 'sku_id' in dp_config_content.keys() and dp_config_content['sku_id'] != tc_config[
                            #     'tc_sku_id']:
                            #     continue
                            dp_item_content = '— {dp_name} x{count}'
                            if type in [1,2,4]:
                                if dp_config_content['desc']:
                                    dp_item_content += '\r\t{dp_desc}'
                            dp_item_contents.append(dp_item_content.format(
                                **{'dp_name': dp_config_content['dp_name'], 'count': dp_config_content['count'],
                                'dp_desc': dp_config_content['desc'].replace(',','\r\t')}))
                        dp_item_contents = '\r\n'.join(dp_item_contents)
                        cp_item_content += dp_item_contents
                        cp_item_contents.append(cp_item_content.format(
                            **{'cp_name': tc_config_content['cp_name'], 'count': tc_config_content['count'], 'cp_attr_info':tc_config_content['cp_attr_info']}))
                    if type==1:
                        cp_item_contents = '</FB>\r\n********************************\r\n'.join(cp_item_contents)
                    elif type in [2,4]:
                        cp_item_contents = '</FB></FS>\r\n********************************\r\n'.join(cp_item_contents)
                    else:
                        cp_item_contents = '\r\n********************************\r\n'.join(cp_item_contents)
                    tc_item_content += cp_item_contents
                    tc_item_contents.append(tc_item_content.format(**{'tc_name': tc_config['tc_name']}))
                if type==1:
                    tc_item_contents = '</FB>\r\n<FB>********************************</FB>\r\n'.join(tc_item_contents)
                elif type in [2,4]:
                    tc_item_contents = '</FB></FS>\r\n<FB>********************************</FB>\r\n'.join(tc_item_contents)
                else:
                    tc_item_contents = '\r\n<FB>********************************</FB>\r\n'.join(tc_item_contents)
                content += tc_item_contents
                if type==1:
                    content += '</FB>'
                elif type in [2,4]:
                    content += '</FB></FS>'
                content += '\r\n'
        content += '********************************\r\n'
        content += '<FS><FB>总计:  ￥{total_fee}</FB></FS>\r\n'
        content += '配送费:  ￥{post_fee}\r\n'
        content += '优惠金额:  ￥{order_discount_fee}\r\n'
        content += '<FS><FB>实付:  ￥{payment}</FB></FS>'
        content += '\r\n\r\n'
        if type == 3:
            content += '********************************\r\n'
            content += '<center>扫码发货↓</center><QR>{url}</QR>\r\n\r\n'.format(
                **{'url': 'http://vc.sto2c.com/youzan/api/logistics-confirm?id=' + str(row['id'])})
        if type == 4:
            content += '********************************\r\n'
            content += '<center>扫码完成↓</center><BR3>{url}</BR3>\r\n\r\n'.format(
                **{'url': str(row['id'])})
        content_text = content.format(**self.params)
        return content_text

    def generate_order_data(self,seller_type,row,is_extra=False):
        cursor = self.cursor
        text_content = row.get('text_content','')
        if text_content:
            text_content = json.loads(text_content)
        order_data = row.get('order_data','')
        order_id = row.get('order_id','')
        if order_data:
            if order_id:
                cursor.execute('select * from order where id = %s'%order_id)
                order_row = cursor.fetchone()
                order_data = json.loads(order_row['order_data'])
            else:
                order_data = json.loads(order_data)
            self.is_reserve = 0
            delivery_time = None
            order_data['text_content'] = text_content
            push_content = json.loads(row['push_content'])
            cursor.execute('select * from get_cus_star where yz_open_id = %s',
                           (push_content['full_order_info']['buyer_info']['yz_open_id']))
            cus_star = cursor.fetchone()
            if cus_star:
                order_data['star_tags'] = cus_star['star']
            # 路线
            route_name = ''
            get_route = GetRoute(int(row.get('distance')), int(row.get('angle')))
            route_name = get_route.execute_to()
            if route_name:
                order_data['route_name'] = route_name
            push_content = json.loads(row['push_content'])
            if push_content['full_order_info']['order_info']['express_type'] == 2:
                try:
                    dbtime = time.strftime('%Y{}%m{}%d{}\r\n%H:%M:%S',time.localtime(row['delivery_start_time'])).format("年", "月", "日")
                    delivery_time = dbtime
                except:
                    if not row['repair_times']:
                        self.is_reserve = 1
                    delivery_time = time.strftime('%Y{}%m{}%d{}\r\n%H:%M:%S', time.localtime(row['delivery_start_time'])).format("年","月","日")
            if delivery_time:
                order_data['delivery_time'] = delivery_time
            if seller_type:
                return self.in_order(order_data, seller_type, cursor)
            elif seller_type == 0:
                return self.all_in_order(order_data, cursor)
            return order_data
        else:
            self.is_reserve = 0
            self.seller_type = seller_type
            cursor = self.cursor
            order_data = {}
            text_content = row.get('text_content','')
            if text_content:
                text_content = json.loads(text_content)
            order_data['text_content'] = text_content
            order_data['order_num'] = row['order_num']
            push_content = json.loads(row['push_content'])
            express_type_label = None
            delivery_time = None
            delivery_start_time = None
            order_data['is_points'] = 0
            order_data['buyer_phone'] = push_content['full_order_info']['buyer_info']['buyer_phone']
            order_data['receiver_tel'] = push_content['full_order_info']['address_info']['receiver_tel']
            if push_content['full_order_info']['order_info']['express_type'] == 9:
                express_type_label = '无需发货(虚拟商品订单)'
                order_data['receiver_tel'] = push_content['full_order_info']['buyer_info']['buyer_phone']
                order_data['is_points'] = 1
            elif push_content['full_order_info']['order_info']['express_type'] == 0:
                express_type_label = '快递发货'
                start_time = row['delivery_start_time']
                dbtime = time.strftime('%Y{}%m{}%d{}\r\n%H:%M:%S', time.localtime(start_time)).format("年", "月", "日")
                delivery_start_time = time.strftime("%Y{}%m{}%d{}%H:%M:%S", time.localtime(start_time - 1800)).format('年', '月', '日')
                delivery_time = dbtime
            elif push_content['full_order_info']['order_info']['express_type'] == 1:
                express_type_label = '到店自提'
                start_time = row['delivery_start_time']
                dbtime = time.strftime('%Y{}%m{}%d{}\r\n%H:%M:%S', time.localtime(start_time)).format("年", "月", "日")
                delivery_start_time = time.strftime("%Y{}%m{}%d{}%H:%M:%S", time.localtime(start_time - 1800)).format('年', '月', '日')
                delivery_time = dbtime
                order_data['delivery_time_label'] = '自提时间'
            elif push_content['full_order_info']['order_info']['express_type'] == 2:
                express_type_label = '同城配送'
                try:
                    start_time = row['delivery_start_time']
                    dbtime = time.strftime('%Y{}%m{}%d{}\r\n%H:%M:%S', time.localtime(start_time)).format("年", "月", "日")
                    delivery_start_time = time.strftime("%Y{}%m{}%d{}%H:%M:%S",time.localtime(start_time - 1800)).format('年', '月', '日')
                    delivery_time = dbtime
                except:
                    if not row['repair_times']:
                        self.is_reserve = 1
                    start_time = row['delivery_start_time']
                    delivery_start_time = time.strftime("%Y{}%m{}%d{}%H:%M:%S",time.localtime(start_time - 1800)).format('年', '月', '日')
                    delivery_time = time.strftime('%Y{}%m{}%d{}\r\n%H:%M:%S', time.localtime(start_time)).format("年", "月", "日")
                order_data['delivery_time_label'] = '配送时间'
            order_data['express_type_label'] = express_type_label
            if delivery_time:
                order_data['delivery_time'] = delivery_time
                order_data['delivery_start_time'] = delivery_start_time
            order_data['tid'] = push_content['full_order_info']['order_info']['tid']
            receiver_name = push_content['full_order_info']['address_info']['receiver_name']
            receiver_name = receiver_name.strip()
            order_data['receiver_name'] = receiver_name
            if receiver_name:
                order_data['receiver_first_name'] = receiver_name[0]+'*'*(len(receiver_name)-1)
            else:
                order_data['receiver_first_name'] = ''
            order_data['nickname'] = push_content['full_order_info']['buyer_info']['fans_nickname']
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
            if is_extra:
                order_data['details'] = details
                return order_data
            for order in push_content['full_order_info']['orders']:
                cursor.execute('select * from goods where id = %s',(order['outer_item_id']))
                goods_row = cursor.fetchone()
                if self.seller_type>0:
                    cursor.execute("select tag_id from goods_tag_assign where goods_id=%s",order['outer_item_id'])
                    is_need = cursor.fetchone()
                    if is_need and goods_row['type']==1 and is_need['tag_id']!=self.seller_type:
                        continue
                    elif not is_need:
                        continue

                detail = {}
                detail['title'] = order['title']
                detail['dp_id'] = order['outer_item_id']
                detail['cp_id'] = 0
                detail['print_config'] = ''
                if goods_row:
                    detail['type'] = goods_row['type']
                    detail['print_config'] = goods_row['print_config']
                else:
                    detail['type'] = 1
                detail['num'] = order['num']
                detail['count'] = detail['num']
                detail['weight'] = 0
                detail['handle'] = ''
                detail['price'] = order['num'] * float(order['price']) if order['price']!='0.00' else 0.00
                space = ' ' if detail['price']<10 else ''
                detail['price_str'] = '￥%.2f%s'%(detail['price'],space) if detail['price']!=0.00 else ''
                sku_properties_name = []
                cursor.execute('select * from goods_sku_detail where goods_id = %s and sku_id = %s', (order['outer_item_id'],order.get('outer_sku_id','')))
                sku_detail_row = cursor.fetchone()
                if order['sku_properties_name']:
                    order['sku_properties_name'] = json.loads(order['sku_properties_name'])
                    sku_ids = []
                    for sku_item in order['sku_properties_name']:
                        if sku_item['k'] in ['重量','单重']:
                            sku_item['k'] = '单重'
                            detail['weight'] = self.deal_weight(sku_item['v'])
                        sku_properties_name.append({'k': sku_item['k'], 'v': sku_item['v']})
                        if sku_item['k'] == 'PROCESS':
                            cursor.execute("select * from goods_attr_item where attr_id=2 and name=%s",sku_item['v'])
                            handle_id = cursor.fetchone()
                            detail['handle'] = '2_'+str(handle_id['id'])
                        if sku_item['k'] not in ['重量','单重','PROCESS']:
                            cursor.execute("select * from goods_attr where name=%s",sku_item['k'])
                            attr_id = cursor.fetchone()
                            if attr_id:
                                cursor.execute("select * from goods_attr_item where attr_id=%s and name=%s",(attr_id['id'],sku_item['v']))
                                item_id = cursor.fetchone()
                                if item_id:
                                    sku_ids.append('%s_%s'%(item_id['attr_id'],item_id['id']))
                        if detail['num'] > 1 and sku_item['k'] == '单重':
                            sku_item['k'] = '总重'
                            sku_item['val'] = self.deal_weight(sku_item['v'])
                            sku_item['total'] = str(sku_item['val'] * order['num'])
                            sku_properties_name.append({'k': sku_item['k'], 'v': sku_item['total']})
                    detail['sku_ids'] = ':'.join(sku_ids)
                if len(sku_properties_name)>0:
                    detail['sku_properties_name'] = sku_properties_name
                if goods_row and goods_row['type'] == 2:
                    detail_name = []
                    g_detail_name = []
                    if sku_properties_name:
                        for sn in sku_properties_name:
                            detail_name.append('[%s][%s]'%(sn['k'],sn['v']))
                            g_detail_name.append(sn['k']+':'+sn['v'])
                    g_detail_name = ','.join(g_detail_name)
                    v_dp_configs = self.get_cp_dp_config(cursor,goods_row['id'],order.get('outer_sku_id',''))
                    if v_dp_configs or self.seller_type==0:
                        order_data['cp_config'].append({'goods_name':goods_row['name'],'adorn_text':goods_row['adorn_text'],'detail_name':detail_name,'g_detail_name':g_detail_name,'dp_config':v_dp_configs,'count':str(detail['num']),'price_str':detail['price_str']})
                if goods_row and goods_row['type'] == 3:
                    detail_name = []
                    g_detail_name = []
                    if sku_properties_name:
                        for sn in sku_properties_name:
                            detail_name.append(sn['k']+']['+sn['v'])
                            g_detail_name.append(sn['k'] + ':' + sn['v'])
                    g_detail_name = ','.join(g_detail_name)
                    tc_row_content = []
                    if sku_detail_row:
                        cursor.execute('select * from goods_tc_config where sku_detail_id = %s',(sku_detail_row['id']))
                        tc_configs = cursor.fetchall()
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
                            tc_row_content.append({"cp_name":cp_goods['name'],"count":1,"dp_config":t_dp_configs,"cp_attr_info":tc_config['cp_attr_info']})
                    order_data['tc_config'].append({'tc_id':order['outer_item_id'],'tc_name': goods_row['name'],'detail_name':detail_name,'g_detail_name':g_detail_name, 'tc_config': tc_row_content, 'tc_sku_id': order.get('outer_sku_id',''),'count':str(detail['num']),'price_str':detail['price_str']})
                details.append(detail)
            order_data['details'] = details
            return order_data

    def in_order(self, order_data, seller_type, cursor):
        tag = str(seller_type)
        details = []
        if order_data['is_points']:
            order_data['details'] = []
            order_data['cp_config'] = []
            order_data['tc_config'] = []
            return order_data
        for i in order_data['details']:
            if i['type'] == 1:
                dp_need = []
                cursor.execute("select tag_id from goods_tag_assign where goods_id=%s", i['dp_id'])
                dp_need = cursor.fetchall()
                dp_list = []
                need_li = []
                if len(dp_need) > 0:
                    dp_list = [item for item in dp_need if
                               item['tag_id'] in eval(self.default_config['goods_tag']['no_print'])]
                    need_li = [itme for itme in dp_need if itme['tag_id'] == seller_type]
                    if not dp_list and len(need_li) > 0:
                        details.append(i)
        order_data['details'] = details
        cp_config = []
        for i in order_data['cp_config']:
            dp_config = []
            for j in i['dp_config']:
                cp_need = []
                cursor.execute("select tag_id from goods_tag_assign where goods_id=%s", j['dp_id'])
                cp_need = cursor.fetchall()
                cp_list = []
                need_li2 = []
                if len(cp_need) > 0:
                    cp_list = [item2 for item2 in cp_need if
                               item2['tag_id'] in eval(self.default_config['goods_tag']['no_print'])]
                    need_li2 = [itme2 for itme2 in cp_need if itme2['tag_id'] == seller_type]
                    if not cp_list and len(need_li2) > 0:
                        dp_config.append(j)
            i['dp_config'] = dp_config
            if dp_config:
                cp_config.append(i)
        order_data['cp_config'] = cp_config
        tc_configs = []
        for i in order_data['tc_config']:
            tc_config = []
            for j in i['tc_config']:
                dp_config = []
                for k in j['dp_config']:
                    tc_need = []
                    cursor.execute("select tag_id from goods_tag_assign where goods_id=%s", k['dp_id'])
                    tc_need = cursor.fetchall()
                    tc_list = []
                    need_li3 = []
                    if len(tc_need) > 0:
                        tc_list = [item3 for item3 in tc_need if
                                   item3['tag_id'] in eval(self.default_config['goods_tag']['no_print'])]
                        need_li3 = [itme3 for itme3 in tc_need if itme3['tag_id'] == seller_type]
                        if not tc_list and len(need_li3) > 0:
                            dp_config.append(k)
                j['dp_config'] = dp_config
                if dp_config:
                    tc_config.append(j)
            i['tc_config'] = tc_config
            if tc_config:
                tc_configs.append(i)
        order_data['tc_config'] = tc_configs
        return order_data

    def all_in_order(self, order_data, cursor):
        details = []
        if order_data['is_points']:
            return order_data
        for i in order_data['details']:
            if i['type'] == 1:
                dp_need = []
                cursor.execute("select tag_id from goods_tag_assign where goods_id=%s", i['dp_id'])
                dp_need = cursor.fetchall()
                if not dp_need:
                    details.append(i)
                dp_list = []
                if len(dp_need) > 0:
                    dp_list = [item for item in dp_need if
                               item['tag_id'] in eval(self.default_config['goods_tag']['no_print'])]
                if not dp_list:
                    details.append(i)
        order_data['details'] = details
        cp_config = []
        for i in order_data['cp_config']:
            dp_config = []
            for j in i['dp_config']:
                cp_need = []
                cursor.execute("select tag_id from goods_tag_assign where goods_id=%s", j['dp_id'])
                cp_need = cursor.fetchall()
                if not cp_need:
                    dp_config.append(j)
                cp_list = []
                if len(cp_need) > 0:
                    cp_list = [item2 for item2 in cp_need if
                               item2['tag_id'] in eval(self.default_config['goods_tag']['no_print'])]
                if not cp_list:
                    dp_config.append(j)
            i['dp_config'] = dp_config
            if dp_config:
                cp_config.append(i)
        order_data['cp_config'] = cp_config
        tc_configs = []
        for i in order_data['tc_config']:
            tc_config = []
            for j in i['tc_config']:
                dp_config = []
                for k in j['dp_config']:
                    tc_need = []
                    cursor.execute("select tag_id from goods_tag_assign where goods_id=%s", k['dp_id'])
                    tc_need = cursor.fetchall()
                    if not tc_need:
                        dp_config.append(k)
                    tc_list = []
                    if len(tc_need) > 0:
                        tc_list = [item3 for item3 in tc_need if
                                   item3['tag_id'] in eval(self.default_config['goods_tag']['no_print'])]
                    if not tc_list:
                        dp_config.append(k)
                j['dp_config'] = dp_config
                if dp_config:
                    tc_config.append(j)
            i['tc_config'] = tc_config
            if tc_config:
                tc_configs.append(i)
        order_data['tc_config'] = tc_configs
        return order_data

    def deal_weight(self, val):
        if not val:
            return 0
        mo = []
        mo = re.findall(r'[0-9]+(?=g)', val)
        if not mo:
            return 0
        return int(mo[0])

    def get_cp_dp_config(self,cursor,goods_id,sku_id):
        cursor = self.cursor
        cursor.execute('select * from goods_sku_detail where goods_id = %s and sku_id = %s',
                       (goods_id, sku_id))
        s_sku_detail = cursor.fetchone()
        cursor.execute('select * from goods_cp_config where goods_id = %s and sku_detail_id = %s order by id asc',
                       ( goods_id,s_sku_detail['id']))
        s_cp_configs = cursor.fetchall()
        v_dp_configs = []
        for s_cp_config in s_cp_configs:
            s_content = json.loads(s_cp_config['content'])
            cursor.execute('select * from goods where id = %s', (s_cp_config['dp_goods_id']))
            s_dp_goods = cursor.fetchone()
            cursor.execute('select * from goods_attr_config where goods_id = %s', (s_cp_config['dp_goods_id']))
            s_dp_goods_attr_config = cursor.fetchone()
            count = 1
            desc = ''
            attr_arr = []
            sku_list = []
            sku_ids = ''
            handle_id = ''
            if s_content['attr']:
                for sd_attr in s_content['attr']:
                    if sd_attr['attr_id'] not in ['1','2']:
                        sku_id = sd_attr['attr_id']+'_'+sd_attr['item_id']
                        sku_list.append(sku_id)
                    if sd_attr['attr_id'] == '2':
                        handle_id = sd_attr['attr_id']+'_'+sd_attr['item_id']
                    cursor.execute('select * from goods_attr where id = %s', (sd_attr['attr_id']))
                    s_attr_row = cursor.fetchone()
                    cursor.execute('select * from goods_attr_item where id = %s', (sd_attr['item_id']))
                    s_item_row = cursor.fetchone()
                    if s_attr_row and s_item_row:
                        attr_arr.append(s_attr_row['name'] + ':' + s_item_row['name'])
                sku_ids = ':'.join(sku_list)
            if s_dp_goods_attr_config['price_count_type'] == 2:
                dp_attr_info = '(%s)'%s_cp_config['dp_attr_info'] if s_cp_config['dp_attr_info'] else ''
                attr_arr.insert(0, '重量:' + s_content['unit'] + 'g' + dp_attr_info)
            desc = ','.join(attr_arr)
            if s_dp_goods_attr_config['price_count_type'] == 1:
                count = int(s_content['unit'])
                weight = 0
            else:
                weight = int(s_content['unit'])
            if handle_id:
                handle = handle_id
            else:
                handle = ''
            if self.seller_type>0:
                cursor.execute("select tag_id from goods_tag_assign where goods_id=%s",s_cp_config['dp_goods_id'])
                is_need = cursor.fetchone()
                if is_need and s_dp_goods['type']==1 and is_need['tag_id']!=self.seller_type:
                    continue
                elif not is_need:
                    continue
            v_dp_configs.append({'cp_id':goods_id,'dp_name': s_dp_goods['name'],'dp_id':s_cp_config['dp_goods_id'], 'handle':handle,'count': count, 'weight':weight, 'sku_ids':sku_ids, 'desc': desc})
        return v_dp_configs

    def create_device_num(self):
        if not self.redis_conn:
            self.redis_conn = self.redis_helper.getConnect()
        key = 'device_num'
        value = self.redis_conn.get(key)
        if value:
            value = int(value) + 1
        else:
            value = 0
        if value > 2:
            value = 0
        self.redis_conn.set(key, value)
        return str(value)

    def get_order_pur_num(self):
        if not self.redis_conn:
            self.redis_conn = self.redis_helper.getConnect()
        order_key = 'order_pur_num_v1'
        order_value = self.redis_conn.get(order_key)
        if order_value:
            order_value = int(order_value) + 1
        else:
            order_value = 1
        if order_value > 999:
            order_value = 1
        self.redis_conn.set(order_key, order_value)
        return str(order_value)



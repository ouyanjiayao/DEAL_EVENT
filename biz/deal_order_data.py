from libs.helper import *
from biz.send_email import *
from biz.deal_order_goods import *
import re
import json
import pandas as pd

class OrderGoodsData:
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.cursor = None
        self.conn = None
        self.sendMail = SendMail()
        self.dealGoods = DealOrderGoods()
        

    def execute_to(self):
        self.conn = self.db_helper.getConnect()
        cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        try:
            cursor.execute("select * from order where order_state>1 and order_num is not null and delivery_start_time BETWEEN UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE)) AND (if(UNIX_TIMESTAMP(concat(cast(SYSDATE() AS DATE),' 12:00:00'))<UNIX_TIMESTAMP(),UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE)+ INTERVAL 1 DAY),UNIX_TIMESTAMP(concat(cast(SYSDATE() AS DATE),' 12:00:00')))) order by id asc ")
            purchase = cursor.fetchall()
            data = purchase.copy()
            self.getOrderData(data)
        except Exception as e:
            order_detail_logger.exception(e)

    def getOrderData(self, data):
        inserts = []
        for i in data:
            order_data = json.loads(i['order_data'])
            for m in order_data['details']:
                #         print(m)
                if m.get('type') == 1 and (m.get('dp_id',0)!=''):

                    sku_properties_name = ''
                    tag = ''
                    if m.get('tag_id','None') != 'None':
                        self.cursor.execute("select name from goods_tag where id=%s", m['tag_id'])
                        tag = self.cursor.fetchone()
                        if tag:
                            tag = tag['name']
                        else:
                            tag = '未分类'
                    else:
                        tag = '未分类'
                    handle = []
                    attr = []
                    handle5 = ''
                    attr5 = ''
                    if m.get('sku_properties_name'):
                        for l in m['sku_properties_name']:
                            sku_properties_name += l['k'] + ':' + l['v']
                            if l['k'] == 'PROCESS':
                                handle.append(l['k'] + ':' + l['v'])
                            else:
                                attr.append(l['k'] + ':' + l['v'])
                            attr5 = ','.join(attr)
                            handle5 = ','.join(handle)
                    inserts.append((m['dp_id'], m['title'], m['sku_ids'], attr5, handle5, m['weight'], m['count'], tag, m.get('tag_id','None')))

            for j in order_data['cp_config']:
                for k in j['dp_config']:
                    #             print(k)
                    handle1 = []
                    attr1 = []
                    handle3 = ''
                    attr3 = ''
                    tag_names = ''
                    if k.get('dp_id', 0) != '':
                        if k['tag_id'] != 'None':
                            self.cursor.execute("select name from goods_tag where id=%s", k['tag_id'])
                            tag_names = self.cursor.fetchone()
                            if tag_names:
                                tag_names = tag_names['name']
                            else:
                                tag_names = '未分类'
                        else:
                            tag_names = '未分类'
                        if k.get('desc'):
                            for d in k['desc'].split(','):
                                if d.find('PROCESS') >= 0:
                                    handle1.append(d)
                                else:
                                    attr1.append(d)
                                attr3 = ','.join(attr1)
                                handle3 = ','.join(handle1)
                        inserts.append((k['dp_id'], k['dp_name'], k['sku_ids'], attr3, handle3, k['weight'], k['count'], tag_names, k['tag_id']))

            for n in order_data['tc_config']:
                for tc in n['tc_config']:
                    for dp_config in tc['dp_config']:
                        tc_tag = ''
                        if dp_config.get('dp_id',0)!='':
                            if dp_config['tag_id'] != 'None':
                                self.cursor.execute("select name from goods_tag where id=%s", dp_config['tag_id'])
                                tc_tag = self.cursor.fetchone()
                                if tc_tag:
                                    tc_tag = tc_tag['name']
                                else:
                                    tc_tag = '未分类'
                            else:
                                tc_tag = '未分类'
                            handle2 = []
                            attr2 = []
                            handle4 = ''
                            attr4 = ''
                            if dp_config.get('desc'):
                                for des in dp_config['desc'].split(','):
                                    if des.find('PROCESS') >= 0:
                                        handle2.append(des)
                                    else:
                                        attr2.append(des)
                                    attr4 = ','.join(attr2)
                                    handle4 = ','.join(handle2)

                            inserts.append((dp_config['dp_id'], dp_config['dp_name'], dp_config['sku_ids'], attr4, handle4, dp_config['weight'], dp_config['count'], tc_tag, dp_config['tag_id']))
        if len(inserts) > 0:
            self.cursor.executemany("insert into bill_dp_on(dp_goods_id,dp_name,sku_id,sku_name,handle,weight,count,tag_name,tag) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)", inserts)
            self.conn.commit()
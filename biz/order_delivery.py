import sys
from libs.youzan import ApiClient
from libs.helper import *
from syn.syn_list import *
from biz.order_control import OrderPrint
from biz.send_email import *
import pandas as pd
import xlsxwriter
import threading
import time
import json
import os

class DealOrder:
    def __init__(self):
        self.api = ApiClient()
        self.db = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.order_print = OrderPrint()
        self.sendMail = SendMail()

    def Config(self,type):
        self.now = time.time()
        self.week_before = self.now - 86400*7
        self.today_row = time.strftime('%Y-%m-%d',time.localtime(self.now))
        self.weekago_row = time.strftime('%Y-%m-%d',time.localtime(self.week_before))
        self.path_name = self.default_config['toexcel']['goods_Statistics_path']
        self.file_name = self.path_name + '/%s_detail_%s_%s.xls'%(self.today_row,type,str(int(self.now)))

    def Dealorder(self,dp_configs,tc_id):
        cursor = self.cursor
        for k in dp_configs:
            if k['dp_id']:
                if k['sku_ids']:
                    sku_id = k['sku_ids']
                else:
                    sku_id = '' 
                cursor.execute("select * from goods_tag_assign where goods_id=%s",k['dp_id'])
                dp_tags = cursor.fetchall()
                dp_tags = [i['tag_id'] for i in dp_tags if i['tag_id'] in self.tg_list]
                if dp_tags:
                    dp_tag_id = dp_tags[0]
                else:
                    dp_tag_id = 'None'
                cursor.execute("select * from order_dp where tc_goods_id=%s and cp_goods_id=%s and dp_goods_id=%s and sku_id=%s and weight=%s and handle=%s and delivery_time=%s",
                                (tc_id,k['cp_id'],k['dp_id'],sku_id,k['weight'],k['handle'],self.delivery_time))
                dp_exist = cursor.fetchone()
                if dp_exist:
                    all_weight = dp_exist['all_weight'] + k['weight']*k['count']
                    count = dp_exist['count']+k['count']
                    cursor.execute("update order_dp set all_weight=%s,count=%s where tc_goods_id=%s and cp_goods_id=%s and dp_goods_id=%s and sku_id=%s and weight=%s and handle=%s and delivery_time=%s",
                                    (all_weight,count,tc_id,k['cp_id'],k['dp_id'],sku_id,k['weight'],k['handle'],self.delivery_time))
                else:
                    all_weight = k['weight']*k['count']
                    count = k['count']
                    pre_weight = '%.2f'%float(k['weight']*0.002)
                    cursor.execute("insert into order_dp (tc_goods_id,cp_goods_id,weight,handle,count,dp_goods_id,sku_id,all_weight,pre_weight,tag,delivery_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                    (tc_id,k['cp_id'],k['weight'],k['handle'],k['count'],k['dp_id'],k['sku_ids'],all_weight,pre_weight,dp_tag_id,self.delivery_time))

    def SelectData(self,start_time,end_time):
        cursor = self.cursor
        cursor.execute("select * from goods_tag where name like 'HA%'")
        tg_list = cursor.fetchall()
        self.tg_name = [i['name'] for i in tg_list]
        self.tg_name.append('未分类')
        self.tg_list = [i['id'] for i in tg_list]
        cursor.execute("delete from order_dp where delivery_time < '%s 00:00'"%(self.weekago_row))
        cursor.execute("select * from order where order_state>1 and push_content not like '%%待支付%%' and push_content not like '%%积分兑换%%' and push_content like '%%\"delivery_start_time\":\"%s%%'"%self.today_row)
        today_order = cursor.fetchall()
        now_order = []
        for i in today_order:
            push_content = json.loads(i['push_content'])
            delivery_time = push_content['full_order_info']['address_info']['delivery_start_time']
            delivery_time_row = int(time.mktime(time.strptime(delivery_time, "%Y-%m-%d %H:%M:%S")))
            start_time_row = int(time.mktime(time.strptime(start_time, "%Y-%m-%d %H:%M:%S")))
            end_time_row = int(time.mktime(time.strptime(end_time, "%Y-%m-%d %H:%M:%S")))
            if start_time_row<delivery_time_row<end_time_row:
                now_order.append(i)
        for i in now_order:
            push_content = json.loads(i['push_content'])
            self.delivery_time = push_content['full_order_info']['address_info']['delivery_start_time']
            order_data = self.order_print.generate_order_data(0,i)
            for j in order_data['details']:
                tc_id = '0'
                dp_configs = [j]
                self.Dealorder(dp_configs,tc_id)
            for j in order_data['cp_config']:
                dp_configs = j['dp_config']
                if dp_configs:
                    tc_id = '0'
                    self.Dealorder(dp_configs,tc_id)
            for j in order_data['tc_config']:
                for k in j['tc_config']:
                    dp_configs = k['dp_config']
                    if dp_configs:
                        tc_id = j['tc_id']
                        self.Dealorder(dp_configs,tc_id)

    def execute_to(self,type):
        try:
            self.Config(type)
            conn = self.db.getConnect()
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            self.cursor = cursor
            if not (type =='total'):
                get_start_time = self.default_config['to_excel_time'][type+'_start_time']
                get_end_time = self.default_config['to_excel_time'][type+'_end_time']
            else:
                get_start_time = self.default_config['to_excel_time']['morning' + '_start_time']
                get_end_time = self.default_config['to_excel_time']['noon' + '_end_time']
            self.start_time = "%s %s"%(self.today_row,get_start_time)
            self.end_time = "%s %s"%(self.today_row,get_end_time)
            self.SelectData(self.start_time,self.end_time)
            df = pd.DataFrame()
            cursor.execute("select * from order_dp where delivery_time between '%s' and '%s'"%(self.start_time,self.end_time))
            today_order = cursor.fetchall()
            df = pd.DataFrame(columns=('配送时间','礼包名','组合名','商品名','规格','重量/g','PROCESS','累计数量','累计重量/g','商品重量/斤','分类'))
            for i in today_order:
                tc_name = ''
                cp_name = ''
                sku_name = ''
                handle_name = ''
                if i['tc_goods_id']:
                    cursor.execute("select name from goods where id=%s",i['tc_goods_id'])
                    tc_name = cursor.fetchone()['name']
                if i['cp_goods_id']:
                    cursor.execute("select name from goods where id=%s",i['cp_goods_id'])
                    cp_name = cursor.fetchone()['name']
                cursor.execute("select name from goods where id=%s",i['dp_goods_id'])
                dp_name = cursor.fetchone()['name']
                if i['sku_id']:
                    sku_list = i['sku_id'].split(':')
                    sku_names = []
                    for s in sku_list:
                        attr = s.split('_')[0]
                        item = s.split('_')[1]
                        cursor.execute("select name from goods_attr where id=%s",attr)
                        attr_name = cursor.fetchone()['name']
                        cursor.execute("select name from goods_attr_item where id=%s",item)
                        item_name = cursor.fetchone()['name']
                        sku_name = attr_name+':'+item_name
                        sku_names.append(sku_name)
                    sku_name = ','.join(sku_names)
                weight = i['weight']
                if i['handle']:
                    item = i['handle'].split('_')[1]
                    cursor.execute("select name from goods_attr_item where id=%s",item)
                    handle_name = cursor.fetchone()['name']
                count = i['count']
                all_weight = i['all_weight']
                pre_weight = i['pre_weight']
                if i['tag'] != 'None':
                    cursor.execute("select name from goods_tag where id=%s",i['tag'])
                    tag = cursor.fetchone()['name']
                else:
                    tag = '未分类'
                delivery_time = i['delivery_time']
                df = df.append(pd.DataFrame({'配送时间':delivery_time,'礼包名':tc_name,'组合名':cp_name,'商品名':dp_name,'规格':sku_name,'重量/g':weight,'PROCESS':handle_name,'累计数量':count,'累计重量/g':all_weight,'商品重量/斤':pre_weight,'分类':tag},index=[0]),ignore_index=True)
            with pd.ExcelWriter(self.file_name,engine='xlsxwriter') as writer:
                for i in self.tg_name:
                    df_sheet = df[df['分类']==i]
                    if not df_sheet.empty:
                        df_sheet.to_excel(writer,sheet_name=i, index=False, encoding='utf_8_sig')
                        worksheet = writer.sheets[i]
                        worksheet.set_column('A:D',20)
                        worksheet.set_column('E:K',12)
            #email
            subject = self.today_row +'_采购商品统计'
            content_text = self.today_row + '商品+组合统计'
            attachments = [self.file_name]
            receivers = []
            cc = []
            self.sendMail.send_email_to(subject, content_text, attachments, receivers, cc)

        except Exception as e:
            order_detail_logger.exception(e)
            subject = self.today_row+'统计异常'
            content_text = self.today_row +'\n'+ str(e)
            attachments = [self.file_name]
            receivers = []
            cc = []
            self.sendMail.send_email_to(subject, content_text, attachments, receivers, cc)
# -*- coding:utf-8 -*-
from libs.helper import *
from biz.order_control import OrderPrint
from toPic.excel_to_png import *
from biz.approval import *
import pandas as pd
import time

class RefundBill:
    def __init__(self):
        self.db = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.order_print = OrderPrint()
        self.excel_to_image = ExcelToImage()
        self.approval = Approval()

    def Config(self):
        self.now = time.time()
        self.week_before = self.now - 86400*7
        self.today_row = time.strftime('%Y-%m-%d',time.localtime(self.now))
        stampArray = time.strptime(self.today_row, "%Y-%m-%d")
        self.today_stamp = int(time.mktime(stampArray))
        self.weekago_row = time.strftime('%Y-%m-%d',time.localtime(self.week_before))
        self.path_name = self.default_config['to_image']['excel_path']
        self.file_name = self.path_name + '_%s_退款_%s.xls'%(self.today_row,str(int(self.now)))
        self.own_name = '_%s_退款_%s' % (self.today_row, str(int(self.now)))

    def execute_to(self,type):
        try:
            self.Config()
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
            startArray = time.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")
            self.start_stamp = int(time.mktime(startArray))
            endArray = time.strptime(self.end_time, "%Y-%m-%d %H:%M:%S")
            self.end_stamp = int(time.mktime(endArray))
            tag_list = []
            cursor.execute("select id,tid,dp_goods_id,dp_name,sku_id,sku_name,count,all_weight,tag,tag_name,delivery_time,unit_price from bill_refund where delivery_time between '%s' and '%s' order by id desc"%(self.start_stamp,self.end_stamp))
            # cursor.execute("select id,tid,dp_goods_id,dp_name,sku_id,sku_name,count,all_weight,tag,tag_name,delivery_time,unit_price from bill_refund order by id desc")
            today_order = cursor.fetchall()
            df = pd.DataFrame(columns=('日期','ORDERNUM','商品名','规格','累计数量','累计重量/g','分类','单位成本','总成本价'))
            for i in today_order:
                sku_name = ''
                if i['dp_goods_id']:
                    dp_name = i.get('dp_name','')
                if i['sku_id']:
                    sku_name = i.get('sku_name','')
                count = i['count']
                tid = i['tid']
                all_weight = i['all_weight']
                unit_price = i['unit_price'] if i['unit_price'] != None else '0.00'
                if i['all_weight'] > 0:
                    all_price = '%.2f' % (float(unit_price) / 500 * all_weight)
                else:
                    all_price = '%.2f' % (float(unit_price) * i['count'])
                tag_name = '未分类'
                if i['tag_name']:
                    tag_name = i['tag_name']
                if tag_name not in tag_list:
                    tag_list.append(tag_name)
                timeArray = time.localtime(int(i['delivery_time']))
                delivery_time = time.strftime("%Y--%m--%d", timeArray)
                df = df.append(pd.DataFrame({'日期':delivery_time,'ORDERNUM':tid,'商品名':dp_name,'规格':sku_name,'累计数量':count,'累计重量/g':all_weight,'分类':tag_name,'单位成本':unit_price,'总成本价':all_price},index=[0]),ignore_index=True)
                if df.empty:
                    continue

            for t in tag_list:
                time.sleep(10)
                sum_col = 0
                row = 0
                df_sheet = df[df['分类'] == t]
                x = df_sheet.copy()
                if not x.empty:
                    for pindex, j in enumerate(x['总成本价']):
                        row += 1
                        sum_col += float(j)
                    x.loc[row*2, '总成本价'] = '%.2f' % sum_col
                    x.loc[row*2, '日期'] = '合计'
                    with pd.ExcelWriter(self.path_name + t + self.own_name + '.xls',engine='xlsxwriter') as writer:
                        x.to_excel(writer,sheet_name=t, index=False, encoding='utf_8_sig')
                        worksheet = writer.sheets[t]
                        worksheet.set_column('A:D',20)
                        worksheet.set_column('E:K',12)
                    self.excel_to_image.to_one_image(self.path_name+t+self.own_name+'.xls',t+self.own_name)
                    self.approval.execute_to(['excel/'+t+self.own_name+'.xls','image/'+t+self.own_name+'.jpg'],self.today_row+' '+t,'%.2f' % sum_col,'refund')
        except Exception as e:
            order_detail_logger.exception(e)

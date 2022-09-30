# -*- coding:utf-8 -*-
from libs.helper import *
from biz.approval import *
from biz.order_control import OrderPrint
from toPic.excel_to_png import *
import pandas as pd
import decimal
import time

class NormalBill:
    def __init__(self):
        self.db = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.order_print = OrderPrint()
        self.excel_to_image = ExcelToImage()
        self.approval = Approval()
        self.toBillTag = []
        self.bill_tag = []

    def Config(self):
        self.now = time.time()
        self.week_before = self.now - 86400*7
        self.today_row = time.strftime('%Y-%m-%d',time.localtime(self.now))
        stampArray = time.strptime(self.today_row, "%Y-%m-%d")
        self.today_stamp = int(time.mktime(stampArray))
        self.weekago_row = time.strftime('%Y-%m-%d',time.localtime(self.week_before))
        self.path_name = self.default_config['to_image']['excel_path']
        self.own_name = '_%s_结算_%s'%(self.today_row,str(int(self.now)))

    def getBillTag(self, cursor,lim):
        cursor.execute("SELECT id,tag_id,merch_id,merch_name,payee,account_num,is_check,recon_cycle,final_recon,status from merchants where final_recon + recon_cycle+86400  = %s and status=1 and is_check=1 order by id desc limit %s"%(self.start_stamp,lim))
        bill_tag = cursor.fetchall()
        while len(bill_tag) > 0:
            self.bill_tag = [i['tag_id'] for i in bill_tag]
            getTag = "select id,name from goods_tag where id in (%s)" % ','.join(['%s'] * len(self.bill_tag))
            cursor.execute(getTag, self.bill_tag)
            bill_tg_id = []
            bill_tg_name = []
            bill_tg_names = cursor.fetchall()
            for tg in bill_tg_names:
                bill_tg_id.append(tg['id'])
                bill_tg_name.append(tg['name'])
            bill_tg_list = dict(zip(bill_tg_id,bill_tg_name))
            for i in bill_tag:
                i['tag_name'] = bill_tg_list[i['tag_id']]
            return bill_tag

    def execute_to(self, type, lim=10):
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
            order_detail_logger.exception(self.start_time)
            order_detail_logger.exception(self.end_time)
            startArray = time.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")
            self.start_stamp = int(time.mktime(startArray))
            endArray = time.strptime(self.end_time, "%Y-%m-%d %H:%M:%S")
            self.end_stamp = int(time.mktime(endArray))
            cursor.execute("select * from goods_tag where name like 'HA%'")
            tg_list = cursor.fetchall()
            self.tg_name = [i['name'] for i in tg_list]
            self.tg_name.append('未分类')
            self.tg_list = [i['id'] for i in tg_list]
            today_bill = self.getBillTag(self.cursor,lim)  # 获取打印列表
            if not today_bill:
                return 0
            for tb in today_bill:
                cursor.execute('update merchants set final_recon = %s where id = %s', (self.today_stamp, tb['id']))
                conn.commit()
                if self.start_stamp-int(tb['recon_cycle'])-86400 != int(tb['final_recon']):
                    continue
                begin_stamp = ''
                begin_stamp = time.strftime('%Y-%m-%d',time.localtime(int(self.start_stamp - tb['recon_cycle'])))
                if begin_stamp == self.today_row:
                    begin_stamp = ''
                cursor.execute("select dp_name,dp_goods_id,sku_id,sku_name,unit_price,tag,tag_name,sum(all_weight) as all_weight,sum(reduce_weight) as all_reduce_weight,sum(count) as all_count,sum(reduce_count) as all_reduce_count,delivery_time from bill_dp where delivery_time between '%s' and '%s' and tag = '%s' GROUP BY sku_id, unit_price, dp_goods_id ORDER BY dp_goods_id desc"%(self.start_stamp-int(tb['recon_cycle']), self.end_stamp, tb['tag_id']))
                today_order = cursor.fetchall()
                df = pd.DataFrame(columns=('日期','商品名','规格','累计数量','累计重量/g','分类','单位成本/500g','总成本价'))
                for i in today_order:
                    sku_name = ''
                    dp_name = i.get('dp_name','')
                    if i['sku_id']:
                        sku_name = i.get('sku_name','')
                    reduce_weight = i['all_reduce_weight'] if i['all_reduce_weight'] != None else 0
                    reduce_count = i['all_reduce_count'] if i['all_reduce_count'] != None else 0
                    all_count = i['all_count'] - reduce_count
                    all_weight = i.get('all_weight',0) - reduce_weight
                    unit_price = i.get('unit_price',0)
                    if unit_price==None:
                        unit_price = 0
                    if int(i['all_weight']) > 0:
                        all_price = '%.2f' % (decimal.Decimal(unit_price)/500 * all_weight)
                    else:
                        all_price = '%.2f' % (decimal.Decimal(unit_price) * all_count)
                    if i['tag'] != 'None':
                        tag = i.get('tag_name','')
                    else:
                        tag = '未分类'
                    timeArray = time.localtime(int(i['delivery_time']))
                    delivery_time = time.strftime("%Y--%m--%d", timeArray)
                    df = df.append(pd.DataFrame({'日期':delivery_time,'商品名':dp_name,'规格':sku_name,'累计数量':all_count,'累计重量/g':all_weight,'分类':tag,'单位成本/500g':unit_price,'总成本价':all_price},index=[0]),ignore_index=True)
                    if df.empty:
                        continue
                sum_col = 0
                row = 0
                x = df.copy()
                if not x.empty:
                    for pindex, j in enumerate(x['总成本价']):
                        row += 1
                        sum_col += float(j)
                    x.loc[row*2, '总成本价'] = '%.2f' % sum_col
                    x.loc[row*2, '日期'] = '合计'
                    with pd.ExcelWriter(self.path_name+tag+begin_stamp+self.own_name+'.xls',engine='xlsxwriter') as writer:
                        x.to_excel(writer,sheet_name=tag, index=False, encoding='utf_8_sig')
                        worksheet = writer.sheets[tag]
                        worksheet.set_column('A:D',20)
                        worksheet.set_column('E:K',12)
                    self.excel_to_image.to_one_image(self.path_name+tag+begin_stamp+self.own_name+'.xls',tag+begin_stamp+self.own_name)
                    self.approval.execute_to(['excel/'+tag+self.own_name+'.xls','image/'+tag+begin_stamp+self.own_name+'.jpg'],tag+begin_stamp+'_'+self.own_name,'%.2f' % sum_col,'settlement',tb['account_num'],tb['payee'])

        except Exception as e:
            order_detail_logger.exception(e)



from libs.helper import *
from biz.send_email import *
from biz.deal_order_goods import *
import time
from datetime import date, timedelta
class OrderGoodsDetail:
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.cursor = None
        self.sendMail = SendMail()
        self.dealGoods = DealOrderGoods()
    def execute_to(self,type):
        now = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        # time.strftime("%Y-%m-%d", time.localtime())
        if not (type =='total'):
            get_start_time = self.default_config['to_excel_time'][type+'_start_time']
            get_end_time = self.default_config['to_excel_time'][type+'_end_time']
        else:
            get_start_time = self.default_config['to_excel_time']['morning' + '_start_time']
            get_end_time = self.default_config['to_excel_time']['afternoon' + '_end_time']

        outfilePath = self.default_config['toexcel']['goods_detail_path']
        outfileName = outfilePath + "/" + now + "goods_detail_"+type+".csv"
        outfile = "'"+outfileName+"'"

        start_time = "'"+now+" "+get_start_time+"'"
        end_time = "'"+now+" "+get_end_time+"'"

        sql = "select delivery_start_time as '配送时间',goods_name as '商品名称',spec_value as '规格',spec_val as '重量',type_val as 'PROCESS',sum(num) as '数量', if(weight=1,sum(total_num),sum(total_num)/500) as '总重量',if(weight=1,sum(weight),sum(weight)/500) as '计量单位',cate_name as '分类' into outfile  "+outfile+" character set gbk fields terminated by ',' optionally enclosed by '\"' lines terminated by '\n' from order_detail  where delivery_start_time between "+start_time+" and "+end_time+" group by goods_name,spec_value,type_val ORDER BY id DESC"
        try:
            conn = self.db_helper.getConnect()
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            self.cursor = cursor
            cursor.execute(sql)
            cursor.close()
            re_file_name = self.dealGoods.execute_to(outfileName)
            subject = now+'采购商品统计'
            content_text = '配送时间，商品名称，规格，重量，PROCESS，累计数量，累计总量，单位总量，分类'
            attachments = [re_file_name]
            receivers = ['3423@qq.com']
            cc = ['']
            self.sendMail.send_email_to(subject, content_text, attachments, receivers, cc)
        except pymysql.Error as e:
            re_file_name = self.dealGoods.execute_to(outfileName)
            error = 'MySQL execute failed! ERROR (%s): %s' % (e.args[0], e.args[1])
            subject = '发送采购邮件失败!error'
            content_text = now + error
            attachments = [re_file_name]
            receivers = ['3423@qq.com']
            cc = []
            self.sendMail.send_email_to(subject, content_text, attachments, receivers, cc)

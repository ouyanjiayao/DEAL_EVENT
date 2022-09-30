from libs.helper import *
from biz.send_email import *
from biz.deal_order_goods import *
import time
class GoodsDetail:
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.cursor = None
        self.sendMail = SendMail()
        self.dealGoods = DealOrderGoods()
    def execute_to(self,type):
        now = time.strftime("%Y-%m-%d", time.localtime())
        if not (type =='total'):
            get_start_time = self.default_config['to_excel_time'][type+'_start_time']
            get_end_time = self.default_config['to_excel_time'][type+'_end_time']
        else:
            get_start_time = self.default_config['to_excel_time']['morning' + '_start_time']
            get_end_time = self.default_config['to_excel_time']['noon' + '_end_time']

     
        outfilePath = self.default_config['toexcel']['goods_detail_path']
        outfileName = outfilePath + "/" + now + "goods_detail_"+type+".csv"
        outfile = "'"+outfileName+"'"

        sql = "select dp_name as '商品名称',sku_name as '规格',weight as '重量',sum(weight*count) as '总重量',sum(count) as '数量', tag_name as '分类' into outfile  "+outfile+" character set gbk fields terminated by ',' optionally enclosed by '\"' lines terminated by '\n' from bill_dp_on group by dp_name,sku_name ORDER BY id DESC"
        try:
            conn = self.db_helper.getConnect()
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            self.cursor = cursor
            cursor.execute(sql)
            cursor.close()
            re_file_name = self.dealGoods.execute_to(outfileName)
            subject = now+'采购商品统计'
            content_text = now
            attachments = [re_file_name]
            receivers = []
            cc = ['']
            self.sendMail.send_email_to(subject, content_text, attachments, receivers, cc)
        except pymysql.Error as e:
            re_file_name = self.dealGoods.execute_to(outfileName)
            error = 'MySQL execute failed! ERROR (%s): %s' % (e.args[0], e.args[1])
            subject = now+'采购商品统计'
            content_text = now + error
            attachments = [re_file_name]
            receivers = ['3423@qqq.com']
            cc = ['']
            self.sendMail.send_email_to(subject, content_text, attachments, receivers, cc)

from libs.helper import *
from biz.send_email import *
import time
class OrderGoodsDetail:
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.sendMail = SendMail()
    def execute_to(self):
        now = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
       
        outfilePath = self.default_config['toexcel']['goods_detail_path']
        file_list = []
        totalName = outfilePath + "/" + now + "goods_detail_total_diff.xlsx"
        morningName = outfilePath + "/" + now + "goods_detail_morning_diff.xlsx"
        afternoonName = outfilePath + "/" + now + "goods_detail_afternoon_diff.xlsx"
        if os.path.exists(totalName):
            if (os.path.getsize(totalName) > 0):
                file_list.append(totalName)

        if os.path.exists(morningName):
            if (os.path.getsize(morningName) > 0):
                file_list.append(morningName)

        if os.path.exists(afternoonName):
            if (os.path.getsize(afternoonName) > 0):
                file_list.append(afternoonName)

        subject = now+'采购商品统计（截至现在数据）'
        content_text = ' 总表：'+totalName+'\r\n 上午：'+morningName+'\r\n 下午：'+afternoonName
        attachments = file_list
        receivers = ['3423@qqq.com']
        cc = []
        self.sendMail.send_email_to(subject, content_text, attachments, receivers, cc)

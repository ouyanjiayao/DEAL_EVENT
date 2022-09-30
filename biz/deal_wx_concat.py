from libs.helper import *
import pandas as pd
import time

class DealWxConcat:
    def execute_to(self, file_name):
        try:
            df = pd.read_csv(file_name, encoding='gbk',header=None)
            df.fillna('', inplace=True)
            re_file_name = file_name.replace(".csv", str(int(time.time())) + ".xlsx")
            writer = pd.ExcelWriter(re_file_name)
            df.columns = ['推广员', '昵称', '性别', '客服号', '添加时间']
            df.to_excel(writer, index=False, encoding='utf_8_sig',header=1)
            writer.save()
            return re_file_name
        except Exception as e:
            syn_logger.exception(e)
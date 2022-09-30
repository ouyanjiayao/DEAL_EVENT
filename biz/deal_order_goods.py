import pandas as pd
import os
import time
class DealOrderGoods:
    def execute_to(self, file_name):
        try:
            data = pd.read_csv(file_name, encoding='gbk')
            rows = data.shape[0]  # 获取行数 shape[1]获取列数
            goods_list = []
            data.columns = ['商品名称', '规格', 'PROCESS', '重量', '累计重量', '累计数量', '分类']
            data.fillna('', inplace=True)
            x = data.copy()
            for i in range(rows):
                if x.iloc[i, 6] == '':
                    x.iloc[i, 6] = '未分类'
                temp = x.iloc[i, 6]
                if temp not in goods_list:  # 防止重复
                    goods_list.append(temp)  # 将分类存在一个列表中
            n = len(goods_list)  # 商品数
            df_list = []
            df_data = {}
            for i in range(n):
                df_data[i] = pd.DataFrame()
                df_list.append(df_data[i])

            for categories in range(n):
                for i in range(0, rows):
                    if x.iloc[i, 6] == goods_list[categories]:
                        df_list[categories] = pd.concat([df_list[categories], x.iloc[[i], :]], axis=0, ignore_index=True)

            re_file_name = file_name.replace(".csv", str(int(time.time()))+".xlsx")
            writer = pd.ExcelWriter(re_file_name)  # 利用pd.ExcelWriter()存多张sheets
            for i in range(n):
                df_list[i].to_excel(writer, sheet_name=str(goods_list[i]), index=False, encoding='utf_8_sig')  # 注意加上index=FALSE 去掉index列
            writer.save()
            return re_file_name
        except Exception as e:
            order_detail_logger.exception(e)
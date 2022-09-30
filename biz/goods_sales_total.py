import sys
from libs.helper import *
from libs.youzan import ApiClient
import time
import gc

class GetGoodsSales:
    def __init__(self):
        self.db_helper = DBHelper()
        self.api = ApiClient()

    def execute(self):
        self.conn_init()
        cursor = self.cursor
        # 查询类型为组合的商品
        cursor.execute('select id,id from goods where is_delete = 0 and type = 2 and id is not null order by id desc')
        cp_goods = cursor.fetchall()
        self.conn.close()
        updates = []
        update = []
        error = []
        try:
            for item in cp_goods:
                response = self.api.invoke('youzan.item.detail.get', '1.0.0', {
                    'item_id' : item['id']
                })
                if response['success'] != True:
                    goods_sales_logger.exception(response['message'])
                if response['success'] == True:
                    sku_list = response['data']
                    update.append((sku_list['sold_num'], int(time.time()), item['id']))
                    # 要分批次执行，一起执行数据量过大，数据库报错
                    if len(update) >= 10:
                        updates.append(update)
                        update = []
                else:
                    error.append((item))
                gc.collect()
        except Exception as e:
            goods_sales_logger.exception(e)

        # 以免面循环中，最后的数量不够50
        if len(update) > 0:
            updates.append(update)

        # 记录请求有赞接口出错的组合
        if len(error) > 0:
            goods_sales_logger.exception("有赞请求错误")
            goods_sales_logger.exception(error)

        # 执行语句
        self.conn_init()
        error = 0
        try:
            if len(updates) > 0:
                for update in updates:
                    self.cursor.executemany("update goods set sales_volume = %s,updated_time = %s where id = %s",update)
                    gc.collect()
            error += 1
            self.conn.close()
            goods_sales_logger.exception("完成")

        except Exception as e:
            self.conn.close()
            str_err = "操作数据错误：" + str(error)
            goods_sales_logger.exception(str_err)
            goods_sales_logger.exception(e)


    def conn_init(self):
        self.conn = self.db_helper.getConnect()
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)


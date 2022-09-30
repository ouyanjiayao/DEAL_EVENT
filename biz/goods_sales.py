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
        # 查询类型为组合的上架商品
        cursor.execute('select * from goods where is_delete = 0 and type = 2 and id is not null')
        cp_goods = cursor.fetchall()
        # 查询商品销售的关联表
        cursor.execute('select * from goods_sales')
        goods_sales = cursor.fetchall()
        # 对比关联表和组合的数据
        cp_goods_id = [item.get('id') for item in cp_goods]
        goods_sales_id = [item.get('goods_id') for item in goods_sales]
        # 商品销售数量表中有的，商品表查询出来没有的，需要删除
        del_goods_sale_id = [i for i in goods_sales_id if i not in cp_goods_id]
        # 商品表查询出来有的，商品销售数量表中没有的，需要添加
        insert_goods_sale_id = [i for i in cp_goods_id if i not in goods_sales_id]
        self.conn.close()
        #
        updates = []
        inserts = []
        update = []
        insert = []
        error = []
        try:
            for item in cp_goods:
                response = self.api.invoke('youzan.item.detail.get', '1.0.0', {
                    'item_id' : item['id']
                })
                if response['success'] == True:
                    sold_num = 0
                    for sku_list in response['data']['sku_list']:
                        if item['id'] in insert_goods_sale_id:
                            insert.append((item['id'], sku_list['sku_no'], sku_list['sold_num'], item['created_time'], int(time.time())))
                        else:
                            update.append((sku_list['sold_num'], int(time.time()), item['id'], sku_list['sku_no']))
                    # 要分批次执行，一起执行数据量过大，数据库报错
                    if len(insert) >= 10:
                        inserts.append(insert)
                        insert = []
                    if len(update) >= 10:
                        updates.append(update)
                        update = []
                else:
                    error.append((item))
                gc.collect()
        except Exception as e:
            goods_sales_logger.exception(e)
            exit()

        # 以免面循环中，最后的数量不够50
        if len(insert) > 0:
            inserts.append(insert)
        if len(update) > 0:
            updates.append(update)

        # 记录请求有赞接口出错的组合
        if len(error) > 0:
            goods_sales_logger.exception("有赞请求错误")
            goods_sales_logger.exception(error)

        # 执行语句
        self.conn_init()
        cursor = self.cursor

        error = 0
        try:
            if len(updates) > 0:
                for update in updates:
                    self.cursor.executemany("update goods_sales set sales_volume = %s,updated_time = %s where goods_id = %s and sku_id = %s",update)
                    gc.collect()
            error += 1
            if len(del_goods_sale_id) > 0:
                cursor.executemany("delete from goods_sales where goods_id = %s", (del_goods_sale_id))
            error += 1
            if len(insert) > 0:
                for insert in inserts:
                    cursor.executemany("insert into goods_sales(goods_id,sku_id,sales_volume,goods_created_time,updated_time) values(%s,%s,%s,%s,%s)", insert)
                    gc.collect()
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


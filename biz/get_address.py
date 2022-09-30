from libs.helper import *
import gc
import json

class MyAddress:
    def __init__(self):
        self.db_helper = DBHelper()

    def execute_to(self):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute("SELECT a.id,a.push_content,a.created_time,a.delivery_start_time,a.tid,b.azimuth,b.order_id,b.distance FROM order a right JOIN  order_purchase b ON a.id= b.order_id WHERE a.state = 0 and b.azimuth is not null and b.distance is not null LIMIT 10")
        res = cursor.fetchall()
        conn.close()
        inserts = []
        updates = []
        if not res:
            exit()
        else:
            try:
                for i in res:
                    push_content = ''
                    address = ''
                    push_content = json.loads(i['push_content'])
                    address = push_content['full_order_info']['address_info']['delivery_address']

                    inserts.append((int(i['distance']), i['azimuth'], address, i['tid'], int(i['order_id']), int(i['created_time']), int(i['delivery_start_time'])))
                    updates.append((1, i['id']))
                    gc.collect()
                    print(inserts)
                if len(inserts) > 0:
                    conn = self.db_helper.getConnect()
                    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
                    cursor.executemany("insert into address(distance,azimuth,address,tid,order_id,created_time,delivery_time) values(%s,%s,%s,%s,%s,%s,%s)", inserts)
                    conn.commit()
                    cursor.executemany("update order set state = %s where id = %s", updates)
                    conn.commit()
                    conn.close()
            except Exception as e:
                order_control_logger.exception(e)



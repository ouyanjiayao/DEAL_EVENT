from libs.helper import *
from libs.youzan import ApiClient
import json
import time

class GoodsGroupSyn:

    def __init__(self):
        self.db_helper = DBHelper()
        self.api = ApiClient()

    def execute_to(self,limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute('select switch_state from syn_switch where switch_name=\"group_syn\"')
        is_run = cursor.fetchone()
        if not is_run['switch_state']:
            pass
        else:
            # cursor.execute('select * from goods_tag where is_delete = 0 and (version is null or version!=version) order by id asc limit %s',(limit))
            cursor.execute('select * from goods_tag where is_delete = 0 and (version is null or version!=version) and type = 1 order by id asc limit %s',(limit))
            rows = cursor.fetchall()
            updates = []
            error_updates = []
            log_inserts = []
            if rows:
                for row in rows:
                    api_name = None
                    syn_state = 1
                    response = None
                    try:
                        id = row['id']
                        if not id:
                            api_name = 'youzan.itemcategories.tag.add'
                            response = self.api.invoke(api_name, '3.0.0', {
                                'name': row['name']
                            })
                            if not (response['code'] == 200):
                                syn_state = 2
                            else:
                                id = response['data']['tag']['id']
                                syn_state = 3
                        else:
                            api_name = 'youzan.itemcategories.tag.update'
                            response = self.api.invoke(api_name, '3.0.0', {
                                'name': row['name'],
                                'tag_id': id
                            })
                            if not (response['code'] == 200):
                                syn_state = 2
                            else:
                                syn_state = 3
                        updates.append((id, row['version'],time.time(),syn_state, row['id']))
                    except Exception as e:
                        syn_logger.exception(e)
                    if syn_state == 1:
                        error_updates.append((time.time(),syn_state, row['id']))
                    if response:
                        response_content = json.dumps(response)
                    else:
                        response_content = '接口调用失败'
                    log_inserts.append((response_content,time.time(),syn_state,row['id'],syn_state,api_name))
                    time.sleep(2)
                if len(updates) > 0:
                    cursor.executemany("update goods_tag set id = %s,version = %s,syn_time = %s,syn_state = %s where id = %s",updates)
                    conn.commit()
                if len(error_updates) > 0:
                    cursor.executemany("update goods_tag set syn_time = %s, syn_state = %s where id = %s",error_updates)
                    conn.commit()
                if len(log_inserts) > 0:
                    cursor.executemany("insert into syn_log(response_content,created_time,type,syn_id,syn_state,api_name) values(%s,%s,%s,%s,%s,%s)",log_inserts)
                    conn.commit()
        conn.close()

    def execute_delete(self,limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute('select switch_state from syn_switch where switch_name=\"group_delete_syn\"')
        is_run = cursor.fetchone()
        if not is_run['switch_state']:
            pass
        else:
            cursor.execute('select id,id from goods_tag where is_delete = 1 order by id asc limit %s',(limit))
            rows = cursor.fetchall()
            deletes = []
            log_inserts = []
            if rows:
                for row in rows:
                    api_name = None
                    response = None
                    syn_state = 1
                    try:
                        id = row['id']
                        if not id:
                            deletes.append((row['id']))
                        else:
                            api_name = 'youzan.itemcategories.tag.delete'
                            response = self.api.invoke(api_name, '3.0.0', {
                                'tag_id': id
                            })
                            if not (response['code'] == 200):
                                syn_state = 2
                            else:
                                deletes.append((row['id']))
                                syn_state = 3
                    except Exception as e:
                        syn_logger.exception(e)
                    if response:
                        response_content = json.dumps(response)
                    else:
                        response_content = '接口调用失败'
                    log_inserts.append((response_content, time.time(), 2, row['id'], syn_state, api_name))
                cursor.executemany('delete from goods_tag where id = %s', deletes)
                conn.commit()
                if len(log_inserts) > 0:
                    cursor.executemany("insert into syn_log(response_content,created_time,type,syn_id,syn_state,api_name) values(%s,%s,%s,%s,%s,%s)",log_inserts)
                    conn.commit()
        conn.close()

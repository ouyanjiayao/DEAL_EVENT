from libs.helper import *
from libs.youzan import ApiClient
from libs.helper import ConfigHelper
import json
import time

class UploadImageSyn:

    def __init__(self):
        self.db_helper = DBHelper()
        self.api = ApiClient()
        self.default_config = ConfigHelper.getDefault()

    def execute_to(self,limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute('select switch_state from syn_switch where switch_name=\"pic_syn\"')
        is_run = cursor.fetchone()
        if not is_run['switch_state']:
            pass
        else:
            # cursor.execute('select * from upload_file where id=2841')
            cursor.execute('select * from upload_file where is_delete = 0 and syn_time = 0 and (version is null or version!=version) order by id asc limit %s',(limit))
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
                        image_url = row['url'].replace('@web', self.default_config['web']['upload_dir'])
                        if not row['id']:
                            id = None
                            files = {'image': open(image_url, 'rb')}
                            api_name = 'youzan.materials.storage.platform.img.upload'
                            response = self.api.invoke(api_name, '3.0.0',files=files)
                            if not (response['code'] == 200):
                                syn_state = 2
                            else:
                                id = response['data']['image_id']
                                url = response['data']['image_url']
                                syn_state = 3
                                updates.append((id,url, row['version'],time.time(),syn_state, row['id']))
                    except Exception as e:
                        syn_state = 1
                        syn_logger.exception(e)
                    if syn_state in [1,2]:
                        error_updates.append((time.time(),syn_state, row['id']))
                    if response:
                        response_content = json.dumps(response)
                    else:
                        response_content = '接口未调用'
                    log_inserts.append((response_content,time.time(),3,row['id'],syn_state,api_name))
                    time.sleep(1)
                if len(updates) > 0:
                    cursor.executemany("update upload_file set id = %s,url = %s,version = %s,syn_time = %s,syn_state = %s where id = %s",updates)
                    conn.commit()
                if len(error_updates) > 0:
                    cursor.executemany("update upload_file set syn_time = %s, syn_state = %s where id = %s",error_updates)
                    conn.commit()
                if len(log_inserts) > 0:
                    cursor.executemany("insert into syn_log(response_content,created_time,type,syn_id,syn_state,api_name) values(%s,%s,%s,%s,%s,%s)",log_inserts)
                    conn.commit()
            cursor.close()
        # conn.close()
        conn.close()


    def execute_delete(self):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        cursor.execute('select switch_state from syn_switch where switch_name=\"delete_pic_syn\"')
        is_run = cursor.fetchone()
        if not is_run['switch_state']:
            pass
        else:
            cursor.execute('delete from upload_file where is_delete = 1')
            conn.commit()
        cursor.close()
        # conn.close()
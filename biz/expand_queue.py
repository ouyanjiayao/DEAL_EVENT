from libs.helper import *
import json
import requests
class ExpandQueue:
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.redis_helper = RedisHelper()
        self.redis = self.redis_helper.getConnect()
        self.cursor = None

    def expand(self):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        try:
            if not self.redis:
                self.redis = self.redis_helper.getConnect()
            token_key = 'token'
            token = self.redis.get(token_key)
            token = self.to_str(token)
            if token:
                external_id = self.redis.lpop("external_uid")
                external_uid = self.to_str(external_id)
                if external_uid:
                    info = self.get_external_concat(token, external_uid)
                    wc_customer = []
                    if info['errcode'] == 0:
                        exter_contact = info.get('external_contact')
                        follow_user = info.get('follow_user')
                        for pindex, j in enumerate(follow_user):
                            state = ''
                            state = j.get('state', '')
                            cursor.execute('select id,external_userid,inner_user_id from wechat_customer where external_userid=%s and inner_user_id = %s',(external_uid,j.get('userid')))
                            is_exist = cursor.fetchone()
                            if not is_exist:
                                wc_customer.append((state,j.get('createtime'),exter_contact.get('external_userid'),exter_contact.get('unionid'),j.get('remark'),j.get('userid'),exter_contact.get('avatar'),exter_contact.get('gender')))
                   
                    if len(wc_customer) > 0:
                        try:
                            wc_sql = "insert into wechat_customer (code_id,created_time,external_userid,union_id,external_name,inner_user_id,avatar,gender) values(%s,%s,%s,%s,%s,%s,%s,%s)"
                            self.cursor.executemany(wc_sql, wc_customer)
                            conn.commit()
                        except Exception as e:
                            syn_logger.exception(e)
                        self.cursor.close()
        except Exception as e:
            syn_logger.exception(e)

    def get_external_concat(self, token, external_uid):
        wc_url = self.default_config['wc']['url_header']+"externalcontact/get?access_token="+str(token)+"&external_userid="+str(external_uid)
        info = requests.get(wc_url)
        res = json.loads(info.text)
        return res

    def to_str(self, bytes_or_str):
        if isinstance(bytes_or_str, bytes):
            value = bytes_or_str.decode("utf-8")
        else:
            value = bytes_or_str
        return value
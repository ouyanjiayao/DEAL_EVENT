import requests
import json
import time
import math
from libs.helper import RedisHelper,ConfigHelper
from urllib import request,parse
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

class ApiClient:

    http_host = 'https://open.youzanyun.com'

    def __init__(self):
        self.redis_helper = RedisHelper()
        self.default_config = ConfigHelper.getDefault()

    def getAccessToken(self):
        redis = self.redis_helper.getConnect()
        access_token = redis.get('access_token')
        if not access_token:
            http_url = self.http_host + '/auth/token'
            data = {
                'client_id': self.default_config['youzan']['client_id'],
                'client_secret':  self.default_config['youzan']['client_secret'],
                'authorize_type': self.default_config['youzan']['authorize_type'],
                'grant_id': self.default_config['youzan']['grant_id']
            }
            re_data = requests.post(http_url, data=json.dumps(data),headers={'Content-Type':'application/json'})
            token_info = re_data.json()
            access_token = token_info['data']['access_token']
            expires = math.ceil(token_info['data']['expires']/1000 - time.time())
            if not (expires <= 10):
                redis.set('token', access_token, expires)
        else:
             access_token = access_token.decode('utf-8')
        return access_token

    def invoke(self,apiName,version,data={},files=None):
        access_token = self.getAccessToken()
        http_url = self.http_host + '/api/'+ apiName + '/' + version + '?access_token='+access_token
        if files:
            data = requests.post(url=http_url,data=data,  files=files)
        else:
            data = requests.post(url=http_url, data=json.dumps(data),headers={'Content-Type':'application/json'})
        data = data.json()
        return data
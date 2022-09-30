import threading
from Lib.Config.config import Config
from Lib.Oauth.oauth import Oauth
from Lib.Protocol.rpc_client import RpcClient
from Lib.Api.yly_print import YlyPrint
from libs.helper import RedisHelper,ConfigHelper

class PrintDevice:

    get_token_lock = threading.Lock()

    def __init__(self,id,secret,device_id):
        self.device_id = device_id
        self.redis_helper = RedisHelper()
        self.print_app_config = Config(id,secret)

    def getPrintApiAccessToken(self):
        access_token = None
        self.get_token_lock.acquire()
        try:
            redis = self.redis_helper.getConnect()
            access_token = redis.get('token')
            if not access_token:
                oauth_client = Oauth(self.print_app_config)
                token_data = oauth_client.get_token()
                access_token = token_data['body']['access_token']
                redis.set('token', access_token, 14400)
        finally:
            self.get_token_lock.release()
        return access_token

    def printContent(self, content, order_id):
        access_token = self.getPrintApiAccessToken()
        rpc_client = RpcClient(self.print_app_config, access_token)
        print_service = YlyPrint(rpc_client)
        if self.device_id == 0:
            return 0
        response = print_service.index(self.device_id, content, order_id)
        if response['error_description'] == 'success':
            return response
        else:
            raise Exception('打印接口调用异常')

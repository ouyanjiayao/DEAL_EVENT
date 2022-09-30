# -*- coding:utf-8 -*-
from libs.helper import *
import requests

class Approval:
    def __init__(self):
        self.default_config = ConfigHelper.getDefault()

    def execute_to(self,flie_name=[],info='',sum_col=0,approval_type='settlement',account_num='',payee=''):
        try:
            url = self.default_config['approval']['approval_url']
            payload = {
              'creator_userid': '',
              'info': info,
              'approval_money': sum_col,
              'pay_type': payee,
              'pay_user': '@g',
              'appr_type': approval_type,
              'account_num': account_num
            }
            for index, i in enumerate(flie_name):
                payload['file_name[%d]' % index] = i
            requests.adapters.DEFAULT_RETRIES = 5
            s = requests.session()
            s.keep_alive = False
            res = requests.post(url, data=payload)
            order_detail_logger.exception(res)
        except Exception as e:
            order_detail_logger.exception(e)

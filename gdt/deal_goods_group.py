from libs.helper import *
from libs.youzan import ApiClient
import json
import time
import os
import re
import shutil
class DealGoodsGroupSyn:

    def __init__(self):
        self.db_helper = DBHelper()
        self.api = ApiClient()
        self.default_config = ConfigHelper.getDefault()
        self.goods_group_path = self.default_config['goods_group']['goods_group_path']
        self.goods_path = self.default_config['goods_group']['goods_path']
        self.group_file_name = self.default_config['goods_group']['group_file_name']
    def execute_to(self, page):
        path = self.goods_group_path
        file_path = self.goods_path + '/' + self.group_file_name
        try:
            if os.path.isdir(file_path):
                shutil.rmtree(file_path, ignore_errors=True)  ##先删除整个文件夹
          
            if not os.path.exists(file_path):
                os.mkdir(file_path)

            with open(path, 'r+') as group_file:
                file_content = group_file.read()
            file_content = json.loads(file_content)
            data_tags = file_content['data']['tags']
            group_file.close()
        except (IOError, KeyError) as e:
            tags_logger.exception(e)

        api_name = 'youzan.showcase.render.api.listGoodsByTagId'
        tags_inserts = []
        for i, tags in enumerate(data_tags):
            if self.filter_tg(data_tags[i]['name']):
                self.getListGoodsByTagId(api_name, page, data_tags[i], file_path)

    def getListGoodsByTagId(self, api_name, page, data_tags, gd_path):
        response = self.api.invoke(api_name, '1.0.0', {
            'tag_id': data_tags['id'],
            'page': page,
            'page_size': 100
        })
        data_tags['name'] = self.filter_name(data_tags['name'])
        out_path = gd_path + '/'+data_tags['name']+'_'+str(page)+'.json'
        try:
            if not response['data']['list']:
                pass
            else:
                res = json.dumps(response)
                with open(out_path, 'w+') as file_object:
                    file_object.write(res)
                file_object.close()
                if not response['data']['has_more']:
                    pass
                else:
                    self.getListGoodsByTagId(api_name, page + 1, data_tags, gd_path)
        except (IOError, KeyError) as e:
            tags_logger.exception(e)

    def filter_name(self, filter_name):
        re_name = filter_name.replace('/', '').replace(':', '').replace('*', '').replace('?', '').replace('"', '').replace('<', '').replace('>', '').replace('|', '')
        return re_name

    def filter_tg(self, tg_str):
        up_str = str.upper(tg_str)
        is_tg = re.findall(r'HA(.+?)', up_str)
        if not is_tg:
            return False
        return True

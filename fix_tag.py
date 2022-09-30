from libs.youzan import ApiClient
from libs.helper import *

default_config = ConfigHelper.getDefault()
db = DBHelper()
api = ApiClient()
conn = db.getConnect()
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

tag = []
data = api.invoke('youzan.itemcategories.tags.get', '3.0.0', {
    'is_sort':'false'
})
for i in data['data']['tags']:
    tag.append((i['id'],i['name']))
cursor.execute('select id,name from `goods_tag`')
goods_tag = cursor.fetchall()
tag_extra = []
for i in goods_tag:
    tag_dum = (i['id'],i['name'])
    if tag_dum not in tag:
        tag_extra.append(tag_dum)
print(tag_extra)
cursor.executemany("delete from `goods_tag` where id=%s and name=%s",tag_extra)

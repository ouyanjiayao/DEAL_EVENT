from libs.youzan import ApiClient
from libs.helper import *
from syn.syn_image import image_syn

default_config = ConfigHelper.getDefault()
db = DBHelper()
api = ApiClient()
conn = db.getConnect()
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

a = image_syn()
cursor.execute('select id,id from `goods`')
b = cursor.fetchall()

for i in b:
    if i['id']:
        a.getImageURL(i['id'])
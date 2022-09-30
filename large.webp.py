from libs.youzan import ApiClient
from libs.helper import *

api = ApiClient()
db = DBHelper()
default_config = ConfigHelper.getDefault()
conn = db.getConnect()
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

cursor.execute("select * from goods_desc where goods_desc like '%%!large.webp%%'")
webps = cursor.fetchall()
for i in webps:
    desc = i['goods_desc'].replace('!large.webp','')
    cursor.execute("update goods_desc set goods_desc=%s where id=%s",(desc,i['id']))
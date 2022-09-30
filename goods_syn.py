from libs.helper import *

default_config = ConfigHelper.getDefault()
db = DBHelper()
conn = db.getConnect()
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
cursor.execute(" update goods set version = version+1,updated_id=1001,updated_time=unix_timestamp(now()) where syn_state<3 and is_delete=0 and auto_syn = 1 order by id")

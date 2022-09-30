from libs.youzan import ApiClient
from libs.helper import *
import time
import sys
import os
import uuid
import urllib
import json

default_config = ConfigHelper.getDefault()

db = DBHelper()
conn = db.getConnect()
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
api = ApiClient()

def downFile(url):
    file_name = os.path.splitext(url)
    ext_name = file_name[1]
    uu_id = uuid.uuid1()
    uu_id = str(uu_id).replace('-','')
    uu_id = uu_id[0:12]
    file_name = str(int(time.time()))+ uu_id + ext_name
    dir = default_config['web']['upload_dir'] + '/'+ time.strftime("%Y%m%d", time.localtime())
    if not os.path.exists(dir):
        os.mkdir(dir)
    file_name = dir + '/' + file_name
    urllib.request.urlretrieve(url, filename=file_name)
    result = file_name.replace(default_config['web']['upload_dir'],'@web')
    return {
        'url':result,
        'size':os.path.getsize(file_name)
    }

cursor.execute("SELECT * FROM `goods` where id=1061 order by id")
rows = cursor.fetchall()

for row in rows:

    if row['id']:
        data = api.invoke('youzan.item.get','3.0.0',{
            'item_id': row['id']
        })

        item = data['data']['item']
        if not data['data']['item']['item_imgs']:
            pass
        else:
            images = []
            down_images = []
            image_ids = []
            for img in item['item_imgs']:
                cursor.execute('select * from upload_file where id = %s', (img['id']))
                exists = cursor.fetchone()

                if not exists:
                    time.sleep(0.5)
                    down_result = downFile(img['url'])
                    insert = (1, item['title'], down_result['url'], down_result['size'], time.time(), 1, 0, 1, img['id'], 1, time.time(), 3)
                    cursor.execute(
                        'insert into upload_file(dir_id,name,url,size,created_time,type,is_delete,version,id,version,syn_time,syn_state) value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                        insert)
                    cursor.execute('SELECT LAST_INSERT_ID() as id')
                    id_row = cursor.fetchone()

                    down_images.append({"id": id_row['id'], "id": img['id'], "url": down_result['url']})
                else:
                    down_images.append({"id": exists['id'], "id": exists['id'], "url": exists['url']})
            for image in down_images:
                images.append({"id":image['id'],"url":image['url']})
                image_ids.append(str(image['id']))
            image_ids = ','.join(image_ids)
            images_str = str(images).replace("\'","\"")
            first_image_str = str(images[0]).replace("\'","\"")
            cursor.execute('update goods set image_ids = %s where id = %s',(image_ids,row['id']))
            cursor.execute('update goods set images = %s where id = %s', (images_str, row['id']))
            cursor.execute('update goods set first_image = %s where id = %s', (first_image_str, row['id']))
            print(image_ids)
#!/usr/bin/env python
# coding: utf-8
from libs.helper import *
import os
import pymysql

conn = pymysql.connect(host='127.0.0.1', user='test', passwd='123456', db='test', charset='utf8',port=3306)

cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)

ospath = 'C:/UPUPW_NP7.2_64/vhosts/vc.sto2c.com/uploads11/'
files = os.listdir(ospath)
try:
    for i in files:
        img = os.listdir(ospath+'/'+i)
        print(i)
        for j in img:
            print(j)
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            cursor.execute("select id from fliename where  date=%s and fliename = %s",(i,j))
            img_exist = cursor.fetchall()
            print(img_exist)
            if not img_exist:
                os.remove(ospath+i+ "/" +j)
            else:
                print(img_exist)
except Exception as e:
    syn_logger.exception(e)






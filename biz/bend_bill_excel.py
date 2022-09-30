import sys
from libs.helper import *
import random
import threading
import time
import os
import re
import json
import operator
import urllib
import pandas as pd
import xlwt

class BendBillExcel:
    def __init__(self):
        self.db_helper = DBHelper()
        self.default_config = ConfigHelper.getDefault()
        self.conn = self.db_helper.getConnect()
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)

    def execute(self, limit):
        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        # 要导出的账单
        cursor.execute('select * from bend_bill_export where (agree_download = 1 or type = 2) and file_name IS NULL order by id desc limit %s',(limit))
        rows = cursor.fetchall()
        for row in rows:
          try:
            if row['type'] == 2:
              cursor.execute('select t.id,t.created_time,t1.goods_name,t1.sku_name,t1.goods_num,t1.sale_price,t2.price_count_type from bend_order t '+
                          'left join bend_order_detail t1 ON t.id = t1.order_id '+
                          'left join goods_attr_config t2 ON t1.goods_id = t2.goods_id ' +
                          'where t1.is_delete = 0 and t.order_state = 4')
            else:
              cursor.execute('select t.id,t.created_time,t1.goods_name,t1.sku_name,t1.goods_num,t1.sale_price,t2.price_count_type from bend_order t '+
                          'left join bend_order_detail t1 ON t.id = t1.order_id '+
                          'left join goods_attr_config t2 ON t1.goods_id = t2.goods_id ' +
                          'where t1.is_delete = 0 and t.order_state = 4 and t.user_id=%s',row['wx_open_id'])
            orderData = cursor.fetchall()
            name = '账单' + time.strftime("%Y-%m", time.localtime(row['date'])) + '-' + str(int(time.time())) + '.xls'
            file_path = self.create_excel(name, orderData)
            # 更新状态
            cursor.execute('update bend_bill_export set file_name = %s where id = %s', (name,row['id']))
            conn.commit()
          except Exception as e:
            print(e)

    def create_excel(self, name, detail):
      # 文件路径
      file_path = self.default_config['toexcel']['Compare_Goods_Attr'] + '/' + name

      # 创建工作簿，并指定写入的格式
      f = xlwt.Workbook(encoding = 'utf8') # 创建工作簿

      # 创建sheet，并指定可以重复写入数据的情况 设置行高度
      sheet = f.add_sheet('账单', cell_overwrite_ok=True)

      #默认参数
      font_name = '微软雅黑'
      font_color = 8
      column = 6

      #设置列宽
      col = sheet.col(0)
      col.width = 256 * 8
      col = sheet.col(1)
      col.width = 256 * 12
      col = sheet.col(2)
      col.width = 256 * 17
      col = sheet.col(3)
      col.width = 256 * 15
      col = sheet.col(7)
      col.width = 256 * 15

      # 基本信息
      style = self.set_style(font_name,230,font_color,False,False,False)
      sheet.write_merge(0, 0, 0, 2, 'XXX有限责任公司', style)
      sheet.write(0, 7, '2022年3月', style)
      sheet.write_merge(3, 3, 0, 4, '客户名称：城市便携酒店', style)
      sheet.write_merge(4, 4, 0, 4, '联系人：小明', style)

      # 加入默认头部
      style = self.set_style(font_name,360,font_color,True,True,True)
      sheet.write_merge(column, column, 0, 7, '供应明细', style)
      sheet.row(column).height_mismatch  = True
      sheet.row(column).height = 20 * 25

      # 加入标题信息
      style = self.set_style(font_name,200,font_color,True,True,True)
      head = ["序号","日期","商品名称","规格","数量","单位","单价\r\n（元）","金额\r\n（不含税）"]
      for index,value in enumerate(head):
          sheet.write(column + 1,index,value, style)
      sheet.row(column + 1).height_mismatch  = True
      sheet.row(column + 1).height = 20 * 33

      # 加入内容
      style = self.set_style(font_name,200,font_color,True,False,True)
      total_price = 0
      for index,value_list in enumerate(detail,1):
          arr = [index, '2022-03-05', value_list['goods_name'], value_list['sku_name'], value_list['goods_num'], ('件' if value_list['price_count_type'] == 1 else '斤'), '￥' + value_list['sale_price'], '￥' + (value_list['goods_num'] * value_list['sale_price'])]
          total_price += value_list['goods_num'] * value_list['sale_price']
          for i,value in enumerate(arr):
              sheet.write(index + (column + 1),i,value, style)

      column = column + len(detail) + 2
      for i in range(0,8):
        sheet.write(column,i,'', style)
      column += 1
      sheet.write_merge(column, column, 0, 0, '总计', style)
      sheet.write_merge(column, column, 7, 7, '￥' + total_price, style)
      for i in range(0,7):
        if i != 0:
          sheet.write(column,i,'', style)

      # 底部
      style = self.set_style(font_name,230,font_color,False,False,False)
      column += 4
      sheet.write_merge(column, column, 0, 4, '供应商：XXX有限责任公司', style)
      sheet.write_merge((column + 1), (column + 1), 0, 4, '地址：XXX', style)

      # 保存excel
      f.save(file_path)

    # 设置样式
    def set_style(self, name, height, color, border, bold, alignment):
      style = xlwt.XFStyle() # 初始化样式

      font = xlwt.Font() # 为字体创建样式
      # 字体类型：比如宋体、仿宋可以是汉仪瘦金书繁
      font.name = name
      # 是否为粗体
      font.bold = bold
      # 设置字体颜色
      font.colour_index = color
      # 字体大小
      font.height = height
      # 字体下划，当值为11时。填充颜色就是蓝色
      font.underline = 0
      # 定义格式
      style.font = font

      if alignment == True:
        alignment = xlwt.Alignment() # 为单元格创建样式
        # 内容居中
        alignment.horz = xlwt.Alignment.HORZ_CENTER
        # 垂直居中
        alignment.vert = xlwt.Alignment.VERT_CENTER
        # 自动换行
        alignment.wrap = xlwt.Alignment.WRAP_AT_RIGHT
        # 定义格式
        style.alignment = alignment

      if border == True:
        borders = xlwt.Borders()
        borders.left = 1
        borders.right = 1
        borders.top = 1
        borders.bottom = 1
        borders.left_colour = 8
        borders.right_colour = 8
        borders.bottom_colour = 8
        borders.top_colour = 8
        # 定义格式
        style.borders = borders

      return style

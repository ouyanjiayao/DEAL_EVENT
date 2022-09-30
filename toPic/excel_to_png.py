from libs.helper import *
import time
from datetime import date, timedelta
import hashlib
import codecs
import imgkit
import pandas as pd
import pdfkit as pdfkit


class ExcelToImage:
    def __init__(self):
        super(ExcelToImage, self).__init__()
        self.html_head = """<!DOCTYPE html>
                                <html lang="en">
                                <head>
                                    <meta charset="UTF-8">
                                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                                    <meta http-equiv="X-UA-Compatible" content="ie=edge">
                                    <title>Document</title>
                                </head>
                                <body>"""

        self.html_end = """</body>
                              </html>"""
        self.default_config = ConfigHelper.getDefault()

    def excel_html(self, excel_path, html_path):
        """
        excel to html
        :param excel_path: excel 路径
        :param html_path: html 存放 路径
        :return: html 路径集合
        """
        html_paths = []

        excel_obj = pd.ExcelFile(excel_path)  # excel 文件对象

        excel_sheets = excel_obj.sheet_names  # 获取 excel 所有单元

        # 将每个单元转换为 html 文件
        for index, sheet in enumerate(excel_sheets):
            html_path_file = html_path + sheet + ".html"
            # 获取本单元 excel 信息
            excel_data = excel_obj.parse(excel_obj.sheet_names[index])
            with codecs.open(html_path_file, 'w', 'utf-8') as html:
                # 加上头尾部, 防止中文乱码
                html_data = self.html_head + excel_data.to_html(header=True, index=True) + self.html_end
                html.write(html_data.replace('NaN', ''))
            html_paths.append(html_path_file)
        return html_paths

    # @staticmethod
    def html_image(self, html_paths, image_path, config, image_name):
        """
        html to image
        :param html_paths: html
        :param image_path: image
        :return:
        """
        for index, html_path in enumerate(html_paths):
            img_obj = image_path + image_name + ".jpg"
            with open(html_path, "r", encoding="utf-8") as html_file:
                imgkit.from_file(html_file, img_obj, config=config)

    def to_one_image(self, excel_name=None, image_name=None):
        if not excel_name:
           return
        ReportImage = ExcelToImage()
        # self.wkhtmltoimage = subprocess.Popen(['which', 'wkhtmltoimage'], stdout=subprocess.PIPE).communicate()[0].strip()
        config = imgkit.config(wkhtmltoimage=self.default_config['to_image']['image_config_path'])
        # excel 转 html
        html_paths = ReportImage.excel_html(excel_name, self.default_config['to_image']['html_path'])
        # html 转 image
        ReportImage.html_image(html_paths, self.default_config['to_image']['image_path'], config, image_name)



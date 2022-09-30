from libs.youzan import ApiClient
from libs.helper import *
import time
import json


class compareGdsYz:
    def __init__(self):
        self.db = DBHelper()
        self.api = ApiClient()
        self.conn = self.db.getConnect()
        self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.default_config = ConfigHelper.getDefault()
        self.file_dir = self.default_config['totxt']['compare_goods_path']
        self.file_name = self.file_dir+'\compare_result_'+str(int(time.time()))+'.txt'

    def compare(self, goods_info):
        cursor = self.cursor
        for i in goods_info:
            cursor.execute('select * from goods_attr_config where goods_id = %s order by id asc', (i['id']))
            attr_config = cursor.fetchone()
            # cursor.execute('select * from goods where id = %s', (i['id']))
            # row = cursor.fetchone()
            self.get_sku_detail(i['id'], i['id'], attr_config, i['type'])

    def get_cp_config_price(self,goods_id,sku_detail_id,attr_config):
        cursor = self.cursor
        cost = 0
        h_costs = 0
        handle_sku_price = []
        cursor.execute('select * from goods_cp_config where goods_id = %s and sku_detail_id = %s order by id asc',
                    (goods_id, sku_detail_id))
        cp_config_rows = cursor.fetchall()
        if cp_config_rows:
            for cp_config_row in cp_config_rows:
                cp_config_content = json.loads(cp_config_row['content'])
                cursor.execute('select * from goods_attr_config where goods_id = %s', (cp_config_row['dp_goods_id']))
                cp_config_row_attr_config = cursor.fetchone()
                d_cost = 0
                h_cost = 0
                if not cp_config_row_attr_config['cost']:
                    cp_config_row_attr_config['cost'] = 0
                if not cp_config_content['attr']:
                    d_cost = float(cp_config_row_attr_config['cost'])
                else:
                    s_sku_id = []
                    sku_jg_ids = []
                    for cp_config_content_attr in cp_config_content['attr']:
                        sku_jg_id = []
                        cursor.execute('select * from goods_attr where id = %s', (cp_config_content_attr['attr_id']))
                        attr_t = cursor.fetchone()
                        if attr_t and attr_t['type'] == 1:
                            s_sku_id.append(cp_config_content_attr['attr_id'] + '_' + cp_config_content_attr['item_id'])
                        sku_jg_id.append(cp_config_content_attr['attr_id'] + '_' + cp_config_content_attr['item_id'])
                        sku_jg_ids = s_sku_id+sku_jg_id
                    s_sku_id = ':'.join(s_sku_id)
                    cursor.execute('select * from goods_sku_price where goods_id = %s and sku_id = %s',
                                (cp_config_row['dp_goods_id'], s_sku_id))
                    s_sku_price = cursor.fetchone()
                    sku_jg_id = ':'.join(sku_jg_ids)
                    cursor.execute('select * from goods_sku_system where goods_id = %s and sku_id = %s and type=2',
                                (cp_config_row['dp_goods_id'], sku_jg_id))
                    handle_sku_price = cursor.fetchone()
                    if not handle_sku_price:
                        h_cost = 0
                    else:
                        h_cost = float(handle_sku_price['handle_price'])
                    if not s_sku_price:
                        d_cost = float(cp_config_row_attr_config['cost'])
                    else:
                        d_cost = float(s_sku_price['cost'])
                if cp_config_row_attr_config['price_count_type'] == 1:
                    cost += d_cost * float(cp_config_content['unit'])
                    h_costs += h_cost * float(cp_config_content['unit'])
                elif cp_config_row_attr_config['price_count_type'] == 2:
                    cost += (d_cost / 500) * float(cp_config_content['unit'])
                    h_costs += (h_cost / 500) * float(cp_config_content['unit'])
        return {'cost': cost, 'sale_price': cost * float(attr_config['sale_scale']) + h_costs}

    def get_tc_config_price(self,goods_id,sku_detail_id,attr_config):
        cursor = self.cursor
        cost = 0
        cursor.execute('select * from goods_tc_config where goods_id = %s and sku_detail_id = %s order by id asc',
                    (goods_id, sku_detail_id))
        s_tc_configs = cursor.fetchall()
        for s_tc_config in s_tc_configs:
            cp_config_content = json.loads(s_tc_config['content'])
            if not cp_config_content['attr']:
                cost += 0
            else:
                s_sku_id = []
                for cp_config_content_attr in cp_config_content['attr']:
                    cursor.execute('select * from goods_attr where id = %s',
                                (cp_config_content_attr['attr_id']))
                    attr_t = cursor.fetchone()
                    if attr_t and attr_t['type'] == 1:
                        s_sku_id.append(cp_config_content_attr['attr_id'] + '_' + cp_config_content_attr['item_id'])
                    else:
                        s_sku_id = []
                s_sku_id = ':'.join(s_sku_id)
                print(s_sku_id)
                cursor.execute('select * from goods_attr_config where goods_id = %s', (s_tc_config['dp_goods_id']))
                s_attr_config = cursor.fetchone()
                cursor.execute('select * from goods_sku_detail where goods_id = %s and sku_id = %s',
                            (s_tc_config['dp_goods_id'], s_sku_id))
                s_sku_detail = cursor.fetchone()
                s_price = self.get_cp_config_price(s_tc_config['dp_goods_id'], s_sku_detail['id'], s_attr_config)
                if not s_price:
                    cost += 0
                else:
                    cost += s_price['cost']
        return {'cost': cost, 'sale_price': cost * float(attr_config['sale_scale'])}

    def get_sku_detail(self, goods_id, id, attr_config, types):
        sku_stocks = []
        cursor = self.cursor
        cursor.execute('select * from goods_sku_detail where goods_id = %s order by id asc', (goods_id))
        rows = cursor.fetchall()
        data = self.api.invoke('youzan.item.get', '3.0.0', {
            'item_id': id
        })
        items = data['data']['item']
        skus = items['skus']
        if rows:
            if len(rows) != len(skus):
                print(goods_id)
                with open(self.file_name, 'a+', encoding='utf-8') as f:
                    f.write('规格数量差，后台ID:%s，后台规格数量:%s，有赞规格数量:%s\n'%(str(goods_id), len(rows), len(skus)))
            else:
                n = 0
                while n < len(rows):
                    if types == 2:
                        config_price = self.get_cp_config_price(goods_id, rows[n]['id'], attr_config)
                        rows[n]['cost'] = config_price['cost']
                        if attr_config['auto_create_sale_price'] == 1:
                            rows[n]['sale_price'] = config_price['sale_price']
                    if types == 3:
                        config_price = self.get_tc_config_price(goods_id, rows[n]['id'], attr_config)
                        rows[n]['cost'] = config_price['cost']
                        if attr_config['auto_create_sale_price'] == 1:
                            rows[n]['sale_price'] = config_price['sale_price']
                    sku_items = []
                    sku_id_splits = rows[n]['sku_id'].split(':')
                    i = 0
                    for sku_id_split in sku_id_splits:
                        sku_id_split = sku_id_split.split('_')
                        attr_id = sku_id_split[0]
                        item_id = sku_id_split[1]
                        cursor.execute('select * from goods_attr where id = %s', (attr_id))
                        attr_row = cursor.fetchone()
                        cursor.execute('select * from goods_attr_item where id = %s', (item_id))
                        item_row = cursor.fetchone()
                        sku_items.append({'k':attr_row['name'],'kid':attr_id,'v':item_row['name'],'vid':item_id})
                        i += 1
                    sale_price = rows[n]['sale_price']
                    if sale_price <= 0:
                        sale_price = 0.01
                    item = {'price':int(sale_price*100),'skus':sku_items,'item_no':rows[n]['sku_id']}
                    item['quantity'] = rows[n]['stock']
                    sku_stocks.append(item)

                    name_list = []
                    for i in sku_items:
                        name_list.append(i['k'])
                        name_list.append(i['v'])
                    for i in skus:
                        m = 0
                        for j in range(len(name_list)):
                            if name_list[j] in i['properties_name_json']:
                                m+=1
                        if m == len(name_list):
                            if int(sale_price*100) != i['price']:
                                print(goods_id)
                                with open(self.file_name,'a+',encoding='utf-8') as f:
                                    f.write('价格差，后台ID:%s，后台售价:%s，有赞售价:%s'%(goods_id, sale_price, i['price']/100))
                                    if float(sale_price)-i['price']/100 > 1 or float(sale_price)-i['price']/100 < -1:
                                        f.write('，差价:%s\n'%(float(sale_price)-i['price']/100))
                                    else:
                                        f.write('\n')
                                n = 999
                                break
                    n+=1
        elif skus:
            print(goods_id)
            with open(self.file_name,'a+',encoding='utf-8') as f:
                f.write('规格数量差，后台ID:%s，后台规格数量:0，有赞规格数量:%s\n'%(str(goods_id),len(skus)))

        else:
            goods_cost = int(attr_config['cost']*100)
            if not attr_config['auto_create_sale_price']:
                goods_price = int(attr_config['sale_price']*100)
            else:
                goods_price = int(goods_cost*attr_config['sale_scale'])
            if items.get('price',0)!=goods_price:
                print(goods_id)
                with open(self.file_name,'a+',encoding='utf-8') as f:
                    f.write('价格差，后台ID:%s，后台售价:%s，有赞售价:%s'%(goods_id,attr_config['sale_price'],items.get('price',0)/100))
                    if float(attr_config['sale_price'])-items.get('price',0)/100 > 1 or float(attr_config['sale_price'])-items.get('price',0)/100 < -1:
                        f.write('，差价:%s\n'%(float(attr_config['sale_price'])-items.get('price',0)/100))
                    else:
                        f.write('\n')

compareYz = compareGdsYz()
compare_goods_ids =[2,
3,
5,
6,
7,
8,
9,
10,
14,
15,
16,
17,
18,
19,
20,
21,
22,
24,
26,
27,
28,
30,
32,
34,
36,
37,
40,
41,
42,
46,
48,
49,
50,
51,
53,
54,
56,
60,
61,
62,
64,
65,
67,
70,
71,
73,
74,
77,
78,
80,
81,
83,
85,
87,
92,
93,
94,
95,
96,
97,
99,
100,
101,
102,
105,
107,
108,
109,
110,
111,
112,
113,
116,
117,
118,
119,
120,
121,
122,
125,
129,
130,
133,
134,
135,
136,
137,
138,
142,
144,
145,
150,
152,
153,
154,
155,
156,
157,
158,
160,
161,
162,
163,
164,
165,
166,
167,
168,
169,
170,
172,
174,
175,
176,
178,
179,
180,
181,
206,
208,
209,
210,
211,
212,
214,
215,
216,
217,
218,
220,
223,
226,
228,
230,
231,
232,
237,
249,
252,
255,
256,
260,
261,
262,
263,
264,
267,
269,
271,
272,
273,
274,
280,
283,
288,
293,
294,
295,
296,
297,
299,
303,
304,
319,
320,
321,
322,
324,
326,
327,
328,
329,
330,
331,
332,
336,
338,
341,
342,
343,
353,
366,
367,
784,
778,
844,
405,
407,
409,
410,
417,
420,
422,
423,
426,
429,
431,
432,
436,
441,
448,
450,
451,
454,
470,
471,
475,
480,
502,
503,
518,
520,
524,
1017,
1894,
1909,
1893,
561,
603,
604,
606,
610,
611,
675,
2753,
1104,
715,
694,
703,
697,
698,
809,
794,
975,
801,
805,
806,
807,
1904,
1902,
1901,
843,
845,
846,
1900,
859,
1897,
1896,
1895,
876,
894,
895,
893,
923,
933,
969,
959,
970,
977,
979,
987,
1231,
1015,
1016,
1059,
1088,
1089,
1098,
1103,
1109,
1111,
1112,
1113,
1118,
1125,
1148,
1156,
1157,
1196,
1233,
1234,
1235,
1236,
1241,
1245,
1266,
1267,
1268,
1269,
1421,
1550,
1291,
1303,
1312,
1351,
1359,
1380,
1419,
1441,
1446,
1491,
2300,
1528,
1529,
1530,
1531,
1532,
1533,
1534,
1535,
1576,
1577,
1641,
1867,
1771,
1907,
1922,
1923,
1926,
1927,
1928,
1929,
1940,
2049,
2108,
2136,
2143,
2146,
2270,
2169,
2171,
2173,
2224,
2220,
2237,
2265,
2321,
2521,
2559,
2373,
2367,
2374,
2549,
2431,
2508,
2509,
2511,
2544,
2516,
2518,
2520,
2522,
2523,
2524,
2525,
2526,
2527,
2528,
2530,
2531,
2534,
2535,
2537,
2539,
2540,
2542,
2543,
2546,
2547,
2548,
2550,
2553,
2554,
2555,
2556,
2557,
2558,
2560,
2561,
2562,
2806,
2593,
2594,
2595,
2596,
2597,
2754,
2760,
2763,
2764,
2765,
2767,
2771,
2773,
2783,
3054,
3115,
3148,
3170,
3487,
3236,
3461,
3462,
3463,
3464,
3465]

get_goods_info = "select * from goods where type=1 and id in (%s)" % ','.join(['%s']*len(compare_goods_ids))
goods_info = compareYz.cursor.execute(get_goods_info, compare_goods_ids)
goods_ids = compareYz.cursor.fetchall()
compareYz.compare(goods_ids)

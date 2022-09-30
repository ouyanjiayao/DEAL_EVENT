from libs.helper import *
import time
import json

class UpdataCustomerTag:
    def __init__(self):
        self.db_helper = DBHelper()
        self.cursor = None

    def execute_to(self):

        conn = self.db_helper.getConnect()
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.cursor = cursor
        t = time.time()
        print(int(t))


        # 先从客户表拿出十个没有关联标签的客户
        cursor.execute("select t1.yz_open_id from (select yz_open_id from customer_order GROUP BY yz_open_id) t1 "
                       "left join(select yz_open_id,syn_tag from customer where syn_tag <> 1) t2 "
                       "on t1.yz_open_id = t2.yz_open_id where t2.syn_tag != 1 limit  10")
        yz_ids = cursor.fetchall()


        yz_id_list = []


        # 判断获取的客户是否为空
        if len(yz_ids) == 0:
            print("标签写入完毕")
            exit()

        # 非空再对用户进行标签判断
        else:
            for i in yz_ids:
                yz_id_list.append(i['yz_open_id'])
            print(yz_ids)






        # 购买是否积极
        cursor.execute('select yz_open_id,count(1) as buyNum FROM customer_order '
                       'where created_time BETWEEN (UNIX_TIMESTAMP(NOW())-30*86400) and UNIX_TIMESTAMP(NOW()) '
                       'and yz_open_id IN %s GROUP BY yz_open_id',((yz_id_list),))
        yz_ids = cursor.fetchall()

        highAct = []    #高度活跃
        modAct =[]      #中度活跃
        liteAct = []    #轻度活跃

        for i in yz_ids:
            buyNum = i["buyNum"]
            if buyNum >= 11:
                highAct.append(i['yz_open_id'])
            elif buyNum >= 8:
                modAct.append(i['yz_open_id'])
            elif buyNum >= 4:
                liteAct.append(i['yz_open_id'])

        for i in highAct:   #高活跃用户写入
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(4,i))

        for i in modAct:    #中度活跃用户写入
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(5,i))

        for i in liteAct:   #轻度活跃用户写入
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(6,i))


        # 判断流失日期
        cursor.execute('select yz_open_id,max(created_time) as lastOrderTime from customer_order '
                       'where yz_open_id IN %s GROUP BY yz_open_id',((yz_id_list),))
        yz_ids = cursor.fetchall()

        highLeave = []  # 高度流失
        modLeave = []   # 流失
        liteLeave = []  # 轻度流失

        for i in yz_ids:
            lastOrderTime = i['lastOrderTime']
            if t-lastOrderTime >= 60*86400:
                highLeave.append(i['yz_open_id'])
            elif t-lastOrderTime >= 31*86400:
                modLeave.append(i['yz_open_id'])
            elif t-lastOrderTime >= 15*86400:
                liteLeave.append(i['yz_open_id'])

        for i in highLeave:   #高度流失用户写入
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(11,i))

        for i in modLeave:    #中度流失用户写入
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(10,i))

        for i in liteLeave:   #轻度流失用户写入
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(9,i))



        # 判断累计订单金额
        cursor.execute('select yz_open_id,sum(cost) as sumCost from customer_order '
                       'where yz_open_id IN %s GROUP BY yz_open_id',((yz_id_list),))
        yz_ids = cursor.fetchall()

        highWorth = []  # 高价值用户
        modWorth = []  # 中价值用户
        liteWorth = []  # 价值用户
        diggable =[]    # 可挖掘用户

        for i in yz_ids:
            sumCost = i['sumCost']
            if sumCost >= 10000:
                highWorth.append(i['yz_open_id'])
            elif sumCost >= 5000:
                modWorth.append(i['yz_open_id'])
            elif sumCost >= 1000:
                liteWorth.append(i['yz_open_id'])
            elif sumCost >= 500:
                diggable.append(i['yz_open_id'])

        for i in highWorth:   #高价值用户写入
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(1,i))

        for i in modWorth:    #中价值用户写入
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(2,i))

        for i in liteWorth:   #价值用户写入
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(3,i))

        for i in diggable:   #可挖掘用户写入
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(13,i))

        # 喜好组合查询
        cursor.execute('select t1.yz_open_id FROM(select yz_open_id,count(1) as cp_num '
                       'from order_goods_assign where goods_id IN '
                       '(select id from goods where type=2) and yz_open_id IN %s GROUP BY yz_open_id) t1 '
                       'LEFT JOIN (select yz_open_id,count(1) as all_num '
                       'from order_goods_assign where yz_open_id IN %s GROUP BY yz_open_id) t2 '
                       'on t1.yz_open_id= t2.yz_open_id where t1.cp_num>=(t2.all_num/2)',((yz_id_list),(yz_id_list),))
        yz_ids = cursor.fetchall()
        for i in yz_ids:
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)', (7, i['yz_open_id']))




        # 只购买过一次的新用户
        cursor.execute('select yz_open_id,count(1) from customer_order where yz_open_id IN %s '
                       'GROUP BY yz_open_id HAVING count(1)=1',((yz_id_list),))
        yz_ids = cursor.fetchall()

        for i in yz_ids:
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(8, i['yz_open_id']))



        # 巨划算用户——需要配置客户手机号数组
        # 数据表配置手机号
        cursor.execute('select a.yz_open_id from customer a INNER JOIN big_worth b on a.mobile=b.mobile '
                       'where yz_open_id IN %s',((yz_id_list),))
        yz_ids = cursor.fetchall()
        for i in yz_ids:
            cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(10000001, i['yz_open_id']))

        # 数组配置手机号
        # bigCount_list = [18666333305,13926767588,15817929332,15989209931,13415143910,13592837817,13428347766,13829653036,13501419733,13809290651,13322706118,13192196373,15017838103,18823994698,18676859911,13929669207,13929699328,15915506023,13592880934,13592818958,15816788749,15019755255,18826490657,13829651830,13612345882,13926775225,13682973869,13502938003,18029557300,13501414068,13809659981,18664488627,13923666608,13643026320,13415120220,13750481011,13750468960,13522459990,13502961537,13542856886,15875470558,13592810032,13433861366,13794109913,13509880703,13417066978,15994956227,13422490991,13433853311,13502994630,13502983898,13929696884,13502937609,13682901920,13829657922,18903040009,13829666753,13267777733,13623023215,13536802254,13502977879,13376616871,13924765669,15814809353,13502980383,13902777707,13531266099,13417100079,13411952530,13825883424,13502752200,18813403709,13902740009,13729222438,13929669940,13502931683,13825885805,18023284677,13802332152,13415012922,13802719352,13809714383,13622583222,13417036026,13923673138,13790842031,15916679047,13750402338,14715956490,15917185937,13592830030,13592857171,13926778069,13809292474,13809658838,13502766829,13929676664,13414070777,13715962943,13670330365,13829500903,13502750620,13902733068,13790870566,13829636196,13825880281,13411991341,13502998788,13509882641,13415111420,13829666585,13502777710,13509889524,13825882903,13829609698,13750403096,18923663553,13822832977,13829653193,13417088809,13556383907,15816611691,13556382924,13829666667,13929648333,13829672227,13502772166,13929665186,13623087696,13428321103,13725420465,13923663227,13829616152,13825845687,13829575007,13411955691,13829606678,13502985185,18826764554,13929626889,18666683823,13902721012,13415048160,15014313135,15815256535,13509882650,13592884992,13902772149,13790865596,13425536722,15817983560,18218209229,13502955995,18923919110,18929673758,13502932062,13923679189,13417111163,15816600996,13809292314,13829663060,18666683686,13825830435,13592824917,13502960868,13433828319,13750421002,13929690289,13360827927,13414029099,13829669929,13926750294,13539679434,13556350527,13318003707,13790855816,13433352895,13726507800,13729209230,13825860987,13923677848,13428328487,18688018777,13509887898,13923981880,13433300872,13502969718,13415122331,13502494358,13802338024,13502967057,13322731862,13536919192,13750418413,13592811744,13715926555,13825882343,13417128185,14749931341,15992266622,13502913300,13809290926,13929673045,15330228800,13790831208,13827326639,18923668018,13085790836,15113083148,13889960088,13592882399,13502982523,13802712523,13670458929,13612354321,13451810887,13302707887,13929613388,13501408408,13531218887,18772277579,13829607133,18688010787,13829454666,13129722147,15802077755,15816725872,13825887717,13536862525,13536918432,13509892849,13417137639,13790873879,13536818999,13692087865,13509889698,13433341369,13539653557,13924762473,18924722225,13592878781,13929635845,15820135135,13622583472,13929696331,13542817002,13600129007,13829489683,13802338265,18607548210,15976920723,13726502868,13421851999,15914773837,18128105656,13433393388,13435590100,18823996268,13929666016,13822889981,13923910644,13502952128,13829667887,13790883768,13923997758,13417012200,15767806452,15918957263,13016667222,13612372590,18664484860,18818941818,13829669982,15815035306,13433387775,13612410300,13502750999,13727669749,13502733412,13417026019,13926762252,13502955808,13923678006,18688006963,13808867488,13551113381,15986862666,13692009403,15816641422,13809842869,18025575505,18929628663,13729276987,13005215486,13825866175,13606893361,17666199744,18029580822,13202116222,13802339312,15802097733,13592866396,13682984646,13612382973,13750423376,13539353135,13829613618,13360803982,13692023688,13923994363,13715889709,13825887925,15113091066,13809291890,18688007959,15994948494,14715951270,13923674786,13415128711,18825112883,13902739691,18665518581,13682996068,13536881896,13433818826,13790839137,13502509287,13798425552,13676133898,13502938270,15994943980,18929680618,15816794321,13809659684,13542868071,13433333787,13829467766,18929678276,13902742151,15915508165,13809652062,13902701214,13318824034,15820537532]
        # yz_ids = []
        # for i in bigCount_list:
        #
        #     cursor.execute('select yz_open_id from customer where mobile = %s order by id asc limit 1',i)
        #     yz_open_id = cursor.fetchone()
        #     yz_ids.append(yz_open_id['yz_open_id'])
        # print("手机号读取完毕，开始写入标签关联表")
        # for i in yz_ids:
        #     cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)',(10000001, i))


        # # 没下过单的
        # cursor.execute('select yz_open_id from customer '
        #                'where yz_open_id NOT IN(SELECT DISTINCT yz_open_id '
        #                'from customer_order GROUP BY yz_open_id)')
        # yz_ids5 = cursor.fetchall()
        #
        # for i in yz_ids5:
        #     cursor.execute('insert INTO customer_tag_affiliate(tag_id,yz_open_id)VALUES(%s,%s)', (12, i['yz_open_id']))

        # 最后修改客户标签标记为1
        cursor.execute('update customer set syn_tag = 1 where yz_open_id IN %s',((yz_id_list),))








from Query_Body_Functions.Demand_Interpret import Demand_Interpret
from Query_Body_Functions.Query_Record import Query_Record
from Query_Body_Functions.Robot import Robot
from Query_Body_Functions.Handler import Handler

import datetime
import pymysql as m
import random
import clickhouse_driver as ck

class Query_Body(Demand_Interpret, Query_Record): #机器人 和 bug-handler
    def __init__(self, dic):
        super(Query_Body, self).__init__(dic)
        super(Query_Record, self).__init__()
        super(Demand_Interpret, self).__init__()

        self.version = "1.2.4"  # 上线前需要更改
        self.is_production = dic.get('is_production')
        self.subinterface = 1   # !! 0-模糊  1-规整  2-明细
        self.is_data = 1
        self.is_report = 0
        self.is_error = 0       # 是否报错
        self.error_type = -1    # 报错类型-1 未知,0 元素识别,1 部门解析,2 日期解析,3 指标解析,4 sql拼接,5 sql查询 6
        self.error_handler = {-1:"",999:"general_E",0:"distinguisher_E",1:"dept_E",2:"date_E",3:"index_E",4:"sqlCombine_E",5:"query_E",
                              6:"resultCombine_E",7:"resultSend_E",8:""} # recording_E

        self.body_data = dic.get('body_data')
        self.send_date = datetime.datetime.fromtimestamp(self.body_data.get('createAt')/1000).strftime("%Y-%m-%d %H:%M:%S")
        self.groupid = dic.get('groupid')
        self.content = dic.get('content')
        self.content_split = dic.get('content_split')
        self.element_cnt = dic.get('element_cnt')

        self.webhook = dic.get('webhook')
        self.dingdingid = dic.get('dingdingid')
        self.name = dic.get('name')
        self.workno = dic.get('workno')
        self.level = dic.get('level')
        self.organid = dic.get('organid')
        self.is_operative = dic.get('is_operative')
        self.is_allpermission = dic.get('is_allpermission')

        self.index = {}
        self.result = ""

        self.conn = m.connect(host='172.16.92.114', port=3306, user='dcp_select', password='DGpq78fh}|?><',database='BIDB')     # 游动浮标
        self.conn2 = ck.connect(host='172.16.92.170', port='19000', database='default', user='cp_chenjinzhao1',password='chenjinzhao1&%#7175')
    def query(self,sql):
        cur = self.conn2.cursor()
        cur.execute(sql)
        self.result_list = cur.fetchall()

        self.conn2.close


    def resultCombine(self):        #查询结果拼接
        for i in self.result_list:
            # 将decimal的指标值转化成字符串
            lst = ['' if j == None else j for j in i]  # 转换成列表方便拼接文本，顺面将None转换成''
            lst.append(str(int(i[-1])))
            lst.pop(-2)

            self.result = self.result + ' | '.join(lst) + '\n'


    def run(self):
        robot = Robot(self.webhook)     # 从Demand_Interpret解析出来的webhook
        handler = Handler()             # 储存发生错误后给用户发送的信息

        ########################################################### 一、解析需求
        try:
            self.elmDistinguisher()             # ① 部门、时间、指标、粒度元素识别
            print(self.require)
        except:
            self.is_error = 1
            self.error_type = 0
            robot.send_msg(handler.distinguisher_E())
            self.insertExacute()
            return ''

        try:
            self.deptSQL()                      # ②计算出select 和 where 条件后面的"部门"相关语句
            print(self.depart_select, self.depart_where)
        except:
            self.is_error = 1
            self.error_type = 1
            robot.send_msg(handler.dept_E())
            self.insertExacute()
            return ''

        try:
            self.dateSQL()                      # ③计算出select 和 where 条件后面的"时间"相关语句
            print(self.date_select, self.date_where, self.date_groupby, self.date_orderby)
        except:
            self.is_error = 1
            self.error_type = 2
            robot.send_msg(handler.date_E())
            self.insertExacute()
            return ''

        try:
            self.indexDict()                    # ④把数据库的所有指标跑出来
            self.indexMatch()                   # ⑤逐个set进行匹配
            if self.indcode == "":
                self.is_error = 1
                self.error_type = 3
                robot.send_msg(handler.index_E())
                self.insertExacute()
                return ''
        except:
            self.is_error = 1
            self.error_type = 3
            robot.send_msg(handler.index_E())
            self.insertExacute()
            return ''

        try:
            self.sqlCombine()                   # ⑥根据以上查询出来的时间、部门、指标相关的语句块拼接成完成sql
            print(self.sql)
        except:
            self.is_error = 1
            self.error_type = 4
            robot.send_msg(handler.sqlCombine_E())
            self.insertExacute()
            return ''

        ########################################################### 二、查询/拼接结果/发送
        try:
            self.query(self.sql)
        except:
            self.is_error = 1
            self.error_type = 5
            msg_query_e = handler.query_E(self.date_where[25:-1])
            robot.send_msg(msg_query_e)
            self.insertExacute()
            return ''

        try:
            self.resultCombine()                # ⑦把查询出来的结果拼接成文本
        except:
            self.is_error = 1
            self.error_type = 6
            robot.send_msg(handler.resultCombine_E())
            self.insertExacute()
            return ''

        try:
            if self.result != '':
                robot.send_msg(self.result)     # ⑧把结果文本发送给客户
            else:
                robot.send_msg(handler.query_E(self.date_where[25:-1]))
        except:
            self.is_error = 1
            self.error_type = 7
            robot.send_msg(handler.resultSend_E())
            self.insertExacute()
            return ''

        ########################################################### 三、查询入库
        try:
            self.insertExacute()
        except:
            self.is_error = 1
            self.error_type = 8
            return ''

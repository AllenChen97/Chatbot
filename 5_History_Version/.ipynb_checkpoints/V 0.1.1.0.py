from flask import Flask, request, jsonify, redirect, url_for
import numpy as np,pandas as pd
import time,requests,random
import os,re
from dateutil.relativedelta import relativedelta
import datetime
import pymysql as m
# from DBUtils.PooledDB import PooledDB,SharedConnection

import json,ast# ast包可以将字符串转换为有效字典
import hashlib,base64,hmac

import sys
sys.path.append('..')
import Data_Validation_Functions as f


# 获取webhook来定义机器人
def webhook(groupid):
    conn = m.connect(host='172.16.92.96', port=3306, user='yonghongbi', password='yonghongbi123', database='BIDB')
    sql = """select webhook from T_Robot_Dim_Group where groupid = '%s' """ % (groupid)
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()[0][0]
    conn.close()
    return result


def record_insert(obj):
    conn = m.connect(host='172.16.92.96', port=3306, user='yonghongbi', password='yonghongbi123', database='BIDB')
    now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    sql = '''insert into T_Robot_Fact_Query values
('%s','%s','%s','%s','%s','%s',1,0,'%s','%s',
0,0,'%s',0,'%s');''' % (uuid.uuid1(), now, organid, dingdingid, groupid, content, query_organid, obj.sql,
                        is_error, errortype, error_handler, now)
    cur = conn.cursor()
    try:
        cur.execute(sql)  # 执行sql语句
        conn.commit()  # 提交到数据库执行
    except:
        conn.rollback()  # 如果发生错误则回滚
    conn.close()
    return ''


class process:  # 规整需求的关键解析函数！！
    def __init__(self, content):
        self.content = content
        self.require = {}

        self.depart_select = ''
        self.departid_where = ''
        self.ctype = ''

        self.date_select = ''
        self.date_groupby = ''
        self.date_orderby = ''

        self.index = {'下单业绩': 'LSF0001', '签收业绩': 'LSF0002', '外部投诉次数': 'LSS000130',
                      '派发新客数': 'LSZ00038'}  # '下单新客数':'LSF0001','下单新客成交率':'LSF0002',
        # '总进粉':'LSF0001','下单客户数':'LSF0001','总进粉成交率':'LSF0002'

        self.sql = ''
        self.result_str = ''

    def content_split(self):
        require_desc = ['部门', '时间', '指标', '部门粒度', '时间粒度']
        require_list = self.content.split(' ')
        while '' in require_list:
            require_list.remove('')

        # 如果元素只有4个需要判断最后一个 是部门粒度还是时间粒度
        nums = len(require_list)  # 元素个数
        is_day = 0
        day_list = ['天', '日', '月', '年']
        for i in day_list:
            if i in require_list[-1]:
                is_day += 1
        if nums == 4 and is_day > 0:
            self.require = dict(zip(['部门', '时间', '指标', '时间粒度'], require_list))
        elif nums == 5 and is_day == 0:
            self.require = dict(zip(['部门', '时间', '指标', '时间粒度', '部门粒度'], require_list))
        else:
            self.require = dict(zip(require_desc[:nums], require_list))

    # 定义organid,ctype 用于拼接SQL
    def check_organid(self, dept):
        conn = m.connect(host='172.16.92.96', port=3306, user='yonghongbi', password='yonghongbi123', database='BIDB')
        sql = """select organid from T_Robot_Dim_department where departname = '%s' """ % (dept)
        cur = conn.cursor()
        cur.execute(sql)
        organid = cur.fetchall()[0][0]
        if dept.find('营') >= 0:
            return organid, 2
        elif dept.find('团') >= 0:
            return organid, 4
        elif dept.find('部') >= 0:
            return organid, 3
        elif dept.find('组') >= 0:
            return organid, 5
        elif dept.find('个人') >= 0 or dept.find('顾问') or dept.find('员工') >= 0:
            return organid, 6
        else:
            return organid, -1

    def ctype_compare(self, require):
        if require['部门粒度'].find('事') >= 0 or require['部门粒度'].find('营') >= 0:
            ctype_cp = 2
        elif require['部门粒度'].find('部门') >= 0:
            ctype_cp = 3
        elif require['部门粒度'].find('团') >= 0:
            ctype_cp = 4
        elif require['部门粒度'].find('组') >= 0:
            ctype_cp = 5
        elif require['部门粒度'].find('个人') >= 0 or require['部门粒度'].find('顾问') >= 0 or require['部门粒度'].find('员工') >= 0:
            ctype_cp = 6
        else:
            ctype_cp = 0
        return ctype_cp

    # 拼接sql语句中的部门ID条件和部门字段
    def dept_sql(self):
        organid, self.ctype = self.check_organid(self.require['部门'])  # 计算指定获得部门的organid 和 ctype
        # ①有部门粒度
        if type(self.require.get('部门粒度')) == str:
            ctype_cp = self.ctype_compare(self.require)  # 计算require['部门粒度']，获得指定的ctype_cp

            if ctype_cp > self.ctype:  # 如果指定统计部门粒度 比 指定的部门级别小，则覆盖之前的ctype
                self.ctype = ctype_cp
                dept_reduction = 1
            else:  # 如果指定统计部门粒度 比 指定的部门级别大/等于，以当前ctype查询即可
                dept_reduction = 0
                pass
        # ②无部门粒度则不用对比，直接根据指定部门的来
        else:
            dept_reduction = 0

        depart_select = {2: 'depart_1st,', 3: 'depart_2nd,', 4: 'depart_3rd,', 5: 'groupname,', 6: 'staffname,'}

        if dept_reduction == 1 and self.ctype > 2:  # "部门粒度"比"部门"的级别低时：sql查询的部门往上展示多一级
            self.depart_select = depart_select[self.ctype - 1] + depart_select[self.ctype]
        else:
            self.depart_select = depart_select[self.ctype]

        departid = {2: 'departid_1st', 3: 'departid_2nd', 4: 'departid_3rd', 5: 'departid_3rd', 6: 'departid_3rd'}
        self.depart_where = departid[self.ctype] + ' like \'' + organid + '%\''

    # 拼接时间筛选条件
    def dtype_compare(self, require):
        if require['时间粒度'].find('年') >= 0:
            dtype_cp = 1
        elif require['时间粒度'].find('月') >= 0:
            dtype_cp = 2
        elif require['时间粒度'].find('日') >= 0 or require['时间粒度'].find('天') >= 0:
            dtype_cp = 3
        else:
            dtype_cp = 0
        return dtype_cp

    # 时间转格式，简化代码
    def datetime_format(self, date, from_format, to_format):
        return datetime.datetime.strftime(datetime.datetime.strptime(date, from_format), to_format)

    def date_sql(self):
        # 时间清洗
        if len(self.require['时间']) >= 8:
            fmat = '%Y%m%d'
        elif len(self.require['时间']) >= 6:
            fmat = '%Y%m'
        elif len(self.require['时间']) >= 4:
            fmat = '%Y'
        self.require['时间'] = self.datetime_format(self.require['时间'], fmat, fmat)

        dtype_list = {4: 1, 6: 2, 8: 3}  # 日期长度:dtype
        dtype = dtype_list[len(self.require['时间'])]

        # ①where 的日期条件
        config_list = {1: ['4', '%Y', '%Y'], 2: ['7', '%Y%m', '%Y-%m'], 3: ['10', '%Y%m%d', '%Y-%m-%d']}  # dtype:config
        substr, from_format, to_format = config_list[dtype]
        # substring取的位置，从xx日期格式，转化xx成日期格式

        strftime = self.datetime_format(self.require['时间'], from_format, to_format)
        self.date_where = 'substring(stadate,1,%s) =\'' % (substr) + strftime + '\''

        # ②select 的日期字段
        if type(self.require.get('时间粒度')) == str:  # 如果有指定粒度
            dtype_cp = self.dtype_compare(self.require)
            if dtype_cp > dtype:  # 如果指定粒度比原来的小
                substr_list = {2: '7', 3: '10'}
                self.date_select = 'substring(stadate,1,%s) d,' % (substr_list[dtype_cp])
                self.date_groupby = self.date_select[:23] + ','
                self.date_orderby = 'd,'
            else:
                self.date_select = ''
                self.date_groupby = ''
                self.date_orderby = ''
        else:
            self.date_select = ''
            self.date_groupby = ''
            self.date_orderby = ''

    def index_match(self):
        import jieba
        word_cut = {}
        jieba.load_userdict('/opt/jupyter-notebook/DingdingProject/1_Intelligent_Robot/词库.txt')
        for indname, indcode in self.index.items():
            word_cut[indcode] = set(jieba.lcut(indname))

        query_ind_cut = set(jieba.lcut(self.require['指标']))
        for self.indcode, cutting_set in word_cut.items():
            if query_ind_cut == cutting_set:
                break

    def sql_combine(self):
        # (日期粒度，部门粒度)
        sql_head = '''select %s %s sum(indresult) ''' % (self.date_select, self.depart_select)

        sql_where = '''from dcp_dw_rpt.ind_result_day
        where ctype in ( %d ) and indcode ='%s' and %s and %s and type in (toInt64(1),toInt64(2)) 
        ''' % (self.ctype, self.indcode, self.depart_where, self.date_where)
        # (部门粒度, 指标编号, 部门条件, 日期条件)

        sql_groupby = '''group by %s %s ''' % (
            self.date_groupby, self.depart_select[:len(self.depart_select) - 1])  # (日期粒度，部门粒度)
        sql_orderby = '''order by %s %s ''' % (
            self.date_orderby, self.depart_select[:len(self.depart_select) - 1])  # (日期粒度，部门粒度)

        self.sql = sql_head + sql_where + sql_groupby + sql_orderby


app = Flask(__name__)  # Flask构造函数使用当前模块（__name __）的名称作为参数。
h = f.Handler()


# route()函数是一个装饰器，它告诉应用程序哪个URL应该调用相关的函数。
@app.route('/', methods=['POST'])
############################################################################## 业绩统计数据
def performance():
    body_data = request.form.to_dict()

    require = process(body_data['content'])  # 定义
    require.content_split()
    print(require.require)

    robot = f.Robot(test=True)
    robot.webhook = webhook(body_data['groupid'])
    robot.secret = ''
    robot.url = robot.get_url()

    # 如果nums=3 就算总数，nums=4就是按部门/时间 汇总，nums=5就按照时间+部门汇总

    ########################################################### 解析需求
    ###########################1.计算查询的部门
    try:
        require.dept_sql()  # 输入需求和指定部门的ctype，比较后拼接出sql内容
        print(require.depart_select, require.depart_where)
    except:
        robot.send_msg(h.dept_e())
        return ''

    ###########################2.时间条件
    try:
        require.date_sql()  # 字段命名规范！！
        print(require.date_select, require.date_groupby, require.date_orderby)
    except:
        robot.send_msg(h.date_e())
        return ''

    ########################################################### 拼接sql
    try:
        require.index_match()
        require.sql_combine()
        print(require.sql)
    except:
        print('sql拼接问题')
        robot.send_msg(h.sql_combine_e())
        return ''

    ########################################################### 拼接结果并发送
    #     emoji_fun =['[推眼镜]','[鲜花]','[送花花]','[吃瓜]','[专注]','[微笑]']
    #     num2 = random.randint(0,len(emoji_fun)-1)
    #     emoji = emoji_fun[num2] + ' '
    #     msg = emoji+require['部门']+' '+ date[26:-1]+'的下单业绩为'+index_value[0]+'，签收业绩'+index_value[1]
    try:
        result = f.query(require.sql)
        result_str = ''
        for i in result:
            # 将decimal的指标值转化成字符串
            lst = ['' if j == None else j for j in i]  # 转换成列表方便拼接文本，顺面将None转换成''
            lst.append(str(int(i[-1])))
            lst.pop(-2)

            result_str = result_str + ' | '.join(lst) + '\n'

        # 结果为空的话，返回"无数据"的错误提示
        if result_str != '':
            robot.send_msg(result_str)
        else:
            msg_query_e = h.sql_query_e_a() + require.date_where[26:-1] + h.sql_query_e_b()
            robot.send_msg(msg_query_e)
        return ''

    except:
        print('sql查询问题')
        msg_query_e = h.sql_query_e_a() + require.date_where[26:-1] + h.sql_query_e_b()
        robot.send_msg(msg_query_e)
        return ''


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='28084')
# -*- coding:utf-8 -*-

from datetime import datetime

class Date_Processing:
    # 解析需求语句中的 '时间'与'时间粒度'
    def __init__(self):
        self.dtype_cp = -1
        self.dtype = 0

    def datetime_format(self, date, from_format, to_format):        # 时间转格式，简化代码
        dt = datetime.strptime(date, from_format)
        # 如果输入日期大于今天就报错
        if dt >= datetime.now():
            raise Exception("日期异常")
        return datetime.strftime(dt, to_format)

    def dtypeCalculation(self):
        # "时间"计算
        time = self.require['时间']

        # 判断'日期'是否有隔断符
        marks = ",-:/\\-+.，。#-—=~·"
        mark = ""
        for i in marks:
            if i in time:
                mark = i
                break

        if mark != "":
        # 如有隔断符 如：2021~07~01
            if len(time) >= 10:
                self.dtype = 3
            elif len(time) >= 7:
                self.dtype = 2
            elif len(time) == 4:
                self.dtype = 1
            f_fmt_cfg_mrk = {1: '%Y', 2: '%Y' + mark + '%m', 3: '%Y' + mark + '%m' + mark + '%d'}
            f_fmt = f_fmt_cfg_mrk[self.dtype]
        else:
        # 如没有隔断符 如：20210701
            if len(time) >= 8:
                self.dtype = 3
            elif len(time) >= 6:
                self.dtype = 2
            elif len(time) == 4:
                self.dtype = 1
            f_fmt_cfg = {1: '%Y', 2: '%Y%m', 3: '%Y%m%d'}
            f_fmt = f_fmt_cfg[self.dtype]


        # 将全部格式转成 2021-07-01
        t_fmt_cfg = {1:'%Y',2:'%Y-%m',3:'%Y-%m-%d'}
        self.query_time = self.datetime_format(self.require['时间'], f_fmt, t_fmt_cfg[self.dtype])

        dtype_list = {4: 1, 7: 2, 10: 3}  # 日期长度:dtype
        self.dtype = dtype_list[len(self.query_time)]


    def dtypeCompare(self):
        # '时间粒度'计算
        date_level = self.require.get('时间粒度')

        if date_level is None:
            self.dtype_cp = -1
        elif date_level.find('年') >= 0:
            self.dtype_cp = 1
        elif date_level.find('月') >= 0:
            self.dtype_cp = 2
        elif date_level.find('日') >= 0 or date_level.find('天') >= 0:
            self.dtype_cp = 3
        else:
            self.dtype_cp = None    #如果有时间粒度但解析失败，就故意让dateS_G_O的比较报错


    def dateSQL(self):
        self.dtypeCalculation()     # '日期'计算
        self.dtypeCompare()         # '日期粒度'计算

        self.date_where = "substring(stadate,1,%s) = '%s' " % (len(self.query_time), self.query_time)

        # ②select 的日期字段
        substr_list = {1:'4', 2: '7', 3: '10'}

        # print(self.dtype_cp,self.dtype)
        if self.dtype_cp > self.dtype:  # 如果指定粒度比原来的小
            self.date_select = 'substring(stadate,1,%s) d,' % (substr_list[self.dtype_cp])
            self.date_groupby = self.date_select[:23] + ','
            self.date_orderby = 'd,'
        else:
            self.date_select = 'substring(stadate,1,%s) d,' % (substr_list[self.dtype])
            self.date_groupby = self.date_select[:23] + ','
            self.date_orderby = 'd,'
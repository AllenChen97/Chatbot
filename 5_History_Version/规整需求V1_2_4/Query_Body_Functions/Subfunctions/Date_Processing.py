# -*- coding:utf-8 -*-

import datetime

class Date_Processing:
    # 解析需求语句中的 '时间'与'时间粒度'
    def __init__(self):
        self.require = {}
        self.dtype_cp = -1
        self.dtype = 0
        self.date_select = ''
        self.date_groupby = ''
        self.date_orderby = ''

    def datetime_format(self, date, from_format, to_format):
        # 时间转格式，简化代码
        return datetime.datetime.strftime(datetime.datetime.strptime(date, from_format), to_format)

    def dtypeCompare(self):
        # 拼接时间筛选条件
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
            self.dtype_cp = -1

    def dtypeCalculation(self):
        # "时间"计算
        sql_time = self.require['时间']
        if len(sql_time) >= 8:
            fmat = '%Y%m%d'
        elif len(sql_time) >= 6:
            fmat = '%Y%m'
        elif len(sql_time) >= 4:
            fmat = '%Y'
        sql_time = self.datetime_format(self.require['时间'], fmat, fmat)

        dtype_list = {4: 1, 6: 2, 8: 3}  # 日期长度:dtype
        self.dtype = dtype_list[len(sql_time)]


    def dateWhere(self):
        # ①where 的日期条件
        config_list = {1: ['4', '%Y', '%Y'], 2: ['7', '%Y%m', '%Y-%m'], 3: ['10', '%Y%m%d', '%Y-%m-%d']}  # dtype:config
        substr, from_format, to_format = config_list[self.dtype]

        # substring取的位置，从xx日期格式，转化xx成日期格式
        strftime = self.datetime_format(self.require['时间'], from_format, to_format)
        self.date_where = 'substring(stadate,1,%s) =\'' % (substr) + strftime + '\''


    def dateS_G_O(self):
        # ②select 的日期字段
        substr_list = {1:'4', 2: '7', 3: '10'}
        if self.dtype_cp > self.dtype:  # 如果指定粒度比原来的小
            self.date_select = 'substring(stadate,1,%s) d,' % (substr_list[self.dtype_cp])
            self.date_groupby = self.date_select[:23] + ','
            self.date_orderby = 'd,'
        else:
            self.date_select = 'substring(stadate,1,%s) d,' % (substr_list[self.dtype])
            self.date_groupby = self.date_select[:23] + ','
            self.date_orderby = 'd,'

    def dateSQL(self):
        self.dtypeCalculation()

        self.dtypeCompare()# 如果有指定粒度

        self.dateWhere()
        self.dateS_G_O()
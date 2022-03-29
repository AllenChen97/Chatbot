# -*- coding:utf-8 -*-

######################################################################
# 类名：用户
# 继承：部门识别类 Department_Processing,日期识别类 Date_Processing,指标识别类 Indicator_Recognision
#      结果拼接类Result_Processing，查询记录类Query_Record

# 接收主接口消息，对拆分后的需求语句的元素进行逐一识别，最终通过继承的方法进行解析，最后在本来中拼接成sql

# 变量一：（来自主接口，通过Flask的request获取）
#     body_data 请求体
#     content_split 内容切分
#     element_cnt 内容切分后字元素个数
#     content 消息内容

#     webhook 钉钉机器人链接
#     dingdingid 用户钉钉ID
#     name 姓名
#     workno 用户工号
#     level 职级
#     organid 数据中心organid
#     is_operative 是否运营系
#     is_allpermission 是否全权限

# 变量二：（本类中计算）
#     sql 查询数据库的sql


######################################################################
import os,sys
sys.path.append(os.getcwd())

from Query_Body_Functions.Subfunctions.Department_Processing import Department_Processing
from Query_Body_Functions.Subfunctions.Date_Processing import Date_Processing
from Query_Body_Functions.Subfunctions.Indicator_Recognision import Indicator_Recognision

class Demand_Interpret( Department_Processing, Date_Processing, Indicator_Recognision):

    def __init__(self, dic):
        super(Demand_Interpret, self).__init__()
        super(Department_Processing, self).__init__()
        super(Date_Processing, self).__init__()
        super(Indicator_Recognision, self).__init__()

        self.require = {}
        self.sql = ""
        self.result = ""

    def elmDistinguisher(self):  #
        require_desc = ['部门', '时间', '指标', '部门粒度', '时间粒度']

        # 如果元素只有4个需要判断最后一个 是部门粒度还是时间粒度
        is_day = 0
        day_list = ['天', '日', '月', '年']
        for i in day_list:
            if self.content_split[-1].find(i) >= 0:
                is_day += 1

        if self.element_cnt == 4 and is_day > 0:
            require_desc.pop(3)
            self.require = dict(zip(require_desc, self.content_split))
        elif self.element_cnt == 5 and is_day == 0:
            self.require = dict(zip(['部门', '时间', '指标', '时间粒度', '部门粒度'], self.content_split))
        else:
            self.require = dict(zip(require_desc[:self.element_cnt], self.content_split))

    def sqlCombine(self):
        if self.dtype==3 or self.dtype_cp==3:
            sql_head = '''select %s %s sum(indresult) ''' % (self.date_select, self.depart_select)

            sql_where = '''from dcp_dw_rpt.ind_result_day
            where ctype in ( %d ) and indcode ='%s' and %s and %s and type in (toInt64(1),toInt64(2)) 
            ''' % (self.ctype, self.indcode, self.depart_where, self.date_where)
            # (部门粒度, 指标编号, 部门条件, 日期条件)

            sql_groupby = '''group by %s %s 
            ''' % (self.date_groupby, self.depart_select[:len(self.depart_select) - 1])  # (日期粒度，部门粒度)
            sql_orderby = '''order by %s %s 
            ''' % (self.date_orderby, self.depart_select[:len(self.depart_select) - 1])  # (日期粒度，部门粒度)

            self.sql = sql_head + sql_where + sql_groupby + sql_orderby
        elif self.ind_type == 0:
            sql_head = '''select %s %s sum(indresult) ''' % (self.date_select, self.depart_select)

            sql_where = '''from dcp_dw_rpt.ind_result_month
            where ctype in ( %d ) and indcode ='%s' and %s and %s and type in (toInt64(1),toInt64(2)) 
            ''' % (self.ctype, self.indcode, self.depart_where, self.date_where)
            # (部门粒度, 指标编号, 部门条件, 日期条件)

            sql_groupby = '''group by %s %s 
            ''' % (self.date_groupby, self.depart_select[:len(self.depart_select) - 1])  # (日期粒度，部门粒度)
            sql_orderby = '''order by %s %s 
            ''' % (self.date_orderby, self.depart_select[:len(self.depart_select) - 1])  # (日期粒度，部门粒度)

            self.sql = sql_head + sql_where + sql_groupby + sql_orderby
        else :
            sql_head = '''select %s %s sum(numerator_value)/sum(denominator_value) ''' % (self.date_select, self.depart_select)

            sql_where = '''from dcp_dw_rpt.ind_result_month
            where ctype in ( %d ) and indcode ='%s' and %s and %s and type in (toInt64(1),toInt64(2)) 
            ''' % (self.ctype, self.indcode, self.depart_where, self.date_where)
            # (部门粒度, 指标编号, 部门条件, 日期条件)

            sql_groupby = '''group by %s %s 
            ''' % (self.date_groupby, self.depart_select[:len(self.depart_select) - 1])  # (日期粒度，部门粒度)
            sql_orderby = '''order by %s %s 
            ''' % (self.date_orderby, self.depart_select[:len(self.depart_select) - 1])  # (日期粒度，部门粒度)

            self.sql = sql_head + sql_where + sql_groupby + sql_orderby


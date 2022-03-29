# -*- coding: utf-8 -*-

######################################################################
# Project Name: 机器人用户更新
# Job Name   : 每天早上9.15运行
# Task Name   : wx_pfm_Data_Validation
# Author     : CJZ
# Create Date : 2021-01-22
# Description : 
# Version    : V1.0    2020-10-21     基本构建检测逻辑
######################################################################

from flask import Flask, request, jsonify, redirect, url_for
import Data_Validation_Functions as f
import numpy as np,pandas as pd
import time,requests
import os,re

import json,ast# ast包可以将字符串转换为有效字典
import hashlib,base64,hmac
import pymysql as m
class user_update:
    def __init__(self):
        self.appkey = "dingivx2npvflqcop1ag"       #智能人事应用
        self.appsecret = "eLNrFcBcaE65Qn6GvFtRX1f8aJ8aHmFlpN93fZEEzbwM0ItFCpNBENSF3vGSLWDb"
        self.url = 'https://oapi.dingtalk.com'
        self.access_token = ''
        
        self.dept_list = []
        self.ding_list = []
        self.workno_dict = {}
    
    def get_token(self):          ### 钉钉接口1：获取access_token
        param_dict = {'appkey':self.appkey,'appsecret': self.appsecret}
        self.access_token = requests.get(self.url + '/gettoken',param_dict).json()['access_token']

    def get_dept(self):          ### 钉钉接口2：递归所有部门
        param_dept = {'access_token': self.access_token}
        self.dept_list = requests.get(self.url + '/department/list',param_dept).json()['department'] 

    def get_dingdingid(self):    ### 钉钉接口3：按部门查找所有员工
        for dept_id in self.dept_list:
            param_ding = {'access_token': self.access_token,'dept_id':dept_id['id']}
            result = requests.get(self.url + '/topapi/user/listid',param_ding).json()['result']
            self.ding_list = self.ding_list + result["userid_list"]
            
    def get_workno(self):        ### 钉钉接口4：从员工花名册获取工号
        param_dict = {
            'access_token': self.access_token,
            'agentid':'1066509961',
            'userid_list':'',
            'field_filter_list':'sys00-jobNumber' #工号的字段编号
        }
        for ding_id in update.ding_list:
            try:
                param_dict['userid_list'] = ding_id
                staff_info = requests.post(update.url + '/topapi/smartwork/hrm/employee/v2/list',param_dict).json()
                dingding_id = staff_info['result'][0]['userid']
                workno = staff_info['result'][0]['field_data_list'][0]['field_value_list'][0].get('value')
                update.workno_dict[dingding_id] = workno
            except:
                try:
                    param_dict['userid_list'] = ding_id
                    staff_info = requests.post(update.url + '/topapi/smartwork/hrm/employee/v2/list',param_dict).json()
                    dingding_id = staff_info['result'][0]['userid']
                    workno = staff_info['result'][0]['field_data_list'][0]['field_value_list'][0].get('value')
                    update.workno_dict[dingding_id] = workno
                except:
                    pass
            
    def run(self):
        try:
            self.get_token()
            self.get_dept()
            self.get_dingdingid()
        except:
            f.print_log('API 1 Error')
        try:
            self.get_workno()
        except:
            f.print_log('API 2 Error')
    
class combine_sql:
    def __init__(self,workno_dict):
        self.workno_dict = workno_dict
        self.user_info = None
        self.users = None
        self.table_merge = None

        self.dtype = {}
        self.sql_ck = ''
    
    def ueser_info_ck(self):          ## 1.Clickhouse-员工表取数
        sql_ck = '''select staffid,name,workno,title,organid,deptid,status,is_operative,loaddate
        ,(case when organid like '015807%' or organid like '0159010102%' or organid like '02620203%' or staffid='niejun'
        or (title='总监' and type in (1,2)) then 1 else 0 end) is_allpermission
        from dcp_dw_dmp.dim_staff_info where company=1 and status=1 '''
        columns_ck = ['staffid','name','workno','title','organid','deptid','status','is_operative','loaddate','is_allpermission']
        self.user_info = pd.DataFrame(f.query(sql_ck),columns=columns_ck)
    
    def merge(self):                 ## 2.将dingdingid 关联过去ck查到的员工信息
        columns_ding = ['dingdingid','workno'] 
        self.users = pd.DataFrame(self.workno_dict.items(),columns=columns_ding)

        self.table_merge = pd.merge(self.users,self.user_info,how='inner',on='workno').drop_duplicates()
        self.table_merge = self.table_merge[['dingdingid', 'staffid', 'name', 'workno', 'title', 'organid', 'deptid',
        'status','is_operative','is_allpermission', 'loaddate']]
    
    def dtype_determine(self):      ## 3.判断字段类型，用于拼接values
        for column_sq,column in enumerate(self.table_merge.iloc[1,:]):
            self.dtype[column_sq] = type(column)
            
    def values_combine(self):       ## 4.利用List的join方法拼接 values
        insert_content=[]
        for row in np.array(self.table_merge):
            empty = []
            for num,content in enumerate(row):
                if self.dtype.get(num)==str and content is None:
                    empty.append('\'\'')
                elif self.dtype.get(num)==str:
                    empty.append('\''+content+'\'')
                else:
                    empty.append(str(content))
            insert_content.append(','.join(empty))

        self.sql = r'insert into T_Robot_Dim_User values (' + r'),('.join(insert_content) + r');'
    
    def sql_exacute(self):          ## 5.执行插入语句
        conn = m.connect(host = '172.16.92.114',port=3306,user = 'dcp_select',password='DGpq78fh}|?><',database = 'BIDB')
        cur = conn.cursor()
        try:
            cur.execute('truncate table T_Robot_Dim_User') 
            conn.commit() # 提交到数据库执行
        except:
            conn.rollback() # 如果发生错误则回滚
        try:
            cur.execute(self.sql)
            conn.commit() 
        except:
            conn.rollback() 
        conn.close()
        
    def run(self):
        try:
            self.ueser_info_ck()
            self.merge()
        except:
            f.print_log('Clickhouse Query Error')
            
        try:
            self.dtype_determine()
            self.values_combine()
        except:
            f.print_log('Combine Error')
            
        try: 
            self.sql_exacute()
        except: 
            f.print_log('Insert Error')
            
            
if __name__ == '__main__':
    update = user_update()
    update.run()
    combiner = combine_sql(update.workno_dict)
    combiner.run()
# -*- coding:utf-8 -*-

import uuid
import datetime

class Query_Record:
    # 结果入库
    def __init__(self):
        self.is_react = 0
        self.lasteditdate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def insert_SQLCombine(self):
        value1 = """'%s', '%s', '%s', '%s', '%s',
        """ % (uuid.uuid4(), self.send_date, self.organid, self.dingdingid, self.groupid)
        # 查询类：uuid, 用户类：消息发送时间,用户organid,用户dingdingid,钉钉群ID,

        value2 = """'%s', %d, %d, %d, %d, '%s',
        """ % (self.content, self.is_data, self.is_report,self.is_production, self.subinterface, self.version)
        # 查询类：消息内容, 是否数据需求, 是否报告, 子接口, 接口版本,

        value3 = """ %d, %d, '%s', %d, '%s' 
        """ % (self.is_error, self.error_type, self.error_handler[self.error_type], self.is_react, self.lasteditdate)
        # 错误类：是否报错, 错误类型, 错误提数, 是否交互, 插入时间

        self.insert_sql = r'''insert into T_Robot_Fact_Query 
        (uuid,send_date,organid,dingdingid,groupid,
        content,is_data,is_report,is_production,subinterface,interface_Version,
        is_error,error_type,error_handler,is_react,lasteditdate)
        values ( %s )''' % (value1 + value2 + value3)

    def insertExacute(self):        ## 5.执行插入语句
        self.insert_SQLCombine()
        cur = self.conn.cursor()
        try:
            cur.execute(self.insert_sql)
            self.conn.commit()      # 提交到数据库执行
        except Exception as e:
            print(e)
            self.conn.rollback()    # 如果发生错误则回滚
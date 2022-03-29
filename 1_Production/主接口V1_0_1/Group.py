######################################################################
# 类名：群

# 变量：
#     groupid 钉钉群ID
#     webhook 机器人链接，如果库中没这个群 发过来的消息content就作为webhook
#     is_newgroup 群是否新建
#     is_production 是否生产环境（开发环境仅能通过114数据库修改，群创建默认是生产环境）

# 主要功能：查用户信息
######################################################################
import os,sys
sys.path.append(os.getcwd())

class Group():
    def __init__(self):
        self.is_newgroup = 0
        self.is_production = 1

    def groupEstablisher(self):
        sql = """insert into T_Robot_Dim_Group values
                    ('%s','%s',1,'%s',now(),'%s','%s','',now()) 
                  """ % (self.groupid, self.groupname, self.dingdingid, self.dingdingid, self.content)
        # 群ID， 小组名， 群主， 创建人， 机器人链接
        # try: #先不捕捉错误，插入提交不了的话就让其报错
        self.cur.execute(sql)  # 执行sql语句
        self.conn.commit()  # 提交到数据库执行
        # except Exception as e:
        #     print(e)
        #     self.conn.rollback()  # 如果发生错误则回滚

    def webhookQuery(self):
        sql = """select webhook,is_production from T_Robot_Dim_Group where groupid = '%s' """ % (self.groupid)
        self.cur = self.conn.cursor()
        self.cur.execute(sql)

        try:
            self.webhook, self.is_production = self.cur.fetchall()[0]  # 如果群在配置表里面有，就按照配置信息定义机器人
            print(self.webhook, self.is_production)
        except:  # 如果没有，就插入后再查
            print("数据库中未有该群，现在新建！")
            if self.content.find("https://oapi.dingtalk.com/robot/send?access_token=") >= 0:
                self.groupEstablisher()  # 新建的群就自动把用户发来的webhook加到数据库里
                self.is_newgroup = 1
                self.webhook = self.content

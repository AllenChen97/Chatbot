######################################################################
# 类名：用户
# 继承：小组类
# 变量：
#     dingdingid 用户钉钉ID
#     name 姓名
#     workno 用户工号
#     level 职级
#     organid 数据中心organid

#     is_operative 是否运营系
#     is_allpermission 是否全权限

#     groupid 钉钉群ID
#     webhook 钉钉机器人链接
#     content 消息内容

# 主要功能：查webhhok，若在114库的小组维表中查不出，就新建一个
######################################################################
from Group import Group


class User(Group):
    def __init__(self):
        super(User, self).__init__()
        self.name = ""
        self.workno = ""
        self.level = ""
        self.organid = ""
        self.is_operative = ""
        self.is_allpermission = ""

    def check_permission(self):
        name = ['钉钉ID', '姓名', '工号', '职级', '数据中心organid', '是否运营系', '是否全权限']
        permission_sql = """select dingdingid,name,workno,level,organid,is_operative,is_allpermission 
        from T_Robot_Dim_User where dingdingid = '%s' """ % (self.dingdingid)

        cur = self.conn.cursor()
        cur.execute(permission_sql)
        user_info = dict(zip(name, cur.fetchall()[0]))  # dict:用户信息

        # 用户信息赋值
        self.name = user_info.get("姓名")
        self.workno = user_info.get("工号")
        self.level = user_info.get("职级")
        self.organid = user_info.get("数据中心organid")
        self.is_operative = user_info.get("是否运营系")
        self.is_allpermission = user_info.get("是否全权限")


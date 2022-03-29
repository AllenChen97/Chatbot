######################################################################
# 类名：主接口类
# 继承：用户类
# 变量一：（来自request）
#     body_data 请求体
#     content 消息内容
#     groupid 钉钉群ID
#     groupname 组名
#     dingdingid 用户钉钉ID

# 变量二：（继承User类）
#     name 姓名
#     workno 用户工号
#     level 职级
#     organid 数据中心organid
#     is_operative 是否运营系
#     is_allpermission 是否全权限

# 变量三：（继承User类 ==> 继承Group类）
#     webhook 钉钉机器人链接
#     content 消息内容

# 变量四：（本类内计算指标）
#     is_data 是否数据需求
#     is_struct 是否规整需求
#     content_split 内容切分
#     element_cnt 内容切分后字元素个数
#     port_fuzzy 模糊类子接口端口对照字段
#     port_struct 规整类子接口端口对照字段
#     port_detail 明细子接口端口对照字段
#     subinterface_url 子接口url

# 主要功能1：接收钉钉回调的body_data，利用从用户类和小组类继承来的权限查询、Webhook查询方法，得到回复的Webhook和用户信息
# 主要功能2：分析用户需求是否数据需求、是规整还是模糊的需求，计算出子接口的url
######################################################################
from User import User
import pymysql as m

class Query(User):
    def __init__(self, body_data):
        super(User, self).__init__()
        self.body_data = body_data

        self.content = body_data['text']['content'].strip()
        self.groupid = body_data['conversationId']
        self.groupname = body_data['conversationTitle']
        self.dingdingid = body_data['senderStaffId']

        self.is_struct = 0
        self.is_data = 0
        self.content_split = []
        self.element_cnt = 0

        self.port_fuzzy = {0: '5001', 1: '28083'}
        self.port_struct = {0: '5002', 1: '28084'}
        self.port_detail = {0: '5003', 1: '28085'}

        self.subinterface_url = 'http://0.0.0.0:'
        self.conn = m.connect(host='172.16.92.114', port=3306, user='dcp_select', password='DGpq78fh}|?><',
                              database='BIDB')

    def isData(self):
        words = ['业绩','客','数','率','均','金额'
                ,'粉','投诉','退款','违规','时长','次']
        for word in words:
            if self.content.find(word)>=0:
                self.is_data = 1
                break

    def split(self):
        self.content_split = self.content.split(' ')  # 切分
        for cnt in range(self.content_split.count('')):
            self.content_split.remove('')
        self.element_cnt = len(self.content_split)  # 数切分后元素个数

    def isStruct(self):
        self.split()
        if self.element_cnt >= 3:
            #             and self.content.find('今')==-1 and self.content.find('本')==-1 and self.content.find('这')==-1
            #             and self.content.find('昨')==-1 and self.content.find('上')==-1
            self.is_struct = 1
        else:
            self.is_struct = 0

    def subInterface(self):
        # 计算、拼接需求分发接口

        if self.content:
            self.isData()  # 计算是否数据

            if self.is_data:
                self.isStruct()  # 若是数据需求，计算是否规整

        try:
            if self.is_data:
                if len(self.content_split) >= 3 and self.is_struct:  # 规整需求
                    self.subinterface_url += self.port_struct.get(self.is_production)
                    self.subinterface = 2
                elif self.is_struct == 0:   # 明细需求
                    self.subinterface_url += self.port_detail.get(self.is_production)
                    self.subinterface = 3
                else:                       # 模糊需求
                    self.subinterface_url += self.port_fuzzy.get(self.is_production)
                    self.subinterface = 1
        except Exception as e:
            print(e)
import random

class Handler():
    def __init__(self):
        self.index = "下单业绩，签收业绩，派发新客数，外部投诉次数"
        self.emoji_sad = ['[吃瓜]', '[可怜]', '[一团乱麻]', '[冷笑]']

    def group_establish(self):
        return """\
[送花花]恭喜建群成功，小数目前可以基于《指标库》提供一下数据查询服务：

%s统计

使用方法：部门 时间 指标 时间粒度 部门粒度
例子： 体管一营 202101 派发新客数 部门

赶紧来体验一下吧[三多]""" % (self.index)

    def distinguisher_E(self):
            return """\
    [抱大腿]小数还不是很清楚主人想查询的部门呢，请输入准确的部门名称试试吧~

    例如：D28体重管理服务部二部六团"""

    def dept_E(self):
        return """\
[抱大腿]小数还不是很清楚主人想查询的部门呢，参照钉钉架构 输入准确的部门名称试试吧~

例如：D28体重管理服务部二部六团"""

    def date_E(self):
        return """\
抱歉，小数还不是很清楚主人想查询的时间范围，请以下格式输入日期试试吧[可爱]

例如：20210122  （表示2021年1月22日）"""

    def index_E(self):
        return """抱歉，小数还不是很清楚主人想查询的指标噢，我们换个指标名试试吧~"""

    def sqlCombine_E(self):
        return '''主人 很抱歉，小数在后台拼接代码的时候发生错误了 -.-'''

    def query_E(self, date):
        num = random.randint(0, len(self.emoji_sad) - 1)
        return '主人，数据库里没有' + date + '的数据哦%s' % (self.emoji_sad[num])

    def resultCombine_E(self):
        return '''主人 很抱歉，小数在后台拼接数据结果的时候发生错误了 -.-'''

    def resultSend_E(self):
        return '''主人 很抱歉，小数在发出数据的时候发生错误了 -.-'''

    def recording_E(self):
        return '''主人，很抱歉，小数在记录本次查询的时候发生错误了 -.-'''

    def general(self):
        return '抱歉 主人 小数还不知道怎么回答[拜托]'

    #     emoji_fun =['[推眼镜]','[鲜花]','[送花花]','[吃瓜]','[专注]','[微笑]']
    #     num2 = random.randint(0,len(emoji_fun)-1)
    #     emoji = emoji_fun[num2] + ' '
    #     msg = emoji+require['部门']+' '+ date[26:-1]+'的下单业绩为'+index_value[0]+'，签收业绩'+index_value[1]
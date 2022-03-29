# -*- coding:utf-8 -*-

import numpy as np,pandas as pd
import clickhouse_driver as ck
import urllib.parse
import urllib.request
import time, datetime, calendar
import random
from dateutil.relativedelta import relativedelta

######################################## 一、定义查ck方法
def query(sql):
    conn = ck.connect(host='172.16.92.170',port='19000',database='default',user='cp_chenjinzhao1', password='chenjinzhao1&%#7175')
    #游动浮标
    cur = conn.cursor()
    cur.execute(sql)
    result=cur.fetchall()
    return result

######################################## 二、Bug Handler
class Handler():
    def __init__(self):
        self.index = "下单业绩，签收业绩，派发新客数，外部投诉次数"
        self.emoji_sad = ['[吃瓜]','[可怜]','[一团乱麻]','[冷笑]']
        
    def group_establish(self):
        return """\
[送花花]恭喜建群成功，小数目前可以基于《指标库》提供一下数据查询服务：

%s统计

使用方法：部门 时间 指标 时间粒度 部门粒度
例子： 体管一营 202101 派发新客数 部门

赶紧来体验一下吧[三多]"""%(self.index)

    def dept_e(self):
        return """\
[抱大腿]小数还不是很清楚主人想查询的部门呢，请输入准确的部门名称试试吧~

例如：D28体重管理服务部二部六团
"""

    def date_e(self):
        return """\
抱歉，小数还不是很清楚主人想查询的时间范围，请输入连续数表示日期试试吧[可爱]

例如：20210122  （表示2021年1月22日）
"""
    def index_e(self):
            return """\
抱歉，小数还不是很清楚主人想查询的指标噢，我们换个指标名试试吧~

例如：20210122  （表示2021年1月22日）
"""

    def sql_combine_e(self):
        return '''\
主人，小数在后台拼接代码的时候发生错误了， —_—

%s'''%(self.index)

    def sql_query_e(self,date):
        num = random.randint(0, len(self.emoji_sad) - 1)
        return '主人，数据库里没有' + date +'的数据哦%s'%(self.emoji_sad[num])

    def general(self):
        return '抱歉，主人 小数还不知道怎么回答[拜托]'


######################################## 六、定义机器人
import requests, json, urllib
import time, hmac, hashlib, base64


class Robot():
    def __init__(self, test=True):
        self.appkey = "dinggusun3wou6cp1eev"
        self.appsecret = "iTYs60fL6G7s_7N5ac4e6wCgDY9_w2X-gWWdJEGYRR9kI3QTC095rGJcbv-J3xn0"
        self.access_token = self.get_token()
        if test == True:
            self.secret = ''
            self.webhook = "https://oapi.dingtalk.com/robot/send?access_token=a96ced2bd30bfe4ae6ef36f5263bf642a4af0a94c50310687c189191d06c9431"
        else:
            self.secret = 'SEC7e55c873c5f418c963ee132000652b17a31971e5e385a577a050ec24c9718466'
            self.webhook = "https://oapi.dingtalk.com/robot/send?access_token=85abf9287c8e8155b9af2d5c030a69badf3fcaf126b363b5d9a311b043f2d1ca"
        self.url = self.get_url()

    def get_token(self):  # 获取token
        params = {"appkey": self.appkey, "appsecret": self.appsecret}
        response = requests.get("https://oapi.dingtalk.com/gettoken", params=params)
        return response.json().get("access_token")

    def get_url(self):  # 获得秘钥 配置url
        timestamp = int(round(time.time() * 1000))

        secret_enc = bytes(self.secret, encoding='utf-8')  # .encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        string_to_sign_enc = bytes(string_to_sign, encoding='utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()

        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return self.webhook + "&timestamp=" + str(timestamp) + '&sign=' + sign

    def send_msg(self, msg):
        header = {"Content-Type": "application/json", "Charset": "UTF-8"}
        data = {"msgtype": 'text', 'text': {'content': msg}}
        sendData = json.dumps(data).encode("utf-8")  # 对请求的数据进行json封装

        request = urllib.request.Request(url=self.url, data=sendData, headers=header)  # 发送请求
        opener = urllib.request.urlopen(request, timeout=10)  # 将请求发回的数据构建成为文件格式
        return json.load(opener)

    def upload_media(self, file_path):
        header = {"Content-Type": "application/json", "Charset": "UTF-8"}
        upload_url = 'https://oapi.dingtalk.com/media/upload?access_token=' + self.access_token + '&type=image'
        # 构建data字典(请求数据)
        files = {'media': open(file_path, 'rb')}
        data = {'access_token': self.access_token, 'type': 'image'}
        # 向带有access_token的url发送post请求，携带data和file参数
        response = requests.post(upload_url, files=files, data=data)
        return response.json()['media_id']

    def send_markdown(self, content):  # 发送markdown
        header = {"Content-Type": "application/json", "Charset": "UTF-8"}
        # 构建data字典(请求数据)，发送post请求
        data = {"msgtype": 'markdown', 'markdown': content}
        sendData = json.dumps(data).encode("utf-8")  # 对请求的数据进行json封装
        # 向带有access_token的url发送post请求，携带data和file参数
        request = urllib.request.Request(url=self.url, data=sendData, headers=header)  # 发送请求
        opener = urllib.request.urlopen(request, timeout=10)  # 将请求发回的数据构建成为文件格式
        return json.load(opener)
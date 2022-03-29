from flask import Flask, request, jsonify, redirect, url_for
import time,requests
import os,re
import json,ast  # ast包可以将字符串转换为有效字典

import sys
from Query import Query
from Robot import Robot
from Handler import Handler

sys.path.append('..')


app = Flask(__name__)# Flask构造函数使用当前模块（__name __）的名称作为参数。

# route()函数是一个装饰器，它告诉应用程序哪个URL应该调用相关的函数。
@app.route('/', methods=['POST'])
def index():
# 1.读取用户发送的内容
    h = Handler()
    q = Query(request.get_json())
    # print(q.body_data)
    try:
        q.check_permission()  # 查询‘用户信息’及‘权限’
        q.webhookQuery()      # 查询’webhook‘
        robot = Robot(q.webhook)
        if q.is_newgroup:                               # 1.新建的群就发送“恭喜建群成功”,然后直接return结束
            robot.send_msg(h.group_establish())
            return ''
        q.subInterface()      # 子接口url拼接
    except Exception as e:
        print(e)
    finally:
        q.conn.close()

# 2.把User分发去对应的接口
    u_param = q.__dict__
    u_param.pop('conn')
    u_param.pop('cur')

    if q.is_data==0:                                    #2.非数据数据需求就报“不懂怎么回答”
        robot.send_msg(h.general())
        print('分发失败，检查子接口状态')
        return ''
    else:                                               #3.数据需求根据计算的到的子接口进行参数传递
        print('开始分发%s'%(q.subinterface_url))
        requests.post(q.subinterface_url, json = json.dumps(u_param))
        return ''

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='28082')
    # netstat -ntulp |grep 28082
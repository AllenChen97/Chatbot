import Data_Validation_Functions as f
from flask import Flask, request, jsonify, redirect, url_for
import numpy as np, pandas as pd
import time, requests
import os, re

import json, ast  # ast包可以将字符串转换为有效字典
import hashlib, base64, hmac
import pymysql as m
app = Flask(__name__)  # Flask构造函数使用当前模块（__name __）的名称作为参数。


# route()函数是一个装饰器，它告诉应用程序哪个URL应该调用相关的函数。
@app.route('/', methods=['POST'])
def index():
    # 1.读取用户发送的内容
    q = Query(request.get_json())
    print(q.body_data)

    q.check_permission()  # 查询用户信息和权限
    q.webhookQuery()  # 查询群对应的webhook
    q.subInterface()  # 子接口链接查询

    robot_main = f.Robot(test=True)
    robot_main.webhook = q.webhook
    robot_main.secret = ''
    robot_main.url = robot_main.get_url()

    h = f.Handler()

    # 2.把User分发去对应的接口
    u_param = q.__dict__
    u_param.pop('conn')

    if q.is_newgroup:  # 1.新建的群就发送“恭喜建群成功”
        robot_main.send_msg(h.group_establish())
        return ''
    elif q.is_data == 0:  # 3.非数据数据需求就报“不懂怎么回答”
        robot_main.send_msg(h.general())
        print('分发失败，检查子接口状态')
        return ''
    else:  # 2.数据需求根据计算的到的子接口进行参数传递
        print('开始分发')
        requests.post(q.subinterface_url, json=json.dumps(u_param))
        return ''


if __name__ == '__main__':
    app.run(host='0.0.0.0', port='28082')
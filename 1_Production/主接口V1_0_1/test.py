from flask import Flask, request, jsonify, redirect, url_for
import time,requests
from Query import Query
import json

dic = {'conversationId': 'cidKbP/mD3nx+VLXi30Qz6r5Q=='
    , 'atUsers': [{'dingtalkId': '$:LWCP_v1:$BQv5ESQx/d1oed5YD7oslWFlFa908B1D'}]
    , 'chatbotCorpId': 'ding84406897bb5bc29c35c2f4657eb6378f'
    , 'chatbotUserId': '$:LWCP_v1:$BQv5ESQx/d1oed5YD7oslWFlFa908B1D'
    , 'msgId': 'msgLsNX2zwL97kkcAoUn0Xs8Q=='
    , 'senderNick': '陈进钊'
    , 'isAdmin': True
    , 'senderStaffId': '302421125238151255'
    , 'sessionWebhookExpiredTime': 1624871071697
    , 'createAt': 1624865671450
    , 'senderCorpId': 'ding84406897bb5bc29c35c2f4657eb6378f'
    , 'conversationType': '2'
    , 'senderId': '$:LWCP_v1:$DqQnaA6LDI1Uvm5d27klbkBsuHe+8WBm'
    , 'conversationTitle': 'Dev测试群'
    , 'isInAtList': True
    , 'sessionWebhook': 'https://oapi.dingtalk.com/robot/sendBySession?session=0f0eddb0af3eafe9651942c4286c3ef2'
    , 'text': {'content': ' 体管一营 2020 派发新客数 部门'}
    , 'msgtype': 'text'}


u = Query(dic)

u.webhookQuery()      #查询群对应的webhook
u.check_permission()  #查询用户信息和权限
u.subInterface()      #子接口链接查询
print('正在向 ' + u.subinterface_url + ' 发送请求')



u_param = u.__dict__
# u_param.pop('conn')

print(u_param)
requests.post(u.subinterface_url, json = json.dumps(u_param))
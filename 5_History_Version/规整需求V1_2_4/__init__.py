# -*- coding:utf-8 -*-

######################################################################
# 规整需求处理接口
#
# 作用：接收主接口28082调过来的消息内容，通过实例化Query_Body，解析用户需求，
#      并向用户直接返回用户结果，在最后把查询结果录入到数据库中

######################################################################

from Query_Body import Query_Body

from flask import Flask, request, jsonify, redirect, url_for
import json


app = Flask(__name__)  # Flask构造函数使用当前模块（__name __）的名称作为参数。

@app.route('/', methods=['POST'])# route()装饰器，它告诉应用程序哪个URL应该调用相关的函数。
def performance():
    #     body_data = request.form.to_dict()
    body_data = json.loads(request.get_json())

    d = Query_Body(body_data)  # 构建查询实例
    try:
        d.run()
    except Exception as e:
        print(e)
    finally:
        d.conn.close() # 无论查询是否执行完都释放资源

    return ''

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='28084')
    # netstat -ntulp |grep 5002

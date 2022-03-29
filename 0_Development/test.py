from Query_Body import Query_Body

dic = {'is_newgroup': 0, 'is_production': 0
        , 'body_data': {'conversationId': 'cidKbP/mD3nx+VLXi30Qz6r5Q==', 'atUsers': [{'dingtalkId': '$:LWCP_v1:$BQv5ESQx/d1oed5YD7oslWFlFa908B1D'}], 'chatbotCorpId': 'ding84406897bb5bc29c35c2f4657eb6378f', 'chatbotUserId': '$:LWCP_v1:$BQv5ESQx/d1oed5YD7oslWFlFa908B1D', 'msgId': 'msgMWjjXzGchuyCt2brc44peg==', 'senderNick': '陈进钊', 'isAdmin': True, 'senderStaffId': '302421125238151255', 'sessionWebhookExpiredTime': 1626783588650, 'createAt': 1626778188416, 'senderCorpId': 'ding84406897bb5bc29c35c2f4657eb6378f', 'conversationType': '2', 'senderId': '$:LWCP_v1:$DqQnaA6LDI1Uvm5d27klbkBsuHe+8WBm', 'conversationTitle': 'Dev测试群', 'isInAtList': True, 'sessionWebhook': 'https://oapi.dingtalk.com/robot/sendBySession?session=5959148a8cdb5abcf26ad59b616714d3'
        , 'text': {'content': ' 体管五营  202106 下单业绩  部门\n'}, 'msgtype': 'text'}
        , 'content': '体管五营  202106 下单业绩  部门', 'groupid': 'cidKbP/mD3nx+VLXi30Qz6r5Q==', 'groupname': 'Dev测试群', 'dingdingid': '302421125238151255'
        , 'is_struct': 1, 'is_data': 1, 'content_split': ['体管五营', '2021', '下单业绩', '部门','年'], 'element_cnt': 5, 'port_fuzzy': {'0': '5001', '1': '28083'}, 'port_struct': {'0': '5002', '1': '28084'}, 'port_detail': {'0': '5003', '1': '28085'}, 'subinterface_url': 'http://0.0.0.0:5002', 'name': '陈进钊', 'workno': '031820', 'level': '职员', 'organid': '01580703', 'is_operative': 1, 'is_allpermission': 1
        , 'webhook': 'https://oapi.dingtalk.com/robot/send?access_token=9ab1c5246d8a42d76297130e1f3f3b88df0dba39ecebe470ed816ea4d926dd9a', 'subinterface': 2
       }
s = Query_Body(dic)

s.elmDistinguisher()
print(s.require)

s.deptSQL()  # ②计算出select 和 where 条件后面的"部门"相关语句
# print(s.depart_select, s.depart_where)

s.dateSQL()
# print(s.date_select, s.date_where, s.date_groupby, s.date_orderby)

s.indexMatch()
# print(s.indcode)

s.sqlCombine()  # ⑥根据以上查询出来的时间、部门、指标相关的语句块拼接成完成sql
# print(s.sql)

s.query(s.sql)
s.resultCombine()
# print(s.result)


s.insertExacute()

s.conn.close()

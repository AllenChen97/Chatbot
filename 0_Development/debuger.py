import time

from Query_Body import Query_Body

content_dept = ['体管五营','体重管理五部','体重管理五部三团']     #,'天龙十八部'
content_date = ['2021','202106','20210625']                #,'209901'
content_index = ['违规次数','下单业绩']              #,'业绩','签收率'
content_dept_l = ['事业部','部门','团队']                  #,'个人','我'
content_date_l = ['年','月','日']                        #,'宇宙年','小时'

# content_dept = ['体重管理五部三团']
# content_date = ['20210103','2021/06/06']
# content_index = ['金额签收率','成交率']
# content_dept_l = ['人','我']
# content_date_l = ['年','小时']

dic = {'is_newgroup': 0, 'is_production': 0
        , 'body_data': {'conversationId': 'cidKbP/mD3nx+VLXi30Qz6r5Q==', 'atUsers': [{'dingtalkId': '$:LWCP_v1:$BQv5ESQx/d1oed5YD7oslWFlFa908B1D'}], 'chatbotCorpId': 'ding84406897bb5bc29c35c2f4657eb6378f', 'chatbotUserId': '$:LWCP_v1:$BQv5ESQx/d1oed5YD7oslWFlFa908B1D', 'msgId': 'msgMWjjXzGchuyCt2brc44peg==', 'senderNick': '陈进钊', 'isAdmin': True, 'senderStaffId': '302421125238151255', 'sessionWebhookExpiredTime': 1626783588650, 'createAt': 1626778188416, 'senderCorpId': 'ding84406897bb5bc29c35c2f4657eb6378f', 'conversationType': '2', 'senderId': '$:LWCP_v1:$DqQnaA6LDI1Uvm5d27klbkBsuHe+8WBm', 'conversationTitle': 'Dev测试群', 'isInAtList': True, 'sessionWebhook': 'https://oapi.dingtalk.com/robot/sendBySession?session=5959148a8cdb5abcf26ad59b616714d3'
        , 'text': {'content': ' 体管五营  202106 下单业绩  部门\n'}, 'msgtype': 'text'}
        , 'content': '体管五营  202106 下单业绩  部门', 'groupid': 'cidKbP/mD3nx+VLXi30Qz6r5Q==', 'groupname': 'Dev测试群', 'dingdingid': '302421125238151255'
        , 'is_struct': 1, 'is_data': 1, 'content_split': ['体管五营', '202106', '下单业绩', '部门', '年'], 'element_cnt': 5, 'port_fuzzy': {'0': '5001', '1': '28083'}, 'port_struct': {'0': '5002', '1': '28084'}, 'port_detail': {'0': '5003', '1': '28085'}, 'subinterface_url': 'http://0.0.0.0:5002', 'name': '陈进钊', 'workno': '031820', 'level': '职员', 'organid': '01580703', 'is_operative': 1, 'is_allpermission': 1
        , 'webhook': 'https://oapi.dingtalk.com/robot/send?access_token=9ab1c5246d8a42d76297130e1f3f3b88df0dba39ecebe470ed816ea4d926dd9a', 'subinterface': 2
       }


empty = []
for a in content_dept:
    for b in content_date:
        for c in content_index:
            for d in content_dept_l:
                for e in content_date_l:
                    empty.append([a,b,c,d,e])
for i in empty:
    dic['content_split'] = i
    dic['content'] = " ".join(i)
    s = Query_Body(dic)

    s.run()
    s.conn.close()
    print(i,s.lasteditdate)
    time.sleep(0.7)


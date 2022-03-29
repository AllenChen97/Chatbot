# -*- coding: utf-8 -*-

######################################################################
# Project Name: 企微推送数据检测
# Job Name    : 每天早上9.40推送
# Task Name   : wx_pfm_Data_Validation
# Author      : CJZ
# Create Date : 2020-10-21
# Description : 《商城（企业微信）每日业绩报表_全部》
######################################################################

import numpy as np,pandas as pd
import clickhouse_driver as ck
import urllib.parse
import urllib.request

import time, datetime
def print_log(logstr):
    datetimestr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    datetimestr.strip()
    print("[", datetimestr, "]:", logstr)

    
######################################## 一、定义查询clickhouse方法
def query(sql):
    conn = ck.connect(host='172.16.92.170',port='19000',database='default',user='cp_chenjinzhao1', password='chenjinzhao1&%#7175')
    #游动浮标
    cur = conn.cursor()
    cur.execute(sql)
    result=cur.fetchall()
    return result


######################################## 二、查两个报表的数拼接成Dataframe
try:
    #《企业微信》
    sql="""select stat_date,sum(ordernum),sum(amount) 
    from dcp_dw_rpt.t_rpt_fans_performance_lvshou_mid f 
    where substring(stat_date,1,7)=substring(toString(addDays(today(),-1)),1,7) and f.wx_channel_dep ='绿瘦商城' and f.wx_company='微雅官方' --and f.channel_1st in ('万兔','祥宸','无')
    group by stat_date; """

    title=['日期','订单数','金额']
    table_wx_sta=pd.DataFrame(query(sql),columns=title).sort_values('日期')

    #《每日业绩及进度》
    sql_pfm="""select orderdate,sum(order_cnt),sum(order_cash) from dcp_dw_dmp.t_report_achievement_lvshou_new
    where first_depart in ('商城(微信)','商城(电购)') and substring(orderdate,1,7)=substring(toString(addDays(today(),-1)),1,7) group by orderdate;"""

    title=['日期','订单数','金额']
    table_pfm_sta=pd.DataFrame(query(sql_pfm), columns = title).sort_values('日期')

    #《合并计算差值》
    table_merge = pd.merge(table_pfm_sta, table_wx_sta, how='inner', on='日期')
    table_merge['Difference'] = table_merge['订单数_x'] - table_merge['订单数_y']
    table_merge.index = table_merge['日期']
    table_merge = table_merge[['订单数_x','金额_x','订单数_y','金额_y','Difference']]

    sql_yesterday = """select addDays(today(),-1); """
    yesterday = query(sql_yesterday)[0][0].strftime('%Y-%m-%d')
    difference = table_merge.loc[yesterday]['Difference'] #差值用于检测、汇报

    if difference!=0 :
        msg_df = "### 每日数据监测 \n> #### 报表间对比：昨日《企微》的去重订单与《每日业绩》中商城订单相比少%d张\n"%(difference)
    elif difference==0 :
        msg_df = "### 每日数据监测 \n> #### 报表间对比：今日《企微》商城订单数与《每日业绩》无差异\n"
    
    print_log('任务一和二 报表间横向对比执行 成功')
    time.sleep(2)
except:
    print_log('任务一和二 报表间横向对比执行 失败\n')

    

######################################## 三、查询没有渠道信息的微信号
try:
    sql_wx_null=r"""select stat_date,wx_id,sum(ordernum),sum(amount) 
    from dcp_dw_rpt.t_rpt_fans_performance_lvshou_mid f 
    where substring(stat_date,1,7)=substring(toString(addDays(today(),-1)),1,7) and f.wx_channel_dep is null group by stat_date,wx_id; """

    title=['日期','微信号','订单数','金额']
    table_wx_null=pd.DataFrame(query(sql_wx_null),columns=title).sort_values('日期')
    table_wx_null.index=table_wx_null['日期']
    table_wx_null=table_wx_null[['微信号','订单数','金额']]
    
    try:
        if len(table_wx_null.loc[yesterday].values.shape)==1:   #如果只有一个缺失渠道的微信号
            od_cnt = table_wx_null.loc[yesterday]["订单数"]
            od_amt = int(table_wx_null.loc[yesterday]["金额"])
            wx_id = table_wx_null.loc[yesterday]["微信号"]
        else:                                    #如果有多个缺失渠道的微信号，则需要求和该微信号的业绩 和 拼接微信号
            od_cnt_list = table_wx_null.loc[yesterday]["订单数"].tolist()
            od_amt_list = table_wx_null.loc[yesterday]["金额"].tolist()

            od_cnt = sum(od_cnt_list)#汇总值
            od_amt = sum(od_amt_list)#汇总值

            wx_id_list = table_wx_null.loc[yesterday]["微信号"].tolist()
            wx_id = ','.join(table_wx_null.loc[yesterday]["微信号"].tolist())
    except KeyError: #如果没有微信号，则让缺失的订单数和业绩为0，用于后续推送的判断条件
        wx_id = '无'
        od_cnt = 0
        od_amt = 0
    
    #定义推送文本
    if od_cnt==0 and od_amt==0:
        msg_wechat=""
    else:
        msg_wechat="> #### %s微信号:%s没有渠道信息，其订单共%d张，业绩共%d元 \n"%(yesterday,wx_id,od_cnt,od_amt)
    
    print_log('任务三 没有渠道号的微信查询 成功')
except:
    print_log('任务三 没有渠道号的微信查询 失败\n')
    
    
    
######################################## 四、定义机器人的类
#网传api使用方法
#上传图片获得 media_ID
import requests,json,urllib
import time,hmac,hashlib,base64

class Robot():
    def __init__(self, appkey, appsecret, secret, webhook):
        self.appkey = appkey
        self.appsecret = appsecret
        self.access_token = self.get_token()
        self.secret = secret
        self.webhook = webhook
        self.url = self.get_url()
    
    def get_token(self): # 获取token
        params = {"appkey": self.appkey,"appsecret": self.appsecret}
        response = requests.get("https://oapi.dingtalk.com/gettoken", params=params)
        return response.json().get("access_token")
    
    def get_url(self): #获得秘钥
        timestamp = int(round(time.time() * 1000))
        
        secret_enc = bytes(self.secret, encoding='utf-8')#.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        string_to_sign_enc = bytes(string_to_sign, encoding='utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()

        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
     #测试群链接配置
        return self.webhook+"&timestamp="+str(timestamp)+'&sign='+sign

    def send_msg(self,msg): #发送消息
        header = {"Content-Type": "application/json","Charset": "UTF-8"}
        data = {"msgtype": 'text' , 'text': {'content':msg}}  
        sendData = json.dumps(data).encode("utf-8")  # 对请求的数据进行json封装

        request = urllib.request.Request(url=self.url, data=sendData, headers=header) #发送请求
        opener = urllib.request.urlopen(request,timeout=10) # 将请求发回的数据构建成为文件格式
        return json.load(opener)

    def upload_media(self,file_path):
        header = {"Content-Type": "application/json","Charset": "UTF-8"}
        upload_url='https://oapi.dingtalk.com/media/upload?access_token='+self.access_token+'&type=image'
    #构建data字典(请求数据)
        files = {'media': open(file_path, 'rb')}
        data = {'access_token': self.access_token,'type': 'image'}
    #向带有access_token的url发送post请求，携带data和file参数
        response = requests.post(upload_url, files=files, data=data)
        return response.json()['media_id']
    
    def send_markdown(self,content):#发送markdown
        header = {"Content-Type": "application/json","Charset": "UTF-8"}
    #构建data字典(请求数据)，发送post请求
        data = {"msgtype":'markdown' , 'markdown':content}
        sendData = json.dumps(data).encode("utf-8")  # 对请求的数据进行json封装
    #向带有access_token的url发送post请求，携带data和file参数
        request = urllib.request.Request(url=self.url, data=sendData, headers=header) #发送请求
        opener = urllib.request.urlopen(request,timeout=10) # 将请求发回的数据构建成为文件格式
        return json.load(opener)

if __name__ == '__main__':
    appkey = "dinggusun3wou6cp1eev"
    appsecret = "iTYs60fL6G7s_7N5ac4e6wCgDY9_w2X-gWWdJEGYRR9kI3QTC095rGJcbv-J3xn0"
    secret = ''
    webhook = ''
    
    
######################################## 五、定义机器人实例，并上传图片
try:
#     secret='SEC52e872ea0f8203dd964927d7c05421bd00b694d3cac0a131127b84310e02a127'#FR测试群
#     webhook="https://oapi.dingtalk.com/robot/send?access_token=cd7925e3d8df74289a4651e6ec9c887921d9558d682f5fd2baf29d13ac7cc01d"
    
    secret='SEC7e55c873c5f418c963ee132000652b17a31971e5e385a577a050ec24c9718466'#数据观察群
    webhook="https://oapi.dingtalk.com/robot/send?access_token=85abf9287c8e8155b9af2d5c030a69badf3fcaf126b363b5d9a311b043f2d1ca"
    #从机器人配置页面获取secret和webhook，计算sign并加入到到url，定义成机器人属性。

    robot_test = Robot(appkey,appsecret,secret=secret,webhook=webhook)

    #上传图片
    image=r'http://172.16.92.9:28080/bi/Viewer?au_act=login&proc=1&action=viewer&hback=true&db=!7eff!!7626!!5546!!57ce!!2f!!4f01!!4e1a!!5fae!!4fe1!!6bcf!!65e5!!4e1a!!7ee9!!62a5!!8868!!2f!!5546!!57ce!!28!!4f01!!4e1a!!5fae!!4fe1!!29!!6bcf!!65e5!!4e1a!!7ee9!!62a5!!8868!.db&isAir=false&isFav=false&adminv=dingdingpush&passv=DingdingPush_LS2019&export=image'
    response=urllib.request.urlopen(image,timeout=10)
    picture=response.read()
    with open(r"/opt/jupyter-notebook/DingdingPush/wechat_sta.jpg",'wb') as f:
        f.write(picture)
        
    media_id = robot_test.upload_media('/opt/jupyter-notebook/DingdingPush/wechat_sta.jpg')
    
    print_log('任务四和五 定义机器人和上传图片 成功')
    time.sleep(2)
except:
    print_log('任务四和五 定义机器人和上传图片 失败\n')  
    
    
######################################## 六、先计算波动，定义msg_vibration
def cal_vibration(series):
    vibration = []
    for i,j in zip(series,range(len(series))):
        if i<50 or j==0 :                         #1号或者节假日则令增幅为0
            vibration.append(0)
        else:
            vb = (i-series[j-1])/series[j-1]
            vibration.append( round(vb,4) )        #增幅
    return vibration                              #输出list

try:
    #判断波动大于50%的字段
    table_merge['Vibration']=cal_vibration(table_merge.loc[:,'金额_y']) #企微的金额
    table_merge

    day_of_yesterday = time.strftime('%w', time.strptime(yesterday,'%Y-%m-%d') )   #昨天周几，用于以下逻辑判断
    vb_ytd = table_merge.loc[yesterday,'Vibration']                                #昨天波动，用于以下逻辑判断和推送语句
    pfm_ytd = table_merge.loc[yesterday,'金额_y']  

    if day_of_yesterday==0 and abs(vb_ytd)>=0.25 and pfm_ytd > 300 :   #周日，非国家节假日，波幅大于20%
        msg_vibration = """> #### 环比：%s周日，《企微》下单金额增长%s，环比增幅大于%s \n"""%(yesterday,str(round(vb_ytd*100,2))+"%",'25%')
    elif abs(vb_ytd)>=0.15 and pfm_ytd > 300:                          #非国家节假日，波幅大于10%
        msg_vibration = """> #### 环比：昨日《企微》商城下单金额增长%s，环比增幅大于%s \n"""%(str(round(vb_ytd*100,2))+"%",'15%')
    else:
        msg_vibration= ""
    
    print_log('任务六 日波动幅度计算 成功')
except:
    print_log('任务六 日波动幅度计算!!! 失败\n')
    
    
######################################## 七、历史数据对比

################### 7.1.定义方法：读取文档、核对新旧表格、存档并计算变化率
import datetime
from dateutil.relativedelta import relativedelta

#7.1.1.输入文件地址、跳过的行数、字段名称、读取行数，以读出存放在指定位置的Excel文档
def read_table(direction, skiprows, names, days): 
    table = pd.read_excel(direction, sheet_name=1, skiprows=skiprows, names=names, index_col='日期')
    
    return table.head(days) 

#7.1.2.输入今天的数据表格和昨天的数据表格做对比，加上字段名（用于中间处理），输出两表相减的表格
def table_his_check(table_ytd, table_dbytd, names, days): 
    table_check = pd.merge(table_ytd,table_dbytd,on='日期')
    for i in names[1:]:
        table_check[i] = table_check[i+'_x']-table_check[i+'_y']
        table_check.pop(i+'_x')
        table_check.pop(i+'_y')
    return table_check.head(days-1)#如《企微》会出现当天数据，剔除当天数据

#7.1.3.输入推送的url 和 读取昨日数据表格的参数
#      输出①：昨日数据 减 前日数据的核对表，②：昨天表格每个字段的总和
def final_check(url,filename,skiprows,names):
    #拼接今天和昨天下载的两个文件路径
    td_fname = time.strftime("%Y_%m_%d_")
    ytd_fname = datetime.datetime.strftime(datetime.datetime.now()+ relativedelta(days=-1) ,"%Y_%m_%d_") 
    dr_td = '/opt/jupyter-notebook/DingdingPush/Data_Validation/' + td_fname + filename
    dr_ytd = '/opt/jupyter-notebook/DingdingPush/Data_Validation/' + ytd_fname + filename
    
    #计算days参数取值：若今天是1号 则计算上个月有多少天，否则按今天日期减1
    import calendar
    if int(time.strftime("%d"))==1:
        days = calendar.monthrange(int(time.strftime("%y")),int(time.strftime("%m"))-1)
    else:
        days = int(time.strftime("%d"))-1
    
    #读取昨天《每日业绩》数据
    dbytd = read_table(dr_ytd, skiprows, names, days)

    #先存档今日《每日业绩》数据，再重新读取
    response = urllib.request.urlopen(url)
    excel = response.read()
    with open(dr_td,'wb') as f:
        f.write(excel)    
    ytd = read_table(dr_td, skiprows, names, days) 

    return table_his_check(ytd, dbytd, names, days) , dbytd.sum()

################### 7.2.开始计算核对表格
try:
    ### 7.2.1.《每日业绩》核对历史数据
    ### 7.2.1.1.根据表格特征定义read_table()的参数
    skiprows_pfm = [0,1,3]
    columns_pfm=['日期','商城单数','商城业绩','事业一部业绩','事业二部业绩','事业三部业绩','事业五部业绩','事业六部业绩','事业八部业绩'
             ,'湖北业绩','D28业绩','体管业绩总计','电商业绩','业绩总计','APP商城']
    url_pfm = 'http://172.16.92.9:28080/bi/Viewer?au_act=login&proc=1&action=viewer&hback=true&db=!9500!!552e!!7ba1!!7406!!90e8!!2f!!ff08!!96c6!!56e2!!ff09!!4e1a!!52a1!!7cfb!!4e0b!!5355!!4e1a!!7ee9!!53ca!!8fdb!!5ea6!.db&isAir=false&isFav=false&adminv=dingdingpush&passv=DingdingPush_LS2019&export=excel'

    ### 7.2.1.2.获取核对表和对照表字段总和
    check_pfm,dbytd_sum  = final_check(url_pfm,"daily_pfm.xlsx",skiprows_pfm,columns_pfm)

    ### 7.2.1.3.计算每个字段变化的差距/本月总业绩的总和
    alter_series_pfm = abs(check_pfm.sum()/dbytd_sum)*100

    ### 7.2.1.4.计算平均波动率
    alter_ratio_pfm = np.mean(alter_series_pfm)
    alter_ratio_pfm_str = str(round(alter_ratio_pfm,2))+'%'     #文本用于推送

    ### 7.2.1.5.取出变化比率高于均值的字段 和 对应值
    alt_pfm = check_pfm.sum()
    major_alt_pfm = '商城业绩' + str(int(alt_pfm['商城业绩'])) +',体管业绩' + str(int(alt_pfm['体管业绩总计']))

    ### 7.2.1.6.拼接文本信息
    msg_alter_pfm = "> 历史数据对比：《每日业绩》历史数据平均变动%s，主要变化如下：%s \n"%(alter_ratio_pfm_str, major_alt_pfm)
    
    
    ### 7.2.2.《企微》核对历史数据，目前仅用于保存文件
    columns_wx=['日期','总进粉','被删粉','被删率','实际粉','有效粉','成交数','成交额','总进粉成交率','实际粉成交率','有效粉成交率','单均'
              ,'资源产出','A类','B类','C类','D类','解封','广告','疾病禁售','16岁以下','无效占比']
    skiprows_wx = [0,1]
    url_wx = 'http://172.16.92.9:28080/bi/Viewer?au_act=login&proc=1&action=viewer&hback=true&db=!7eff!!7626!!5546!!57ce!!2f!!4f01!!4e1a!!5fae!!4fe1!!6bcf!!65e5!!4e1a!!7ee9!!62a5!!8868!!2f!!5546!!57ce!!28!!4f01!!4e1a!!5fae!!4fe1!!29!!6bcf!!65e5!!4e1a!!7ee9!!62a5!!8868!.db&isAir=false&isFav=false&adminv=dingdingpush&passv=DingdingPush_LS2019&export=excel'

    check_wx,alt_series_wx = final_check(url_wx,'wx_pfm.xlsx',skiprows_wx,columns_wx) 
    
    ### 7.2.2.2.计算每个字段变化的差距/本月总业绩的总和
    alter_series_wx = abs(check_wx.sum()/alt_series_wx)*100

    ### 7.2.2.3.计算平均波动率
    alter_ratio_wx = np.mean(alter_series_wx)
    alter_ratio_wx_str = str(round(alter_ratio_wx,2))+'%'     #文本用于推送

    ### 7.2.2.4.主要的字段及其变化
    alt_wx = check_wx.sum()
    major_alt_wx = '总进粉' + str(int(alt_wx['总进粉']))+',成交数' + str(int(alt_wx['成交数']))

    ### 7.2.2.5.拼接文本信息
    msg_alter_wx = "> 历史数据对比：《企微》历史数据平均变动%s，主要变化如下：%s \n"%(alter_ratio_pfm_str, major_alt_wx)
    
    
    
    print_log('任务七 历史数据对比 成功')
except:
    print_log('任务七 历史数据对比!!! 失败\n') 
    

######################################## 八、拼接完整msg，发送Markdown
try:
    pic = "> ![screenshot](%s) \n"%(media_id)
    
    # 二、msg_df   三、msg_wechat   7.2.1.6.msg_alter     六、msg_vibration
    msg = msg_df + pic + msg_wechat + msg_alter_wx + msg_vibration
    
    robot_test.send_markdown(content={"text": msg, "title": "Data Validation"} )    

    print_log('任务八 Markdown发送 成功\n\n')
except:
    print_log('任务八 Markdown发送 失败\n\n')  


######################################## 九、每周一合并一次文件
from dateutil.relativedelta import relativedelta
import os
try:
    if time.strftime("%w")=='1':
        #写入一个_all结尾的文件，循环结束后执行
        writer_wx = pd.ExcelWriter(time.strftime("%Y_%m_%d_")+"wx_pfm_all.xlsx")
        writer_pfm = pd.ExcelWriter(time.strftime("%Y_%m_%d_")+"daily_pfm_all.xlsx")

        for i in range(7):
            #计算日期day，用于拼接文件名
            
            day = datetime.datetime.strftime(datetime.datetime.now().date() + relativedelta(days=-1-i) ,"%Y_%m_%d_")
            dr_wx = '/opt/jupyter-notebook/DingdingPush/Data_Validation/' + day + "wx_pfm.xlsx"
            dr_pfm = '/opt/jupyter-notebook/DingdingPush/Data_Validation/' + day + "daily_pfm.xlsx"

            #计算读取的行数days
            if int(time.strftime("%d"))==1:
                days = calendar.monthrange(int(time.strftime("%y")),int(time.strftime("%m"))-1)[1]
            else:
                days = int(day[8:10])-1

            #读取表格，定义to_excel，循环结束后执行    
            excel_wx = read_table(dr_wx, skiprows_wx, columns_wx, days)
            excel_pfm = read_table(dr_pfm, skiprows_pfm, columns_pfm, days)

            excel_wx.to_excel(writer_wx,day) #页面以"%Y_%m_%d_"格式展示
            excel_pfm.to_excel(writer_pfm,day) #页面以"%Y_%m_%d_"格式展示

            os.remove(dr_wx)
            os.remove(dr_pfm)

        writer_wx.save()
        writer_pfm.save()

    print_log('任务九 合并文件 成功\n\n')
except:
    print_log('任务九 合并文件!!! 失败\n\n') 
    

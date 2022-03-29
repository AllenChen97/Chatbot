import pymysql as m
import jieba
from datetime import datetime

conn = m.connect(host='172.16.92.114', port=3306, user='dcp_select', password='DGpq78fh}|?><',database='spark_project_code_processing')
sql = """SELECT fir_ind,fir_type,ind_code,ind_name,case when ind_type='计算指标' then 1 when ind_type='基础指标' then 0 else -1 end ind_type,status,is_push,valid_result 
from ind_config where status=1"""
cur = conn.cursor()
cur.execute(sql)

jieba.load_userdict('/opt/jupyter-notebook/DingdingProject/1_Intelligent_Robot/词库.txt')  # 加载分词库

insert_context = "insert into T_Robot_Dim_Ind_Config values \n"
for i in cur.fetchall():
    ind = []
    for j in range(len(i)):
        # 第5个字段是分词
        if j == 5:
            # 分词
            words = jieba.lcut(i[3])
            # 去符号
            biaodian = r'[’!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]+，。！？“”《》：、．() '
            words_new = []
            for word in words:
                if str(word) in biaodian:
                    pass
                words_new.append(str(word))
            ind.append("'" +",".join(words_new) + "'")

        # 其他字段判断是否字符串，是就加单引号
        if type(i[j]) == str:
            ind.append("'" + i[j] + "'")
        elif i[j] is None:
            ind.append('0')
        else:
            ind.append(str(i[j]))
    loaddate = ",'" + str(datetime.now())[:21] + "'"
    insert_context += "(" + ",".join(ind) + loaddate + "),\n"

print(insert_context[:-2])
conn.close()
conn2 = m.connect(host='172.16.92.114', port=3306, user='dcp_select', password='DGpq78fh}|?><',database='BIDB')
cur2 = conn2.cursor()
try:
    cur2.execute("delete from T_Robot_Dim_Ind_Config")
    cur2.execute(insert_context[:-2])
    conn2.commit()  # 提交到数据库执行
except Exception as e:
    print(e)
    conn2.rollback()  # 如果发生错误则回滚
conn2.close()




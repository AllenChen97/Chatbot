# -*- coding:utf-8 -*-
import jieba

class Indicator_Recognision:
    # 解析需求语句中的 '指标'，优化空间较大
    def __init__(self):

        self.index = {}             # 指标对照表
        self.indcode = ''
        self.ind_type_dic = {}      # 指标类型对照表
        self.ind_type = -1

    def indexDict(self):  ## 把所有指标的 indcode 和 分词查询出来
        cur = self.conn.cursor()
        try:
            cur.execute("select indcode,ind_name_split,ind_type from T_Robot_Dim_Ind_Config where status=1")
            for i in cur.fetchall():
                self.index[i[0]] = set(i[1].split(","))
                self.ind_type_dic[i[0]] = i[2]
        except Exception as e:
            print(e)

    def indexMatch(self):
        self.indexDict()
        jieba.load_userdict('/opt/jupyter-notebook/DingdingProject/1_Intelligent_Robot/词库.txt')  # 加载分词库

        # 切分用户语句中的'指标'
        query_cut = set(jieba.lcut(self.require.get('指标')))

        for indcode, cutting_sets in self.index.items():
            if query_cut == cutting_sets:
                self.indcode = indcode
                self.ind_type = self.ind_type_dic[indcode]
                break
        if self.indcode == '':
            raise Exception("指标不存在")
        # 把index里面的dict切分成set放在word_cut里


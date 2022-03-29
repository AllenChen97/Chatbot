# -*- coding:utf-8 -*-

class Indicator_Recognision:
    # 解析需求语句中的 '指标'，优化空间较大
    def __init__(self):

        self.require = {}
        # '总进粉':'LSF0001','下单客户数':'LSF0001','总进粉成交率':'LSF0002'
        # ①后期需要到ind_config里面找这个指标
        #     index 指标对照表
        self.index = {}
        # '下单业绩': 'LSF0001', '签收业绩': 'LSF0002', '外部投诉次数': 'LSS000130',
        #                       '派发新客数': 'LSZ00038', '下单新客数': 'LSZ000179', '签收新客数': 'LSZ000180'
        self.indcode = ''

    def indexDict(self):  ## 把所有指标的 indcode 和 分词查询出来
        cur = self.conn.cursor()
        try:
            cur.execute("select indcode,ind_name_split from T_Robot_Dim_Ind_Config where status=1")
            for i in cur.fetchall():
                self.index[i[0]] = set(i[1].split(","))
        except Exception as e:
            print(e)

    def indexMatch(self):
        # ②后期可落地优化效率
        import jieba
        word_cut = {}
        jieba.load_userdict('/opt/jupyter-notebook/DingdingProject/1_Intelligent_Robot/词库.txt')  # 加载分词库

        # 切分用户语句中的'指标'
        query_cut = set(jieba.lcut(self.require.get('指标')))

        for indcode, cutting_sets in self.index.items():
            if query_cut == cutting_sets:
                self.indcode = indcode
                break
        # 把index里面的dict切分成set放在word_cut里


# -*- coding:utf-8 -*-

class Department_Processing:
    # 解析需求语句中的 '部门'与'部门粒度'
    def __init__(self):
        self.require = {}
        self.sql_organid = ''
        self.ctype = 0
        self.ctype_cp = 0
        self.dept_reduction = 0

        self.depart_select = ''
        self.departid_where = ''

    def organidQuery(self):
        # 计算需求语句中的部门的organid 和 ctype 用于拼接SQL
        departname = self.require.get('部门')
        sql = """select organid from T_Robot_Dim_department where departname = '%s' """ % (departname)
        cur = self.conn.cursor()
        cur.execute(sql)
        self.sql_organid = cur.fetchall()[0][0]
        if departname.find('营') >= 0:
            self.ctype = 2
        elif departname.find('团') >= 0:
            self.ctype = 4
        elif departname.find('部') >= 0:
            self.ctype = 3
        elif departname.find('组') >= 0:
            self.ctype = 5
        elif departname.find('个人') >= 0 or departname.find('顾问') or departname.find('员工') >= 0:
            self.ctype = 6
        else:
            self.ctype = -1

    def ctypeCompare(self):
        # 将'部门粒度'转化成self.ctype_cp
        dept_level = self.require.get('部门粒度')

        if dept_level.find('事') >= 0 or dept_level.find('营') >= 0:
            self.ctype_cp = 2
        elif dept_level.find('部门') >= 0:
            self.ctype_cp = 3
        elif dept_level.find('团') >= 0:
            self.ctype_cp = 4
        elif dept_level.find('组') >= 0:
            self.ctype_cp = 5
        elif dept_level.find('个人') >= 0 or dept_level.find('顾问') >= 0 or dept_level.find('员工') >= 0:
            self.ctype_cp = 6
        else:
            self.ctype_cp = 0

    # 拼接sql语句中的部门ID条件和部门字段
    def deptSQL(self):
        self.organidQuery()  # 计算require['部门粒度'],获得部门的organid 和 ctype

        # ①有部门粒度
        if type(self.require.get('部门粒度')) == str:
            self.ctypeCompare()  # 计算require['部门粒度']，获得部门粒度ctype_cp

            if self.ctype_cp > self.ctype:  # 如果指定统计部门粒度 比 指定的部门级别小，则覆盖之前的ctype
                self.ctype = self.ctype_cp
                self.dept_reduction = 1
            else:  # 如果指定统计部门粒度 比 指定的部门级别大/等于，以当前ctype查询即可
                self.dept_reduction = 0

        # ②无部门粒度则不用对比，直接根据指定部门的来
        else:
            self.dept_reduction = 0

        depart_select = {2: 'depart_1st,', 3: 'depart_2nd,', 4: 'depart_3rd,', 5: 'groupname,', 6: 'staffname,'}

        if self.dept_reduction == 1 and self.ctype > 2:  # "部门粒度"比"部门"的级别低时：sql查询的部门往上展示多一级
            self.depart_select = depart_select[self.ctype - 1] + depart_select[self.ctype]
        else:
            self.depart_select = depart_select[self.ctype]

        departid = {2: 'departid_1st', 3: 'departid_2nd', 4: 'departid_3rd', 5: 'departid_3rd', 6: 'departid_3rd'}
        self.depart_where = departid[self.ctype] + ' like \'' + self.sql_organid + '%\''
机器人 Readme.txt

版本号命名：
主版本号.接口编号.修订次数
V 1.0.1（V1版本的主接口，第1版）
    __init__.py     接口
    Group.py        群查询
    Query.py        接口计算
    User.py         用户（权限）识别

V 1.1.0 模糊需求（V1版本的模糊需求接口，开发环境）

    处理示例： 五营今天业绩是多少

V 1.2.5 规整需求（V1版本的规整需求接口，开发环境，第4次修改）
    处理示例： 体管五营 202107 下单业绩

    类名                         文件内容        子类
    _init_.py                   接口代码
    Query_Body.py               查询体

    Demand_Interpret.py         解析器          查询体
    Query_Record.py             查询记录入库     查询体
    Robot.py                    机器人
    Handler.py                  出bug回复内容

    Date_Processing.py          日期处理        解析器
    Department_Processing.py    部门处理        解析器
    Indicator_Recognision.py    指标识别        解析器

    修改次数    修改日期        修改人         修改内容
       4      20210719        CJZ          ①Query_Record-查询入库功能上线，②Demand_Interpret-指标可查询到月
       5      20210720        CJZ          ①Demand_Interpret-计算指标的值=分子/分母，②Date_Processing-泛化时间

V 1.3.0 明细需求 (V1版本的模糊需求接口，开发环境)


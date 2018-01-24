# -*- coding: utf-8 -*-
# @Time : 2017/12/14 15:35
# @Author : xionglinquan
# @Site :
# @File : auto_generate_partitions.py
# @Desc : 自动生成Oracle添加分区脚本，需安装相关包cx_Oracle、dateutil、pandas，运行环境win+cmd下
# @license : Copyright(C), TYDIC
# @Contact : xionglinquan@gmail.com

'''
输入1查看range类型分区表信息
输入2生成添加分区脚本
输入3退出程序
注意：分区命名必须为类似P201701
'''

import cx_Oracle
from datetime import datetime
from dateutil.relativedelta import *
import pandas as pd
import os
import sys

def usage():
    print u'''
Usage:python auto_generate_partitions.py 数据库用户名 数据库密码 tns串
    '''.encode("gbk")


def get_tab_info(user,passwd,tns):
    connection=cx_Oracle.connect(user+"/"+passwd+"@"+tns)
    cursor=connection.cursor()
    get_part_table_sql="""
    SELECT A.*, K.OWNER, K.NAME, K.COLUMN_NAME, TC.DATA_TYPE
      FROM (SELECT TABLE_OWNER,
                   TABLE_NAME,
                   PARTITION_NAME,
                   TABLESPACE_NAME,
                   PARTITION_POSITION,
                   HIGH_VALUE
              FROM (SELECT /*+ rule */
                     T1.TABLE_OWNER,
                     T1.TABLE_NAME,
                     T1.PARTITION_NAME,
                     T1.TABLESPACE_NAME,
                     PARTITION_POSITION,
                     HIGH_VALUE,
                     ROW_NUMBER() OVER(PARTITION BY T1.TABLE_OWNER, T1.TABLE_NAME ORDER BY PARTITION_POSITION DESC) RN
                      FROM DBA_TAB_PARTITIONS T1, DBA_PART_TABLES T2
                     WHERE T1.TABLE_OWNER = T2.OWNER
                       AND T1.TABLE_NAME = T2.TABLE_NAME
                       AND T2.PARTITIONING_TYPE = 'RANGE')
             WHERE RN = 1) A,
           DBA_PART_KEY_COLUMNS K,
           DBA_TAB_COLUMNS TC
     WHERE K.NAME = A.TABLE_NAME
       AND K.COLUMN_POSITION = 1
       AND K.OBJECT_TYPE = 'TABLE'
       AND K.OWNER = TC.OWNER
       AND K.NAME = TC.TABLE_NAME
       AND K.COLUMN_NAME = TC.COLUMN_NAME
       AND TABLE_OWNER NOT IN ('SYSTEM', 'SYS')
      
    """

    cursor.execute(get_part_table_sql)
    list_tab_info=[]
    for row in cursor.fetchall():
        dict_tab_info={}
        dict_tab_info["table_owner"]=row[0]
        dict_tab_info["table_name"]=row[1]
        dict_tab_info["part_name"]=row[2]
        dict_tab_info["tablespace_name"]=row[3]
        dict_tab_info["part_max_time"]=row[5]
        dict_tab_info["part_column"]=row[8]
        dict_tab_info["part_type"]=row[9]
        list_tab_info.append(dict_tab_info)

        # create_time=row[3].strftime("%Y%m%d%H%M%S")

        #判断是否是max类型分区
        list_default_part_name = ["PDEFAULT","P_OTHER"]
        if dict_tab_info["part_name"] in list_default_part_name:
            max_part_name=dict_tab_info["part_name"]
            #删掉之前存的内容
            list_tab_info.remove(dict_tab_info)
            cursor1 = connection.cursor()
            get_part_table_sql1="""
            SELECT A.*, K.OWNER, K.NAME, K.COLUMN_NAME, TC.DATA_TYPE
              FROM (SELECT TABLE_OWNER,
                           TABLE_NAME,
                           PARTITION_NAME,
                           TABLESPACE_NAME,
                           PARTITION_POSITION,
                           HIGH_VALUE
                      FROM (SELECT /*+ rule */
                             T1.TABLE_OWNER,
                             T1.TABLE_NAME,
                             T1.PARTITION_NAME,
                             T1.TABLESPACE_NAME,
                             PARTITION_POSITION,
                             HIGH_VALUE,
                             ROW_NUMBER() OVER(PARTITION BY T1.TABLE_OWNER, T1.TABLE_NAME ORDER BY PARTITION_POSITION DESC) RN
                              FROM DBA_TAB_PARTITIONS T1, DBA_PART_TABLES T2
                             WHERE T1.TABLE_OWNER = T2.OWNER
                               AND T1.TABLE_NAME = T2.TABLE_NAME
                               AND T2.PARTITIONING_TYPE = 'RANGE')
                     WHERE RN = 2) A,
                   DBA_PART_KEY_COLUMNS K,
                   DBA_TAB_COLUMNS TC
             WHERE K.NAME = A.TABLE_NAME
               AND K.OBJECT_TYPE = 'TABLE'
               AND K.OWNER = TC.OWNER
               AND K.NAME = TC.TABLE_NAME
               AND K.COLUMN_NAME = TC.COLUMN_NAME
               AND A.TABLE_OWNER = '%s'
               AND A.TABLE_NAME = '%s'
            """ % (dict_tab_info["table_owner"],dict_tab_info["table_name"])
            #添加新内容
            cursor1.execute(get_part_table_sql1)
            for row1 in cursor1.fetchall():
                dict_tab_info = {}
                dict_tab_info["table_owner"] = row1[0]
                dict_tab_info["table_name"] = row1[1]
                dict_tab_info["part_name"] = row1[2]
                dict_tab_info["tablespace_name"] = row1[3]
                dict_tab_info["part_max_time"] = row1[5]
                dict_tab_info["part_column"] = row1[8]
                dict_tab_info["part_type"] = row1[9]
                #max_part属性
                dict_tab_info["max_part_name"] = max_part_name
            list_tab_info.append(dict_tab_info)
            cursor1.close()
    cursor.close()
    connection.close()
    return list_tab_info

#默认脚本保存在当前目录
def save_to_file(content,path=os.getcwd()):
    current_date=datetime.now().strftime("%Y%m%d")
    file_name=path+"\\add_part_script_"+current_date+".sql"
    #如果文件存在删除
    if os.path.isfile(file_name):
        os.remove(file_name)

    with open(file_name,"a+") as f:
        f.writelines(content)



def generate_scripts(table_owner,table,tbs,part_name,max_part_date,max_part_name=''):
    if not max_part_name:
        extend_script="alter table %s.%s add partition %s values less than (%s) tablespace %s;\n" % (table_owner,table,part_name,max_part_date,tbs)
    else:
        extend_script = "alter table %s.%s split partition %s at (%s) INTO (partition %s, partition %s);\n" % (table_owner,table,max_part_name,max_part_date,part_name,max_part_name)

    return extend_script

def datelist(beginDate, endDate):
    date_list = [datetime.strftime(x, '%Y%m') for x in list(pd.date_range(start=beginDate, end=endDate,freq='M'))]
    return date_list

def addMonth(date,interval_month):
    return date+relativedelta(months=interval_month)

def choice(user,passwd,tns):
    while True:
        n=raw_input(
u'''\n\n#################Oracle自动分区脚本#################
1.查看当前库表分区情况
2.生成当前库增加分区脚本-->请输入要分区到几几年1月前
3.退出
####################################################\n\n
按键请选择要操作的类型：'''.encode("gbk"))

        if n == '1':
            for i in get_tab_info(user,passwd,tns):
                print "####################################\nTABLE_OWNER: %s\nTABLE_NAME: %s\nTABLESPACE_NAME: %s\nPART_MAX_TIME: %s\n####################################\n\n\n" % (i["table_owner"],i["table_name"],i["tablespace_name"],i["part_max_time"])

        elif n == '2':
            current_year=datetime.now().year
            list_tab_info=get_tab_info(user,passwd,tns)
            input_year=raw_input(u"请输入要分区到几几年1月前(格式yyyy，例如：生成分区到2018年年底，则输入2019)：".encode("gbk"))
            #输入年份如果小于或等于当前年份，抛异常
            if len(input_year) != 4:
                raise Exception(u"Error:Sorry,输入的格式错误！格式：yyyy".encode("gbk"))
            if input_year < current_year or input_year == current_year:
                raise Exception(u"Error:Sorry,输入年份必须大于当前年份！".encode("gbk"))
            else:

                result_list=[]
                for i in list_tab_info:
                    #通过分区名决定分区时间，因此分区名必须规范
                    if len(i["part_name"]) != 7:
                        continue


                    start_time=datetime.strftime(addMonth(datetime.strptime(i["part_name"][1:],"%Y%m"),1),"%Y%m")
                    list_date=datelist(datetime.strptime(start_time, '%Y%m'), datetime.strptime(input_year+"01", '%Y%m'))
                    for p_month in list_date:
                        str_part_name="P"+p_month
                        month_date=datetime.strptime(p_month, '%Y%m')
                        if i["part_type"] == "NUMBER":
                            str_max_part_date=datetime.strftime(addMonth(month_date,1),'%Y%m')
                        else:
                            str_max_part_date="TO_DATE('"+datetime.strftime(addMonth(month_date,1),'%Y-%m-%d %H:%M:%S')+"', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"

                        #判断是否含有max_part
                        if not i.has_key("max_part_name"):
                            result=generate_scripts(i["table_owner"],i["table_name"],i["tablespace_name"],str_part_name,str_max_part_date)
                        else:
                            result=generate_scripts(i["table_owner"],i["table_name"],i["tablespace_name"],str_part_name,str_max_part_date,i["max_part_name"])
                        print result
                        result_list.append(result)
                save_to_file(result_list)
                print u"已保存到本地文件。".encode("gbk")
        elif n == '3':
            break
        else:
            raise Exception(u"Error:Sorry,只能选择123，请重新选择！".encode("gbk"))



if __name__=="__main__":
    # 数据库用户密码
    if len(sys.argv) != 4:
        print u"Error:参数个数错误！".encode("gbk")
        usage()
    else:
        username,password,tnsname=sys.argv[1:]

        # try:
        #     choice(username,password,tnsname)
        # except Exception as err:
        #     print "Encounter an error: "+str(err)
        choice(username, password, tnsname)

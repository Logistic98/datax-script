# -*- coding: utf-8 -*-

import json
import os
import string
import pymysql


# 查询mysql的所有字段列
def query_mysql_column(mysql_connect, mysql_db, mysql_table):
    cursor = mysql_connect.cursor()
    sql = "SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ',') FROM information_schema.COLUMNS WHERE " \
          "TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'".format(mysql_db, mysql_table)
    cursor.execute(sql)
    result = cursor.fetchall()[0][0]
    cursor.close()
    return result


# 按行追加写入文件（没有文件会新创建文件）
def write_content_to_file(file_path, content):
    a = open(file_path, 'a')
    a.write(content + '\n')
    a.close()


if __name__ == '__main__':

    ## 1. 填写原始配置信息
    # 1.1 job配置信息
    job_base_path = "/root/datax/job"
    job_name = "generate_msyql2mysql_job_test.json"
    # 1.2 mysql reader配置信息
    reader_mysql_host = '127.0.0.1'
    reader_mysql_user = 'root'
    reader_mysql_password = 'reader_mysql_password'
    reader_mysql_port = 3306
    reader_mysql_db = 'reader_mysql_db'
    reader_mysql_table = 'reader_mysql_table'
    # 1.3 mysql writer配置信息
    writer_mysql_host = '127.0.0.1'
    writer_mysql_user = 'root'
    writer_mysql_password = 'writer_mysql_password'
    writer_mysql_port = 3306
    writer_mysql_db = 'writer_mysql_db'
    writer_mysql_table = 'writer_mysql_table'

    ## 2. 将原始配置信息构造二级配置信息
    job_file_path = job_base_path + "/" + job_name
    reader_jdbcUrl = "jdbc:mysql://{}:{}/{}?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=GMT%2B8" \
        .format(reader_mysql_host, reader_mysql_port, reader_mysql_db)
    writer_jdbcUrl = "jdbc:mysql://{}:{}/{}?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=GMT%2B8" \
        .format(writer_mysql_host, writer_mysql_port, writer_mysql_db)

    ## 3. 查询字段信息并处理
    # 3.1 查询reader中的MySQL字段信息并拼接
    # 获取逗号分割的字段字符串
    reader_mysql_connect = pymysql.connect(host=reader_mysql_host, user=reader_mysql_user,
                                           password=reader_mysql_password, port=reader_mysql_port,
                                           db=reader_mysql_db, charset='utf8')
    reader_mysql_column = query_mysql_column(reader_mysql_connect, reader_mysql_db, reader_mysql_table)
    reader_mysql_connect.close()
    # 拼接querySql
    reader_querySql = "select {} from {} where 1=1".format(reader_mysql_column, reader_mysql_table)
    # 3.2 查询writer中的MySQL字段列表
    # 获取逗号分割的字段字符串
    writer_mysql_connect = pymysql.connect(host=writer_mysql_host, user=writer_mysql_user,
                                           password=writer_mysql_password, port=writer_mysql_port,
                                           db=writer_mysql_db, charset='utf8')
    writer_mysql_column = query_mysql_column(writer_mysql_connect, writer_mysql_db, writer_mysql_table)
    writer_mysql_connect.close()
    writer_mysql_column_list = writer_mysql_column.split(",")
    result = []
    for writer_mysql_column_item in writer_mysql_column_list:
        result.append("`{}`".format(writer_mysql_column_item))
    # 格式化输出json
    writer_column = json.dumps(result, sort_keys=True, indent=2)

    ## 4. 使用模板生成job配置
    # 替换模板中的变量
    file = open('msyql2mysql_template.json', encoding='utf-8')
    content = file.read()
    template_setting = string.Template(content)
    job_config = template_setting.safe_substitute(reader_username=reader_mysql_user,
                                                  reader_password=reader_mysql_password,
                                                  reader_jdbcUrl=reader_jdbcUrl, reader_querySql=reader_querySql,
                                                  writer_username=writer_mysql_user,
                                                  writer_password=writer_mysql_password,
                                                  writer_jdbcUrl=writer_jdbcUrl, writer_table=writer_mysql_table,
                                                  writer_column=writer_column)
    # 输出结果到文件
    print(job_config)
    if os.path.exists(job_file_path):
        os.remove(job_file_path)
    write_content_to_file(job_file_path, job_config)
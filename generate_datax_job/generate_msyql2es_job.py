# -*- coding: utf-8 -*-

import json
import os
import string
import pymysql
from elasticsearch import Elasticsearch


# 获取单个ES索引的详细信息
def get_index_info(es_connect, writer_es_index):
    return es_connect.indices.get_mapping(index=writer_es_index)


# 查询mysql的所有字段列
def query_mysql_column(mysql_connect, reader_mysql_db, reader_mysql_table):
    cursor = mysql_connect.cursor()
    sql = "SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ',') FROM information_schema.COLUMNS WHERE " \
          "TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'".format(reader_mysql_db, reader_mysql_table)
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
    job_name = "generate_msyql2es_job_test.json"
    # 1.2 mysql reader配置信息
    reader_mysql_host = '127.0.0.1'
    reader_mysql_user = 'root'
    reader_mysql_password = 'mysql_password'
    reader_mysql_port = 3306
    reader_mysql_db = 'mysql_db'
    reader_mysql_table = 'mysql_table'
    # 1.3 es writer配置信息
    writer_es_endpoint = "http://ip:port"
    writer_es_accessId = "elastic"
    writer_es_accessKey = "es_password"
    writer_es_index = "es_index"

    ## 2. 将原始配置信息构造二级配置信息
    job_file_path = job_base_path + "/" + job_name
    reader_jdbcUrl = "jdbc:mysql://{}:{}/{}?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=GMT%2B8"\
        .format(reader_mysql_host, reader_mysql_port, reader_mysql_db)

    ## 3. 查询字段信息并处理
    # 3.1 查询MySQL字段信息并拼接
    # 获取逗号分割的字段字符串
    mysql_connect = pymysql.connect(host=reader_mysql_host, user=reader_mysql_user,
                                    password=reader_mysql_password, port=reader_mysql_port,
                                    db=reader_mysql_db, charset='utf8')
    reader_mysql_column = query_mysql_column(mysql_connect, reader_mysql_db, reader_mysql_table)
    mysql_connect.close()

    # 拼接querySql
    reader_querySql = "select {} from {} where 1=1".format(reader_mysql_column, reader_mysql_table)
    # 3.2 查询ES字段信息并处理
    # 获取单个索引的信息
    es_connect = Elasticsearch(
        hosts=[writer_es_endpoint],
        http_auth=(writer_es_accessId, writer_es_accessKey),
        timeout=60
    )
    index_info = get_index_info(es_connect, writer_es_index)
    index_mappings_info = index_info[writer_es_index]['mappings']['properties']
    # 遍历字典，过滤key中包含@的（@timestamp、@version），按照datax的column重新生成格式
    result = []
    default_dict_item = {"name": "pk", "type": "id"}
    result.append(default_dict_item)
    for key, value in index_mappings_info.items():
        dict_item = {}
        if "@" not in key:
            dict_item['name'] = key
            dict_item['type'] = value['type']
            result.append(dict_item)
    # 格式化输出json
    writer_column = json.dumps(result, sort_keys=True, indent=2)

    ## 4. 使用模板生成job配置
    # 替换模板中的变量
    file = open('msyql2es_template.json', encoding='utf-8')
    content = file.read()
    template_setting = string.Template(content)
    job_config = template_setting.safe_substitute(reader_username=reader_mysql_user, reader_password=reader_mysql_password,
                                                  reader_jdbcUrl=reader_jdbcUrl, reader_querySql=reader_querySql,
                                                  writer_endpoint=writer_es_endpoint, writer_accessId=writer_es_accessId,
                                                  writer_accessKey=writer_es_accessKey, writer_index=writer_es_index,
                                                  writer_column=writer_column)
    # 输出结果到文件
    print(job_config)
    if os.path.exists(job_file_path):
        os.remove(job_file_path)
    write_content_to_file(job_file_path, job_config)

    ## 5. 附加：生成清空该ES索引的脚本
    clear_es_index_script_path = 'clear_es_index_data.sh'
    content_template = string.Template("curl -u ${writer_es_accessId}:${writer_es_accessKey} -XPOST '${writer_es_endpoint}/${writer_es_index}/_delete_by_query?refresh&slices=5&pretty' -H 'Content-Type: application/json' -d'{\"query\": {\"match_all\": {}}}'")
    script_content = content_template.safe_substitute(writer_es_accessId=writer_es_accessId, writer_es_accessKey=writer_es_accessKey, writer_es_endpoint=writer_es_endpoint, writer_es_index=writer_es_index)
    write_content_to_file(clear_es_index_script_path, script_content)
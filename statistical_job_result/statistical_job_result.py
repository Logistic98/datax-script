# -*- coding: utf-8 -*-

import os
import csv
import time
import schedule


# 读取指定目录下的所有文件夹保存成列表
def read_dir_to_list(file_dir_path):
    file_dir_list = os.listdir(file_dir_path)
    return file_dir_list


# 按行读取txt文件的内容，保存成列表
def read_txt_to_list(txt_path):
    result = []
    with open(txt_path, 'r') as f:
        for line in f:
            result.append(line.strip('\n'))
    return result


# 创建csv并写入字典
def create_csv(fieldnames, path, dict):
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(dict)


# 将字典追加写入csv
def append_csv(fieldnames, path, dict):
    with open(path, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerow(dict)


# 统计任务最后执行时间写入CSV文件
def statistical_job_result(job_txt_path, job_result_path):

    statistical_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    job_txt_file_list = read_dir_to_list(job_txt_path)
    fieldnames = []
    fieldnames.append('statistical_time')
    csv_data = {}
    csv_data['statistical_time'] = statistical_time
    for job_txt_file in job_txt_file_list:
        file_name, file_ext = os.path.splitext(job_txt_file)      # 截取文件名作为csv的列名
        job_last_update = read_txt_to_list(job_txt_path + "/" + job_txt_file)[0]       # 读取job的最后更新时间
        csv_data[file_name] = job_last_update
        fieldnames.append(file_name)

    if not os.path.exists(job_result_path):
        create_csv(fieldnames, job_result_path, csv_data)
    else:
        append_csv(fieldnames, job_result_path, csv_data)


if __name__ == '__main__':

    job_txt_path = '../job/txt'
    job_result_path = './job_result.csv'

    # 配置定时任务，可同时启动多个
    # schedule.every(30).minutes.do(statistical_job_result(job_txt_path, job_result_path))         # 每隔30分钟运行一次job
    schedule.every().hour.do(statistical_job_result(job_txt_path, job_result_path))              # 每隔1小时运行一次job
    # schedule.every().day.at("23:59").do(statistical_job_result(job_txt_path, job_result_path))   # 每天在23:59时间点运行job
    # schedule.every().monday.do(statistical_job_result(job_txt_path, job_result_path))            # 每周一运行一次job

    statistical_job_result(job_txt_path, job_result_path)  # 启动时立即执行一次，随后进入定时任务
    while True:
        schedule.run_pending()
        time.sleep(1)

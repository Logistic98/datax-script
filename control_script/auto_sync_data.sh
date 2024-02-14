#!/bin/bash

# 错误检查，有错误的时候退出
set -e

# dataX脚本里需要的参数
systemTime=`date +%Y-%m-%d,%H:%M:%S`

# 获取当前时间，这个是同步后要写到txt文件的变量
currentTime=`date +"%Y-%m-%d %H:%M:%S"`
echo ${currentTime}

# datax根路径定义[需要修改]
basePath=/root/datax
txtPath=${basePath}/job/txt
if [ ! -d "${txtPath}" ];then
    mkdir ${txtPath}
fi
tempPath=${basePath}/job/temp
if [ ! -d "${tempPath}" ];then
    mkdir ${tempPath}
fi

# python命令版本[需要修改]
pythonCmd=python3  # python或python3

# json配置文件数组[需要修改]
jsonArray[${#jsonArray[*]}]=demo_mysql2mysql.json
jsonArray[${#jsonArray[*]}]=demo_mysql2es.json
# ...

# 遍历json文件
for i in ${jsonArray[@]}
do
    # 去除.json，拼接获取txt文件名
    echo ${i}
    txtName=${i%.*}.txt
    echo ${txtName}

    # 找到txt文本文件，并将内容读取到一个变量中
    if [ ! -f "${txtPath}" ];then
        touch ${txtPath}/${txtName}
    fi
    MAX_TIME=`cat ${txtPath}/${txtName}`
    echo ${MAX_TIME}

    # 如果最大时间不为""的话，修改全部同步的配置，进行增量更新；如果最大时间为null ,进行全量更新;
    if [ "$MAX_TIME" != "" ]; then
        # 设置增量更新过滤条件
        WHERE="update_time > str_to_date('$MAX_TIME', '%Y-%m-%d %H:%i:%s')"
        # 创建改写后的json临时配置文件（把1=1替换成上面的where条件）
        sed "s/1=1/$WHERE/g" ${basePath}/job/${i} > ${tempPath}/${i/.json/_temp.json}
        # 增量更新
        ${pythonCmd} ${basePath}/bin/datax.py -p "-DsystemTime='$systemTime'" -j "-Xms1g -Xmx1g" ${tempPath}/${i/.json/_temp.json}
    else
        # 全量更新
        ${pythonCmd} ${basePath}/bin/datax.py -p "-DsystemTime='$systemTime'" -j "-Xms1g -Xmx1g"  ${basePath}/job/${i}
    fi

    if [ $? -ne 0 ]; then
      # 执行失败
      echo "datax task execution failed"
    else
      # 执行成功，将最大日期写入txt文件，覆盖写入
      echo  ${currentTime} > ${txtPath}/${txtName}
    fi

# 删除临时文件
rm -rf ${tempPath}/${i/.json/_temp.json}

done
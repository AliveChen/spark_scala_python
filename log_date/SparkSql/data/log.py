# encoding:utf-8

"""
# author:chenpeng
# time:2019-07-06 10:57
describle:
    生成 模拟日志数据，
    供spark日志分析
"""

import time
import random


def timeRandomCreate():    
    # 生成随机时间格式
    
    timestart = (2019,1,1,0,0,0,0,0,0)    # 设置开始时间（19年0点）
    timeend =(2019,7,6,23,59,59,0,0,0)    # 深圳结束时间19年7-06号24点

   # 生成开始和结束的时间戳
    startTime = time.mktime(timestart)
    endTime = time.mktime(timeend)

    t=random.randint(startTime,endTime)    #在开始和结束时间戳中随机取出一个
    date_touple=time.localtime(t)
    # print(date_touple)
    # 将时间戳生成时间元组
    date=time.strftime("%Y-%m-%d %H:%M:%S",date_touple)  #将时间元组转成格式化字符串（1976-05-21）
    return date


"""
# 日志格式
# id;int ip:str url:str traffic:int time:str
"""

# 把爬虫采集的ip文件读进来，也可以自己模拟生成
ipDate = open("ip.txt").readlines()
cmsType = ["spark", "python","java", "scala", "php","go"]
logDate = open("log.txt","w")
for i in range(10000):
    logDate.write("https+url"+"\t" + str(cmsType[random.randint(0,5)]) +"subject" +"\t" +
                  str(random.randint(10,20)) + "\t"+ str(random.randint(0,1000))
                  +"\t"+ str(ipDate[random.randint(0,143)].split()[1]) + "\t"
                  + str(random.randint(0,1000)) +"\t" + str(random.randint(1,34))
                  +"city" + "\t" + str(timeRandomCreate()) +"\n")

# ipDate.close()

logDate.close()    # 不关闭IO流文本为空





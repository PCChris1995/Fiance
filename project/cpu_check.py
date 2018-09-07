'''/*
 * @Author: PCChris: https://github.com/PCChris1995/Fiance 
 * @Date: 2018-09-07 10:23:42 
 * @Last Modified by:   PCChris 
 * @Last Modified time: 2018-09-07 10:23:42 
 */'''
#coding:utf-8

import matplotlib.pyplot as plt
import string
def getCpuInfData(fileName):
    ret = {}
    f = open(fileName,"r")
    lineList = f.readlines()
    for line in lineList:
        tmp = line.split()
        sz = len(tmp)
        t_key = string.atoi(tmp[0]) # 得到key
        t_value = 100.001-string.atof(line.split(':')[1].split(',')[3].split('%')[0]) # 得到value
        print(t_key,t_value)  
        if not ret.has_key(t_key) :
            ret[t_key] = []
        ret[t_key].append(t_value)
    f.close()
    return ret
    
retMap1 = getCpuInfData("file.txt")
# 生成CPU使用情况趋势图
list1 = retMap1.keys()
list1.sort() 
list2 = []
for i in list1:list2.append(retMap1[i])
plt.plot(list1,list2)
plt.show()
#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, datetime, os, simplejson, subprocess
from collections import defaultdict
from unipath import Path
import cPickle,ConfigParser
import lz4,csv
import glob

import YhLog,  YhTool
logger = logging.getLogger(__file__)

cwd = Path(__file__).absolute().ancestor(1) 
step = 100000
def load_doctor(ifn='/data/data/all/club/expert_doctor.csv', cols=[('id', 0), ('name', 1), ('pinyin', 2), ('sex', 3), ('birthday', 4), ('docimg', 5),('goodat', 11)], ofn_pic='./pic/doc.pic', quotechar='"', test=0):
    list_data = []
    for l in csv.reader(open(ifn), delimiter=',', quotechar=quotechar, quoting=csv.QUOTE_ALL):
        #logger.error('|'.join(l[:12]))
        dict_d = {}
        for (k,v) in cols:
            dict_d[k] = l[v]
        
        #logger.error(dict_d)
        list_data.append(dict_d)
    logger.error('load_doctor total %s' % len(list_data))
    for i in range(len(list_data)/step+1):
        list_buf = list_data[i * step : (i+1) * step]
        if list_buf:
            ofn_buf = '%s.%s' % (ofn_pic, i)
            cPickle.dump(list_buf, open(ofn_buf, 'w+'))
            logger.error('%s len %s' % (ofn_buf, len(list_buf)))
    if test:
        validate(list_data)
        
def validate(list_data=[]):
    for l in list_data[:10]:
        logger.error('|'.join(l.values()))
        
def run():
    #doctor
    load_doctor(quotechar='\'', test=1)
    #load_doctor(ifn='/data/data/all/club/expert_ill.csv', cols=[('id', 0), ('name', 1), ('pinyin', 2), ('samename', 3)], ofn_pic='./pic/ill.pic')
    load_doctor(ifn='/data/data/all/club/ixywy_new_ill.csv', cols=[('id', 0), ('name', 2), ('samename', 3)], ofn_pic='./pic/ill.pic', test=1)
    load_doctor(ifn='/data/data/all/club/ixywy_new_jck.csv', cols=[('id', 0), ('name', 2), ('pname', 3)], ofn_pic='./pic/jck.pic', test=1, quotechar='\'')
    load_doctor(ifn='/data/data/all/club/ixywy_new_zzk.csv', cols=[('id', 0), ('name', 1), ('pjck', 2)], ofn_pic='./pic/zzk.pic', test=1)
    load_doctor(ifn='/data/data/all/club/expert_hospital.csv', cols=[('id', 0), ('name', 1), ('smallname', 3), ('samename', 4)], ofn_pic='./pic/hospital.pic', test=1, quotechar='\'')
    load_doctor(ifn='/data/data/all/club/searchdb_wksc_yao_teble.csv', cols=[('id', 0), ('name', 2)], ofn_pic='./pic/yao.pic', test=1)
if __name__=='__main__':
    run()
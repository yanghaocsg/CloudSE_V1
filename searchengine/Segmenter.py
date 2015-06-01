#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson, subprocess
from multiprocessing import Pool, Queue, Lock, managers, Process
import copy
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle
sys.path.append('/data/CloudSE/AdWords')

#darts for entity
from AdDarts import adDarts


import YhLog, YhChineseNorm,Query

logger = logging.getLogger(__file__)

#bigram seg based on darts
def biseg_darts(query=''):
    list_darts = adDarts.run(query,filter=1)
    list_res = []
    for l in list_darts:
        list_res.extend(YhChineseNorm.string2ListBigram(l))
    logger.error('biseg_darts %s\t%s' % (query, '|'.join(list_res)))
    return list(set(list_res))
        

dict_seg ={0:YhChineseNorm.string2List, 1:YhChineseNorm.string2ListBigram, 2:Query.yhTrieSeg.seg, 3:biseg_darts}


    
def get_seg(id=0):
    return dict_seg[id]

if __name__=='__main__':
    #for i in range(3):
    #    list_s = dict_seg[i](u'你好，北京欢迎你，360')
    #    logger.error(list_s)
    for i in range(len(dict_seg)):
        list_res = dict_seg[i](u"左边屁股长十多个大包腿上也有硬皮里肉外")
        logger.error('seg %s\t%s' % (i, '|'.join(list_res)))
    

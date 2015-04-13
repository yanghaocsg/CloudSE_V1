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

import YhLog, YhChineseNorm,Query

logger = logging.getLogger(__file__)
dict_seg ={0:YhChineseNorm.string2List, 1:YhChineseNorm.string2ListBigram, 2:Query.yhTrieSeg.seg}
def get_seg(id=0):
    return dict_seg[id]

if __name__=='__main__':
    #for i in range(3):
    #    list_s = dict_seg[i](u'你好，北京欢迎你，360')
    #    logger.error(list_s)
    list_res = dict_seg[0](u"我是新疆的，宝宝现在六个多月，能不能开")
    logger.error(list_res)

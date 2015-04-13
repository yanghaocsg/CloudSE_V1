#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, datetime, os, simplejson, subprocess
from collections import defaultdict
from unipath import Path
import cPickle,ConfigParser
import lz4,csv
import glob

import YhLog,  YhTool, YhChineseNorm
logger = logging.getLogger(__file__)

cwd = Path(__file__).absolute().ancestor(1) 
step = 100000
def prebuildEntity(ifn='./pic/entity_zzk.pic'):
    dict_data = cPickle.load(open(ifn))
    ofn = '%s.test' % ifn
    open(ofn,'w+').write(('\n'.join(dict_data.keys())).encode('utf8', 'ignore'))
        

def run(ifn=''):
    prebuildEntity()
if __name__=='__main__':
    if len(sys.argv)>=2 and sys.argv[1]:
        run(sys.argv[1])
    else:
        run()
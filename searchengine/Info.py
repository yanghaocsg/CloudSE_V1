#!/usr/bin/env python
#coding:utf8
import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
import string,ConfigParser
from unipath import Path
import cPickle, lz4, glob


#self module
sys.path.append('../YhHadoop')
import YhLog, YhTool
import Redis_zero
logger = logging.getLogger(__file__)

class Info:
    def __init__(self, conf='./conf/se.conf', head='info'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.config = ConfigParser.ConfigParser({'src':'','prefix':''})
        self.config.read(Path(self.cwd, conf))
        self.src = self.config.get(head, 'src')
        self.prefix = self.config.get(head, 'prefix')
        
    def getInfoById(self, list_id=range(100)):
        list_res = Redis_zero.redis_zero.hmget(self.prefix, list_id[:200])
        list_info = []
        for l in list_res:
            try:
                if l:
                    list_info.append(simplejson.loads(l))
            except:
                logger.error('%s\t%s' % (l, traceback.format_exc()))
        logger.error('getInfoById req %s res %s info %s' % (len(list_id), len(list_res), len(list_info)))
        return list_info
    
    def saveInfo(self):
        pipeline_zero = Redis_zero.redis_zero.pipeline()
        num_execute = 0
        for f in glob.glob('%s*' % self.src):
            try:
                dict_info = cPickle.load(open(f))
                for k in dict_info:
                    pipeline_zero.hset(self.prefix, k, simplejson.dumps(dict_info[k]))
                    num_execute += 1
                    if num_execute % 10000 == 0:
                        pipeline_zero.execute()
                logger.error('saveInfo %s %s' % (f, num_execute))
            except:
                logger.error('saveInfo failed %s %s' % (f, traceback.format_exc()))
        pipeline_zero.execute()
        logger.error('saveInfo %s' % num_execute)


info = Info()        

if __name__=='__main__':
    info.saveInfo()
    info.getInfoById()
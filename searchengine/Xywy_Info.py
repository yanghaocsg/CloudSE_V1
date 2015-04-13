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

class Xywy_Info(object):
    def __init__(self, conf="./conf/xywy_se.conf", head = "doctor_info"):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.config = ConfigParser.ConfigParser()
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

    def save_info(self):
        pipeline_zero = Redis_zero.redis_zero.pipeline()
        num_execute = 0
        print self.src
        for f in glob.glob('%s*' % self.src):
            print f
            try:
                dict_info = cPickle.load(open(f))
                for key,value in dict_info.items():
                    pipeline_zero.hset(self.prefix, key, simplejson.dumps(value))
                    #print key, value["name"].encode("utf-8")
                    num_execute += 1
                    if num_execute % 10000 == 0:
                        pipeline_zero.execute()
                logger.error('saveDoctorInfo %s %s' % (f, num_execute))
            except:
                logger.error('saveDoctorInfo failed %s %s' % (f, traceback.format_exc()))
        pipeline_zero.execute()
        logger.error('saveDoctorInfo %s' % num_execute)


    def get_info(self, doctor_id):
        print self.prefix
        res = Redis_zero.redis_zero.hmget(self.prefix, doctor_id)
        print simplejson.loads(res[0])

doctor_info = Xywy_Info(conf="./conf/xywy_se.conf", head = "doctor_info")
ill_info = Xywy_Info(conf="./conf/xywy_se.conf", head="ill_info")
hospital_info = Xywy_Info(conf="./conf/xywy_se.conf", head="hospital_info")
yao_info = Xywy_Info(conf="./conf/xywy_se.conf", head="yao_info")
zixun_info = Xywy_Info(conf="./conf/xywy_se.conf", head="zixun_info")

if __name__ == "__main__":
    doctor_info.save_info()
    yao_info.save_info()
    ill_info.save_info()
    hospital_info.save_info()
    zixun_info.save_info()
    doctor_info.get_info(100)
    ill_info.get_info(100)
    hospital_info.get_info(100)
    yao_info.get_info(100)
    zixun_info.get_info(100)

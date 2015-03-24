#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis, traceback, time, os, simplejson, datetime
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle, lz4
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import tornado.gen, tornado.web

import Searcher
import Xywy_Searcher
sys.path.append('../queryprocess')
from ErrorCorrection import ec
from RelatedSearch import rs

#self module
import YhLog, YhTool, YhMc
logger = logging.getLogger(__file__)

class Compose_Searcher(object):
    def __init__(self, company="xywy_com", db="tag_com", cache_prefix="comse_cache"):
        self.cache_prefix = "%s:%s" %(cache_prefix, company)

    def process(self, query='', start=0, num=10, cache=1, mark=1, st=1):
        last_res = {}
        bbs_num = 200
        other_num = 10
        try:
            cache_key = "%s:%s" %(self.cache_prefix, query)
            cache_res = YhMc.yhMc.get_cache(cache_key)
            if cache_res and cache:
                last_res = simplejson.loads(cache_res)
            else:
                doc_res = Xywy_Searcher.doctor_searcher.process(query, start, other_num, cache)
                ill_res = Xywy_Searcher.ill_searcher.process(query, start, other_num, cache)
                hospital_res = Xywy_Searcher.hospital_searcher.process(query, start, other_num, cache)
                yao_res = Xywy_Searcher.yao_searcher.process(query, start, other_num, cache)
                zx_res = Xywy_Searcher.zixun_searcher.process(query, start, other_num, cache)
                bbs_res = Searcher.searcher.process(query, start, bbs_num, cache)
                dict_ec = ec.process(query)
                query_ec = dict_ec.get('res','')
                bbs_ec = []
                if query_ec:
                    bbs_ec = Searcher.searcher.process(query_ec, start, bbs_num, cache)
                    logger.error('bbs_ec %s\t%s' % (query_ec, len(bbs_ec)))
                    if bbs_ec:
                        bbs_res['res'] = bbs_ec['res'][:5] + bbs_res['res']
                last_res = self.merge_res(bbs_res, doc_res, ill_res, hospital_res, yao_res, zx_res)
                if last_res:
                    YhMc.yhMc.add_cache(cache_key, simplejson.dumps(last_res))
            last_res["res"] =  last_res["res"][start:start+num]
            return last_res
        except:
            logger.error(traceback.format_exc())
            return {}

    def merge_res(self, bbs_res, doc_res, ill_res, hospital_res, yao_res, zx_res):
        dict_res = {}
        dict_res["seg"] = bbs_res["seg"]
        dict_res["status"] = bbs_res["status"]
        list_data = []
        bbs_data = bbs_res["res"]
        doc_data = doc_res["res"]
        ill_data = ill_res["res"]
        hospital_data = hospital_res["res"]
        yao_data = yao_res["res"]
        zixun_data = zx_res["res"]

        
        bbs_tmp = bbs_data[:6]
        bbs_data = bbs_data[6:]
        while (len(bbs_tmp) > 0):
            if doc_data:
                bbs_tmp.append(doc_data[:1])
                doc_data = doc_data[1:]
            if ill_data:
                bbs_tmp.extend(ill_data[:1])
                ill_data = ill_data[1:]
            if hospital_data:
                bbs_tmp.extend(hospital_data[:1])
                hospital_data = hospital_data[1:]
            if yao_data:
                bbs_tmp.extend(yao_data[:1])
                yao_data = yao_data[1:]
            if zixun_data:
                bbs_tmp.extend(zixun_data[:1])
                zixun_data = zixun_data[1:]
            list_data.extend(bbs_tmp)
            bbs_tmp = bbs_data[:6]
            bbs_data = bbs_data[6:]

        dict_res["res"] = list_data
        dict_res["totalnum"] = len(list_data)

        return dict_res

compose_searcher = Compose_Searcher()

if __name__ == "__main__":
    searcher = Compose_Searcher()
    searcher.process("陈龙奇")

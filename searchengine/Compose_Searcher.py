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
                last_res = self.merge_res(query, bbs_res, doc_res, ill_res, hospital_res, yao_res, zx_res)
                if last_res:
                    YhMc.yhMc.add_cache(cache_key, simplejson.dumps(last_res))
            last_res["res"] =  last_res["res"][start:start+num]
            return last_res
        except:
            logger.error(traceback.format_exc())
            return {}

    def merge_res(self, query, bbs_res, doc_res, ill_res, hospital_res, yao_res, zx_res):
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

        list_data_head = bbs_data[:6]
        list_data_other = bbs_data[6:]
        for data in doc_data, ill_data, hospital_data, yao_data, zixun_data:
            if data:
                if 'name' in data[0] and data[0]["name"] == query:
                    list_data_head.insert(0, data[0])
                else:
                    list_data_head.append(data[0])
                if len(data)>=2:
                    list_data_other.insert(11, data[1])
        for data in doc_data, ill_data, hospital_data, yao_data, zixun_data:
            list_data_other.extend(data[2:])
        dict_res["res"] = list_data_head + list_data_other
        dict_res["totalnum"] = len(list_data_head + list_data_other)
        return dict_res

compose_searcher = Compose_Searcher()

if __name__ == "__main__":
    searcher = Compose_Searcher()
    searcher.process(u'陈龙奇')

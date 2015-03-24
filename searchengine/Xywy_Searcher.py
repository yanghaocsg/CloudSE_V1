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

from Searcher import Searcher
from Xywy_Indexer import Xywy_Indexer
from Xywy_Info import Xywy_Info


import YhLog, YhTool,  YhMc
logger = logging.getLogger(__name__)

class Xywy_Searcher(Searcher):
    def __init__(self, company="", cache_prefix="", conf="", index_head="", info_head=""):
        logger.error("===================%s %s %s %s %s" %(company, cache_prefix, conf, index_head, info_head))
        Searcher.__init__(self,company=company, cache_prefix=cache_prefix)
        self.indexer = Xywy_Indexer(conf, index_head)
        self.info = Xywy_Info(conf, info_head)

    def get_cache(self, query='',  uni_query='', start=0, num=10, cache=1, st=1):
        res = YhMc.yhMc.get_cache('%s:%s' % (self.cache_prefix, uni_query))
        list_url,num_url = [], 0
        if res and cache:
            try:
                logger.error('get_cache cached [%s]' % '|'.join(list_s))
                dict_res = simplejson.loads(res)
                list_url = dict_res['list_url']
                num_url = dict_res['num_url']
            except:
                logger.error('get_cache cache_data error')
        if not list_url:
            logger.error('get_cache dached [%s]' % uni_query)
            list_idx = self.indexer.parse_query(uni_query)
            if list_idx:
                list_id = [idx[0] for idx in list_idx]
                list_url = self.info.getInfoById(list_id[:200])
                num_url = len(list_url)
                for d in list_url:
                    d['mark_red_title'], d['mark_red_brief'] = '',''
                    if 'title' in d and d['title']:
                        d['mark_red_title'] = self.mark_red(uni_query.split(), d['title'])
                    if 'brief' in d and d['brief']:
                        d['mark_red_brief'] = self.mark_red(uni_query.split(), d['brief'])
        
        buf = simplejson.dumps({'list_url':list_url, 'num_url':num_url})
        if list_url:
            YhMc.yhMc.add_cache('%s:%s' % (self.cache_prefix, uni_query), buf)        
        logger.error('get_cache list_url [%s]' % num_url)
        return list_url[start:start+num], num_url

doctor_searcher = Xywy_Searcher(company="xywy_doctor", cache_prefix="cache", conf="./conf/xywy_se.conf", index_head="doctor_index", info_head="doctor_info")
ill_searcher = Xywy_Searcher(company="xywy_ill", cache_prefix="cache", conf="./conf/xywy_se.conf", index_head="ill_index", info_head="ill_info")
hospital_searcher = Xywy_Searcher(company="xywy_hospital", cache_prefix="cache", conf="./conf/xywy_se.conf", index_head="hospital_index", info_head="hospital_info")
yao_searcher = Xywy_Searcher(company="xywy_yao", cache_prefix="cache", conf="./conf/xywy_se.conf", index_head="yao_index", info_head="yao_info")
zixun_searcher = Xywy_Searcher(company="xywy_zixun", cache_prefix="cache", conf="./conf/xywy_se.conf", index_head="zixun_index", info_head="zixun_info")

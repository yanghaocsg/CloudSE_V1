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
from Xywy_Blacklist import Xywy_Blacklist


import YhLog, YhTool,  YhMc
logger = logging.getLogger(__name__)

class Xywy_Searcher(Searcher):
    def __init__(self, company="", cache_prefix="", conf="", index_head="", info_head="", prefix_black_tableid=""):
        logger.error("===================%s %s %s %s %s" %(company, cache_prefix, conf, index_head, info_head))
        Searcher.__init__(self,company=company, cache_prefix=cache_prefix)
        self.indexer = Xywy_Indexer(conf, index_head)
        self.info = Xywy_Info(conf, info_head)
        self.company = company
        self.blacklist = Xywy_Blacklist(prefix_black_tableid)

    def get_cache(self, query='',  uni_query='', start=0, num=10, cache=1, st=1):
        res = YhMc.yhMc.get_cache('%s:%s' % (self.cache_prefix, uni_query))
        list_url,num_url = [], 0
        list_last = []
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
            if self.company == "xywy_doctor" or self.company == "xywy_hospital":
                list_idx = self.indexer.parse_full_query(query)
            if self.company == "xywy_hospital" and not list_idx:
                list_idx = self.indexer.parse_full_query(query + u"医院")
            else:
                list_idx = self.indexer.parse_query(uni_query)
            if list_idx:
                list_id = [idx[0] for idx in list_idx]
                list_url = self.info.getInfoById(list_id[:200])
                list_table_id = list()
                for item in list_url:
                    list_table_id.append((item["cat"], item["id"]))
                list_black = self.blacklist.get_table_id(list_table_id)
                for i, d in enumerate(list_url):
                    if list_black[i]:
                        continue
                    list_last.append(d)
                    d['mark_red_title'], d['mark_red_brief'] = '',''
                    if 'title' in d and d['title']:
                        d['mark_red_title'] = self.mark_red(uni_query.split(), d['title'])
                    if 'brief' in d and d['brief']:
                        d['mark_red_brief'] = self.mark_red(uni_query.split(), d['brief'])
                num_url = len(list_url)
        
        buf = simplejson.dumps({'list_url':list_last, 'num_url':num_url})
        if list_last:
            YhMc.yhMc.add_cache('%s:%s' % (self.cache_prefix, uni_query), buf)        
        logger.error('get_cache list_url [%s][%s][%s]' % (num_url, start, start+num))
        return list_last[start:start+num], num_url

doctor_searcher = Xywy_Searcher(company="xywy_doctor", cache_prefix="cache", conf="./conf/xywy_se.conf", index_head="doctor_index", info_head="doctor_info", prefix_black_tableid="table_id_%s_%s")
ill_searcher = Xywy_Searcher(company="xywy_ill", cache_prefix="cache", conf="./conf/xywy_se.conf", index_head="ill_index", info_head="ill_info", prefix_black_tableid="table_id_%s_%s")
hospital_searcher = Xywy_Searcher(company="xywy_hospital", cache_prefix="cache", conf="./conf/xywy_se.conf", index_head="hospital_index", info_head="hospital_info", prefix_black_tableid="table_id_%s_%s")
yao_searcher = Xywy_Searcher(company="xywy_yao", cache_prefix="cache", conf="./conf/xywy_se.conf", index_head="yao_index", info_head="yao_info", prefix_black_tableid="table_id_%s_%s")
zixun_searcher = Xywy_Searcher(company="xywy_zixun", cache_prefix="cache", conf="./conf/xywy_se.conf", index_head="zixun_index", info_head="zixun_info", prefix_black_tableid="table_id_%s_%s")

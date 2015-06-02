#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, datetime, os, simplejson, subprocess
from collections import defaultdict
from unipath import Path
import cPickle,ConfigParser
import lz4,csv
import glob
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import tornado.gen, tornado.web

sys.path.append('/data/CloudSE/AdWords/')
sys.path.append('/data/CloudSE/Suggest')
import YhLog,  YhTool, YhChineseNorm
from YhPinyin import yhpinyin
from ErrorCorrection import ec
from AdDarts import adDarts
from SuggestSearcher import suggestSearcher
logger = logging.getLogger(__file__)



class RelatedSearch:
    def __init__(self, conf='./conf/ec.conf', head='rs'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.config = ConfigParser.ConfigParser({'ifn_pic':'', 'ofn_pic':''})
        self.config.read(Path(self.cwd, conf))

    def process(self, query=u'我服用利培酮已经2个月了，我想问一下我是否可以出去工作，'):
        #entity mining
        list_darts = adDarts.run(query, filter=0)
        list_entity,list_fuzzy, list_rs = [],[],[]
        if list_darts:
            for la in list_darts:
                list_entity.extend([l for l in suggestSearcher.get_data(la)])
        if len(list_entity)<10:
            list_fuzzy = self.rs_fuzzy(query[:10])
        set_rs = set()
        for l in list_entity+list_fuzzy:
            q = l['query']
            if len(q)>=2 and q not in set_rs:
                list_rs.append(q)
                set_rs.add(q)
        
        logger.error('rs %s %s' % (query, '|'.join(list_rs[:10])))
        if list_rs:
            dict_res= {'res':list_rs[:10], 'status':0}
        else:
            dict_res= {'res':[], 'status':-1}
        return dict_res
        
    def rs_fuzzy(self, query=''):
        list_rs = []
        for i in range(len(query)-3):
            list_trigram = suggestSearcher.get_data(query[i:i+3])
            list_rs.extend(list_trigram)
        if list_rs:
            logger.error('rs_fuzzy trigram %s %s' % (query, '|'.join([q['query'] for q in list_rs])))
        if len(list_rs)<=10:
            for i in range(len(query)-2):
                list_bigram = suggestSearcher.get_data(query[i:i+2])
                list_rs.extend(list_bigram)
        if list_rs:
            logger.error('rs_fuzzy bigram %s %s' % (query, '|'.join([q['query'] for q in list_rs])))        
        if len(list_rs)<=10:
            for i in range(min(len(query)-1,10)):
                list_bigram = suggestSearcher.get_data(query[i:i+1])
                list_rs.extend(list_bigram)
        if list_rs:
            logger.error('rs_fuzzy unigram %s %s' % (query, '|'.join([q['query'] for q in list_rs])))        
        logger.error('rs_fuzzy [%s][%s]' % (query, len(list_rs)))
        return list_rs
    
    
        

class Rs_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'] )
            query, start, num,cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('query[%s]\tstart[%s]\tnum[%s]\tcache[%s]\tpid[%s]' % (query, start, num, cache, os.getpid()))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(simplejson.dumps(rs.process(query)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            logger.error('request_time %s [%s]' %(self.request.uri, self.request.request_time()))

rs = RelatedSearch()

def run():
    rs.process()
    rs.process(u'冠心病')
    rs.process(u'心藏病')
    rs.process(u'利培')
    
if __name__=='__main__':
    run()
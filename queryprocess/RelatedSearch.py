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

sys.path.append('../searchengine')
import YhLog,  YhTool, YhChineseNorm
from YhPinyin import yhpinyin
import Query
from ErrorCorrection import ec

logger = logging.getLogger(__file__)



class RelatedSearch:
    def __init__(self, conf='./conf/ec.conf', head='rs'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.config = ConfigParser.ConfigParser({'ifn_pic':'', 'ofn_pic':''})
        self.config.read(Path(self.cwd, conf))
        #src file
        self.ifn_pic = self.config.get(head, 'ifn_pic')
        self.ofn_pic = self.config.get(head, 'ofn_pic')
        self.ofn_fuzzy_pic = self.config.get(head, 'ofn_fuzzy_pic')
        self.dict_rs =  {}
        self.dict_fuzzy = {}
        self.load()
        
    def load(self):
        try:
            self.dict_rs = cPickle.load(open(Path(self.cwd, self.ofn_pic)))
            self.dict_fuzzy = cPickle.load(open(Path(self.cwd, self.ofn_fuzzy_pic)))
            logger.error('len rs %s %s' % (len(self.dict_rs), len(self.dict_fuzzy)))
        except:
            logger.error('load error %s' % traceback.format_exc())
    
    
    def rs(self, query=u'xinzangbin'):
        list_buf = Query.yhTrieSeg.seg(query)
        logger.error('list_buf %s\t%s' % (query, '|'.join(list_buf)))
        set_res = set()
        for b in sorted(list_buf, key=lambda x:len(x), reverse=True):
            if len(b)>=2 and b in self.dict_rs:
                set_res |= set(self.dict_rs[b])
                if len(set_res)>10:
                    break
        list_res = sorted(set_res, key=lambda x:len(x))
        logger.error('rs %s %s' % (query, '|'.join(list_res[:10])))
        return list_res
        
    def rs_fuzzy(self, list_buf=[]):
        set_fuzzy = set()
        for b in list_buf:
            bool_chinese = 0
            for q in b:
                if YhChineseNorm.is_chinese(q):
                    bool_chinese = 1
            if len(b)>=2 and bool_chinese: 
                set_fuzzy = set(self.dict_fuzzy[b[:2]]) | set(self.dict_fuzzy[b[2:]])
        list_fuzzy = sorted(set_fuzzy, key=lambda x:len(x))
        if len(list_fuzzy)<10:
            set_fuzzy_more = self.dict_fuzzy[list_buf[0]]
            list_fuzzy += sorted(set_fuzzy_more,key=lambda x:len(x))
        logger.error('rs_fuzzy %s %s' % ('|'.join(list_buf), '|'.join(list_fuzzy[:10])))
        return list_fuzzy[:10]
    
    def process(self, query=u'心藏病'):
        if not isinstance(query, unicode):
            query = unicode(query, 'utf8', 'ignore')
        list_rs, list_ec, list_fuzzy= [], [], []
        list_rs = self.rs(query)
        if len(list_rs)<10:
            query_ec = ''
            try:
                ec_res = ec.process(query)
                query_ec = ec_res['res']
            except:
                logger.error('%s\t%s' % (query, traceback.format_exc()))
            if query_ec:
                list_ec = self.rs(query_ec)
        if len(list_rs)<10:
            list_seg = Query.yhTrieSeg.seg(query)
            if list_seg:
                list_fuzzy = self.rs_fuzzy(list_seg)
        list_tmp = list_rs + list_ec[:3] + list_fuzzy[:3] + list_ec[3:] + list_fuzzy[3:]
        list_res = []
        for rs in list_tmp:
            if rs not in list_res and len(list_res)<10:
                list_res.append(rs)
        logger.error('process [%s] [%s]' % (query, '|'.join(list_res)))
        return {'res':list_res, 'status':0}
        
    #build process
    def build(self, test=1):
        dict_entity = cPickle.load(open(Path(self.cwd, self.ifn_pic)))
        dict_rs = defaultdict(set)
        dict_fuzzy = defaultdict(set)
        for q in dict_entity:
            for i in range(len(q)+1):
                for j in range(i):
                    sub_q = q[j:i]
                    if sub_q in dict_entity:
                        dict_rs[sub_q].add(q)
            for i in range(len(q)):
                for sub_q_len in [2,3]:
                    sub_q = q[i:i+sub_q_len]
                    if len(sub_q)>=2:
                        dict_fuzzy[sub_q].add(q)
        cPickle.dump(dict_rs, open(Path(self.cwd, self.ofn_pic), 'w+'))
        cPickle.dump(dict_fuzzy, open(Path(self.cwd, self.ofn_fuzzy_pic), 'w+'))
        
        if test:
            self.validate(dict_rs)
            self.validate(dict_fuzzy)
            
    def validate(self, dict_query={}, len=10):
        for i, k in enumerate(dict_query): 
            if i > 10: break
            logger.error('validate %s\t%s' % (k, '|'.join(['%s' % s for s in dict_query[k]])))
        

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
    rs.build()
    rs.load()
    rs.process()
    rs.process(u'冠心病')
    rs.process(u'心藏病')
    rs.process('xinzangbing')
    
if __name__=='__main__':
    run()
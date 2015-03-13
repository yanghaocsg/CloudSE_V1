#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson, datetime
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


#self module
import YhLog, YhTool,  YhMc
import Info, Indexer, Query
from Redis_zero import redis_zero

logger = logging.getLogger(__name__)
cwd = Path(__file__).absolute().ancestor(1)



class Searcher(object):
    def __init__(self, company='xywy', db='tag', cache_prefix='cache'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.company = company
        self.cache_prefix = '%s:%s' % (cache_prefix, self.company)
    
    def process(self, query='', start=0, num=10, cache=1, mark=1, st=1):
        try:
            before = datetime.datetime.now()
            if isinstance(query, str):
                query = unicode(query, 'utf8', 'ignore')
            uni_query = Query.query.run(query)
            list_url, num_url = [], 0
            if uni_query:
                #logger.error('process st %s' % st) 
                list_url, num_url = self.get_cache(query, uni_query, start, num, cache, st)
            else:
                raise
            dict_res = {'seg': Query.yhTrieSeg.seg(query) , 'res':list_url, 'status':0, 'totalnum':num_url}
            end = datetime.datetime.now()
            logger.error('query %s list_url %s num_url %s time %s' % (query, len(list_url), num_url, end-before))
            return dict_res
        except:
            dict_res={'status':2, 'errlog':traceback.format_exc()}
            logger.error(traceback.format_exc())
            return dict_res
            
    def mark_red(self, list_s=[], buf=''):
        res_buf = buf
        for s in list_s[:3]:
            res_buf = re.sub(s, 'mr_begin%smr_end' % s, res_buf)
        return res_buf
        
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
            list_idx = Indexer.indexer.parse_query(uni_query)
            if list_idx:
                list_id = [idx[0] for idx in list_idx]
                list_url = Info.Info().getInfoById(list_id[:200])
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
    
        
searcher = Searcher()
class Search_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'] )
            query, start, num,cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('query[%s]\tstart[%s]\tnum[%s]\tcache[%s]\tpid[%s]' % (query, start, num, cache, os.getpid()))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(simplejson.dumps(searcher.process(query, start, num, cache)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            logger.error('request_time %s [%s]' %(self.request.uri, self.request.request_time()))
                    
if __name__=='__main__':
    Searcher().process(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), cache=0)
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

import YhLog, YhTool
import Xywy_Searcher
import Compose_Searcher
sys.path.append('../queryprocess')
from ErrorCorrection import ec
from RelatedSearch import rs

logger = logging.getLogger(__name__)


class Doc_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'])
            query, start, num, cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('query[%s]\tstart[%s]\tnum[%s]\tcache[%s]\tpid[%s]' % (query, start, num, cache, os.getpid()))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(simplejson.dumps(Xywy_Searcher.doctor_searcher.process(query, start, num, cache)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            logger.error('request_time %s [%s]' %(self.request.uri, self.request.request_time()))
            

class Ill_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'])
            query, start, num, cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('query[%s]\tstart[%s]\tnum[%s]\tcache[%s]\tpid[%s]' % (query, start, num, cache, os.getpid()))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(simplejson.dumps(Xywy_Searcher.ill_searcher.process(query, start, num, cache)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            logger.error('request_time %s [%s]' %(self.request.uri, self.request.request_time()))
            

class Hospital_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'])
            query, start, num, cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('query[%s]\tstart[%s]\tnum[%s]\tcache[%s]\tpid[%s]' % (query, start, num, cache, os.getpid()))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(simplejson.dumps(Xywy_Searcher.hospital_searcher.process(query, start, num, cache)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            logger.error('request_time %s [%s]' %(self.request.uri, self.request.request_time()))



class Yao_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'])
            query, start, num, cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('query[%s]\tstart[%s]\tnum[%s]\tcache[%s]\tpid[%s]' % (query, start, num, cache, os.getpid()))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(simplejson.dumps(Xywy_Searcher.yao_searcher.process(query, start, num, cache)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            logger.error('request_time %s [%s]' %(self.request.uri, self.request.request_time()))

class Zixun_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'])
            query, start, num, cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('query[%s]\tstart[%s]\tnum[%s]\tcache[%s]\tpid[%s]' % (query, start, num, cache, os.getpid()))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(simplejson.dumps(Xywy_Searcher.zixun_searcher.process(query, start, num, cache)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            logger.error('request_time %s [%s]' %(self.request.uri, self.request.request_time()))

class Compose_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'])
            query, start, num, cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('query[%s]\tstart[%s]\tnum[%s]\tcache[%s]\tpid[%s]' % (query, start, num, cache, os.getpid()))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            dict_res = Compose_Searcher.compose_searcher.process(query, start, num, cache)
            dict_ec = ec.process(query)
            dict_rs = rs.process(query)
            dict_res.update({'ec':dict_ec.get('res',''), 'rs':dict_rs.get('res',[])})
            self.write(simplejson.dumps(dict_res))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            logger.error('request_time %s [%s]' %(self.request.uri, self.request.request_time()))

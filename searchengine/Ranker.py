#!/usr/bin/env python
#coding: utf8
import sys, re, logging, redis,traceback, time, os, simplejson, datetime
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle, lz4
import logging, urllib
from eventlet.green import urllib2
from eventlet.timeout import Timeout
#self module
import YhLog
logger = logging.getLogger(__file__)
url = 'http://127.0.0.1:8889/se?req=%s'
url_reload = 'http://127.0.0.1:8889/reload'
def get_res(name='', query=[]):
    dict_res = {}
    if name and query:
        try:
            with Timeout(0.05, False):
                dict_req = {'dict':name, 'query':['%s'%q for q in query]}
                str_req = simplejson.dumps(dict_req)
                url_req = url  % (urllib.quote_plus(str_req))
                res = urllib2.urlopen(url_req).read()
                dict_res = simplejson.loads(res)
        except:
            logger.error('name %s query %s error %s' % (name, query, traceback.format_exc()))
    logger.error('%s\t%s\t%s' % (name, query, dict_res))
    return dict_res

class Ranker:
    def __init__(self):
        self.cwd = Path(__file__).absolute().ancestor(1)
        
    def getRank(self, name='unigram_rank', list_id=[]):
        dict_res = get_res(name, list_id)
        list_res = []
        if dict_res:
            list_res = sorted(dict_res.iteritems(), key=lambda x: (x[1],int(x[0])), reverse=True)
        for id in list_id:
            if id not in dict_res:
                list_res.append((id, 0))
        return list_res
    def reload(self):
        with Timeout(0.05, False):
            urllib2.urlopen(url_reload).read()
            
if __name__ == '__main__':
    logger.error(get_res('keyword', [u'感冒', u'abc', u'二炮手']))
    logger.error(Ranker().getRank(list_id=['9437190', '8388615', '电视', '6291465']))
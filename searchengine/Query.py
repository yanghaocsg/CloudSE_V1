#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
import string
from unipath import Path
import cPickle, lz4


#self module
sys.path.append('../YhHadoop')
import YhLog, YhTool, YhChineseNorm
import YhTrieSeg

logger = logging.getLogger(__name__)
cwd = Path(__file__).absolute().ancestor(1)
yhTrieSeg = YhTrieSeg.YhTrieSeg(fn_domain=[Path(cwd, './txt/tag.txt')], fn_pic=Path(cwd, './txt/trieseg.pic'))

class Query(object):
    def __init__(self, fn_stop='./txt/stoplist.txt', pic_stop='./txt/stoplist.pic'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.fn_stop = fn_stop
        self.pic_stop = pic_stop
        self.set_stop = set()
        self.pickle()
        
    def pickle(self, pic_stop=''):
        try:
            if pic_stop:
                self.set_stop = cPickle.load(open(Path(self,cwd, pic_stop)))
            else:
                self.set_stop = cPickle.load(open(Path(self.cwd, self.pic_stop)))
        except:
            logger.error('pickle error %s' % traceback.format_exc())
            self.load()
        logger.error('pickle dict_all %s' % len(self.set_stop))
        
    def load(self):
        self.set_stop = set()
        for l in open(Path(self.cwd, self.fn_stop)):
            l = unicode(l.strip(), 'utf8', 'ignore')
            if not l: continue
            self.set_stop.add(l.lower())
        cPickle.dump(self.set_stop, open(Path(self.cwd, self.pic_stop), 'w+'))
        logger.error('load dict_all %s' % len(self.set_stop))
            
    def run(self, query):
        list_res =[]
        try:
            list_s = yhTrieSeg.seg(query)
            list_res =[s for s in list_s if s not in self.set_stop]
            logger.error('run %s\t%s\t%s' % (query, ','.join(list_s), ','.join(list_res)))
        except:
            logger.error('run_error %s\t%s' % (query, traceback.format_exc()))
        if not list_res:
            list_res.append(query)
        return '\t'.join(list_res)
query = Query()
if __name__=='__main__':
    query.run(u'我不开心怎么办')
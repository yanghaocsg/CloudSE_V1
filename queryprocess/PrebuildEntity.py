#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, datetime, os, simplejson, subprocess
from collections import defaultdict
from unipath import Path
import cPickle,ConfigParser
import lz4,csv
import glob

import YhLog,  YhTool
logger = logging.getLogger(__file__)

cwd = Path(__file__).absolute().ancestor(1) 
step = 100000
def prebuildEntity(list_ifn=[('ill', '../data/pic/ill.pic'), ('doctor', '../data/pic/doc.pic'), ('jck', '../data/pic/jck.pic'), ('zzk', '../data/pic/zzk.pic'), \
    ('hospital', '../data/pic/hospital.pic'), ('yao', '../data/pic/yao.pic')], list_field=['name', 'samename', 'smallname'], ofn_pic='./pic/entity_%s.pic', test=1):
    for (tag, lf) in list_ifn:
        dict_entity = {}
        for f in glob.glob('%s*' % Path(cwd, lf)):
            dict_data = cPickle.load(open(f))
            for key, data in dict_data.iteritems():
                for field in list_field:
                    if field in data and data[field]:
                        buf = data[field]
                        if not isinstance(buf, unicode):
                            buf = unicode(buf, 'utf8', 'ignore')
                        list_query = re.split(r'[,，\s]', buf)
                        
                        for query in list_query: 
                            if u'沈阳市中西' == query: logger.error('error data %s' % buf)
                                
                            if not isinstance(query, unicode):
                                query = unicode(query, 'utf8', 'ignore')
                            if not isinstance(query, unicode) or not query: continue
                            dict_entity[query] = {'category':tag, 'value':query, 'field':field}
            logger.error('file %s len %s' % (f, len(dict_entity)))
        cPickle.dump(dict_entity, open(Path(cwd, ofn_pic % tag), 'w+'))
        logger.error('dump file %s  len %s' % (ofn_pic % tag, len(dict_entity)))
        if test:
            validate(dict_entity)
        
def validate(dict_entity={}, len=10):
    for i, k in enumerate(dict_entity):
        logger.error('test %s\t%s' % (k, dict_entity[k]))
        if i > len:
            break

def run():
    prebuildEntity()
if __name__=='__main__':
    run()
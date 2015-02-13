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

class DataException(RuntimeError):
    def __init__(self, arg='Data Error'):
        self.args = arg
        
class Data:
    def __init__(self, conf='./conf/data.conf', head='info'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.config = ConfigParser.ConfigParser({'id':'', 'src':'','filetype':'csv', 'head':'','pic':'','id':''})
        self.config.read(Path(self.cwd, conf))
        #src file
        self.src = self.config.get(head, 'src')
        #src filetype
        self.filetype=self.config.get(head, 'filetype')
        #src fields
        self.head=[p.strip() for p in self.config.get(head, 'head').split(',')]
        #pickle output
        self.pic = self.config.get(head, 'pic')
        #id field
        self.id = self.config.get(head, 'id')
        if not self.src or not self.pic:
            raise DataException(['conf %s head %s' % (conf, head)])
        self.dict_data = {}
        
    def prebuild(self):
        self.dict_data = {}
        list_file = glob.glob('%s*' % self.src)
        idx_num = 0 
        dict_pic = {}
        for f in list_file:
            if not self.head:
                for l in csv.DictReader(open(f)):
                    try:
                        dict_tmp = dict([(unicode(key, 'utf8'), unicode(value, 'utf-8')) for (key, value) in l.iteritems() if key and isinstance(value, str)])
                        id = 0
                        if self.id:
                            id = int(dict_tmp[self.id])
                        else:
                            id = int(dict_tmp['id'])
                        dict_tmp['id']=id
                        self.dict_data[id] = dict_tmp
                        dict_pic[id] = dict_tmp
                    except:
                        logger.error('prebuild error %s [%s] %s' % (f, l, traceback.format_exc()))
                    if len(dict_pic)>=100000:
                        ofn = Path(self.cwd, '%s.%s' % (self.pic, idx_num))
                        cPickle.dump(dict_pic, open(ofn, 'w+'))
                        logger.error('data dump %s %s' % (ofn, len(dict_pic)))
                        idx_num += 1
                        dict_pic = {}
                    if len(self.dict_data)>=500000:
                        break
            else:
                for l in csv.reader(open(f)):
                    try:
                        dict_tmp = {}
                        for i, h in enumerate(self.head):
                            dict_tmp[h] = unicode(l[i], 'utf8', 'ignore')
                        id = 0
                        if self.id:
                            id = int(dict_tmp[self.id])
                        else:
                            id = int(dict_tmp['id'])
                        dict_tmp['id']=id
                        self.dict_data[id] = dict_tmp
                        dict_pic[id] = dict_tmp
                    except:
                        logger.error('prebuild error %s [%s] %s' % (f, l, traceback.format_exc()))
                    if len(dict_pic)>=100000:
                        ofn = Path(self.cwd, '%s.%s' % (self.pic, idx_num))
                        cPickle.dump(dict_pic, open(ofn, 'w+'))
                        logger.error('data dump %s %s' % (ofn, len(dict_pic)))
                        idx_num += 1
                        dict_pic = {}
                    if len(self.dict_data)>=500000:
                        break
            logger.error('rebuild dict_data %s %s' % (f, len(self.dict_data)))
        
        
    def load_data(self, rebuild=0):
        if rebuild:
            self.prebuild()
        else:
            try:
                list_file = glob.glob('%s*' % self.pic)
                logger.error('pic %s list_file %s' % (self.pic,  list_file))
                for f in list_file:
                    dict_tmp = cPickle.load(open(f))
                    self.dict_data.update(cPickle.load(open(f)))
                    logger.error('load_data list_file %s\t%s' % (f, len(self.dict_data)))
                if not self.dict_data:
                    raise DataException(['load_data dict_data is zero'])
            except:
                logger.error('load_data %s' % traceback.format_exc())
                self.prebuild()
        logger.error('load_data %s' % len(self.dict_data))
        
    def test(self):
        num_k = 0
        for k in self.dict_data:
            logger.error('test %s [%s]' % (k, self.dict_data[k]))
            num_k += 1
            if num_k > 3: break
            
class DataRank:
    def __init__(self, conf='./conf/data.conf', head='rank'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.config = ConfigParser.ConfigParser({'rank':'', 'pic':'', 'src':''})
        self.config.read(Path(self.cwd, conf))
        #src file
        logger.error(self.config.get(head, 'rank'))
        self.dict_rank_factor = simplejson.loads(self.config.get(head, 'rank'))
        logger.error('rank_factor %s' % self.dict_rank_factor)
        #pickle output
        self.pic = self.config.get(head, 'pic')
        self.src = self.config.get(head, 'src')
        self.dict_data = {}
        
    def load_data(self):
        list_file = glob.glob('%s*' % self.src)
        idx_num = 0
        for f in list_file:
            dict_tmp = cPickle.load(open(f))
            dict_rank = {}
            for d in dict_tmp:
                rank = 0
                for k, v in self.dict_rank_factor.iteritems():
                    try:
                        rank += int(dict_tmp[d][k]) * int(v)
                    except:
                        logger.error('rank error %s %s %s %s' % (d, k, v, traceback.format_exc()))
                if rank:
                    self.dict_data[d] = rank
                    dict_rank['%s' % d] = rank
                    if len(dict_rank)<=3:
                        logger.debug('rank load_data test %s\t%s' % (d, rank))
            ofn = Path('%s.%s' % (self.pic, idx_num))
            cPickle.dump(dict_rank, open(ofn, 'w+'))
            logger.error('rank dump  %s %s' % (ofn, len(dict_rank)))
            idx_num += 1
        logger.error('rank load_data %s' % len(self.dict_data))
            
    def test(self):
        num_k = 0
        for k in self.dict_data:
            logger.error('test %s [%s]' % (k, self.dict_data[k]))
            num_k += 1
            if num_k > 3: break
            
data = Data()
data.load_data()
datarank = DataRank()
datarank.load_data()

if __name__=='__main__':
    data.test()
    datarank.test()
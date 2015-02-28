#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
import string
from unipath import Path
import cPickle
from bitstring import BitArray
import lz4, glob, ConfigParser

#local module
import Segmenter

#self module
import YhLog
from Redis_zero import redis_zero
import YhBitset
import Ranker

logger = logging.getLogger(__file__)

def fun_idx(ifn='', field=[], seg='', ofn=''):
    dict_data = cPickle.load(open(ifn))
    dict_kw = defaultdict(set)
    for k,v in dict_data.iteritems():
        str_kw = '\t'.join([v[f] for f in field])
        list_kw = seg(str_kw)
        for kw in list_kw:
            dict_kw[kw].add(k)
    '''
    for k in dict_kw.keys()[:3]:
        logger.error('%s\t%s' % (k, dict_kw[k]))
    '''
    cPickle.dump(dict_kw, open(ofn, 'w+'))
    logger.error('idx ifn %s ofn %s dict_kw %s pid %s' %  (ifn, ofn, len(dict_kw), os.getpid()))
    return os.getpid()
    
class Indexer(object):
    def __init__(self, conf='./conf/se.conf', head='index'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.config = ConfigParser.ConfigParser({'src':'','one':'', 'two':'','three':'', 'pic':''})
        self.config.read(Path(self.cwd, conf))
        #src file
        self.src = self.config.get(head, 'src')
        self.one = self.config.get(head, 'one')
        self.two = self.config.get(head, 'two')
        self.three = self.config.get(head, 'three')
        self.one_idx = {}
        self.two_idx = {}
        self.three_idx = {}
        if self.one:
            self.one_idx = simplejson.loads(self.one)
        if self.two:
            self.two_idx = simplejson.loads(self.two)
        if self.three:
            self.three_idx = simplejson.loads(self.three)
        self.pic =self.config.get(head,'pic')
        
    def build_idx(self):
        pool = Pool(10)
        idx_num = 0
        list_res = []
        for idx in [self.one_idx, self.two_idx, self.three_idx]:
            if not idx: continue
            field, seg, prefix = idx['field'], idx['seg'], idx['prefix']
            segmenter = Segmenter.get_seg(id=int(seg))
            for f in glob.glob('%s*' % self.src):
                ofn='%s.%s' % (self.pic, idx_num)
                res = pool.apply_async(fun_idx, args=(f, field, segmenter, ofn))
                list_res.append(res)
                idx_num += 1
            for r in list_res:
                logger.error('idx %s finished' % r.get(600))
            self.merge_idx(ifn=self.pic, prefix=prefix)
            
    def merge_idx(self, ifn='', prefix=''):
        dict_idx = defaultdict(set)
        for f in glob.glob('%s*' % ifn):
            dict_part = cPickle.load(open(f))
            for k,v in dict_part.iteritems():
                dict_idx[k] |= v
            logger.error('merge idx ifn %s len %s' % (f, len(dict_idx)))
        pipeline_zero = redis_zero.pipeline()
        len_num = 1
        for k,v in dict_idx.iteritems():
            yhBitSet = YhBitset.YhBitset()
            yhBitSet.set_list(v)
            pipeline_zero.hset(prefix, k, yhBitSet.tobytes())
            if len_num % 10000 == 0:
                pipeline_zero.execute()
                logger.error('merge_idx %s' % len_num)
            len_num += 1
        pipeline_zero.execute()
        logger.error('merge_idx ifn %s prefix %s len_num %s ' % (ifn, prefix, len_num))
            
    def parse_query(self, uni_query='', ):
        list_id = []
        list_id.extend(self.match(uni_query, self.one_idx))
        if len(list_id)<10 and self.two:
            list_id.extend(self.match(uni_query, self.two_idx))
        if len(list_id)<10 and self.three:
            list_id.extend(self.match(uni_query, self.three_idx))
        if len(list_id)<10:
            list_id.extend(self.match_fuzzy(uni_query,self.one_idx))
        return list_id
        
        
    def match(self, uni_query='', idx={}):
        if not idx: return []
        segmenter = Segmenter.get_seg(id=idx['seg'])
        prefix = idx['prefix']
        list_s = segmenter(uni_query)
        list_bitset, list_docid, len_list_docid = [], [], 0
        for s in list_s:
            str_s = redis_zero.hget(prefix, s)
            if str_s:
                yhBitset = YhBitset.YhBitset()
                yhBitset.frombytes(str_s)
                list_bitset.append(yhBitset)
                #logger.error('%s matched len %s' % (s, len(yhBitset.search())))
            else:
                logger.error('%s filtered' % s)
        bitset = YhBitset.YhBitset()
        if list_bitset:
            bitset = list_bitset[0]
            for bs in list_bitset[1:]:
                test = bitset.anditem(bs)
                if len(test.search(200, 1))<10:
                    break
                bitset = test
        list_docid = bitset.search(200, 1)
        list_docid = Ranker.Ranker().getRank(name='unigram_rank', list_id=list_docid)
        return list_docid[:200]
    
    def match_fuzzy(self, uni_query='', idx={}):
        if not idx: return []
        segmenter = Segmenter.get_seg(id=idx['seg'])
        prefix = idx['prefix']
        list_s = segmenter(uni_query)
        list_bitset, list_docid, len_list_docid = [], [], 0
        for s in list_s:
            str_s = str_s = redis_zero.hget(prefix, s)
            if str_s:
                yhBitset = YhBitset.YhBitset()
                yhBitset.frombytes(str_s)
                list_docid = yhBitset.search(200, 1)
                break
            else:
                logger.error('%s filtered' % s)
        #logger.error('match_title seg %s  len %s ids %s' % ('|'.join(list_s), len(list_docid), list_docid[:3]))
        list_docid = Ranker.Ranker().getRank(name='unigram_rank', list_id=list_docid)
        return list_docid[:200]
    
    
    
        
indexer = Indexer()

if __name__=='__main__':
    #indexer.build_idx() 
    logger.error(indexer.parse_query(u'感冒'))
    
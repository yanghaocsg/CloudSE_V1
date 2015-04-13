#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
import string
from unipath import Path
import cPickle
from bitstring import BitArray
from Redis_zero import redis_zero
import lz4, glob, ConfigParser
import YhBitset

from Indexer import Indexer
import Ranker

logger = logging.getLogger(__file__)

class Xywy_Indexer(Indexer):
    def __init__(self, conf="./conf/xywy_se.conf", head="doctor_index"):
        Indexer.__init__(self, conf, head)

    def full_match(self, uni_query="", idx={}):
        if not idx: return []
        prefix = idx['prefix']
        list_s = [uni_query]
        logger.error("================lists:%s"  %("|".join(list_s)))
        list_bitset, list_docid, len_list_docid = [], [], 0
        for s in list_s:
            str_s = redis_zero.hget(prefix, s)
            if str_s:
                yhBitset = YhBitset.YhBitset()
                yhBitset.frombytes(str_s)
                list_bitset.append(yhBitset)
                logger.error('%s matched len %s' % (s, yhBitset.length()))
            else:
                logger.error('%s filtered' % s)
        bitset = YhBitset.YhBitset()
        if list_bitset:
            bitset = list_bitset[0]
            for bs in list_bitset[1:]:
                test = bitset.anditem(bs)
                if test.length()<=0:
                    break
                bitset = test
        list_docid = bitset.search(200, 1)
        list_docid = ['%s' % id for id in list_docid]
        list_docid = Ranker.Ranker().getRank(name='unigram_rank', list_id=list_docid)
        logger.error('match [%s] [%s] [%s]' % (uni_query, list_s, list_docid))
        return list_docid[:200]
        
    def parse_query(self, uni_query='', ):
        list_id = []
        
        list_id.extend(self.match(re.sub('\s+','',uni_query), self.one_idx))
        list_id.extend(self.match(uni_query, self.one_idx))
        if len(list_id)<10 and self.two:
            list_id.extend(self.match(uni_query, self.two_idx))
        if len(list_id)<10 and self.three:
            list_id.extend(self.match(uni_query, self.three_idx))
        return self.unify_id(uni_query, list_id)

    def parse_full_query(self, uni_query='', ):
        list_id = []
        
        list_id.extend(self.full_match(re.sub('\s+','',uni_query), self.one_idx))
        list_id.extend(self.full_match(uni_query, self.one_idx))
        return self.unify_id(uni_query, list_id)

if __name__ == "__main__":
    doctor_index = Xywy_Indexer("./conf/xywy_se.conf", "doctor_index")
    doctor_index.build_idx()

    ill_index = Xywy_Indexer("./conf/xywy_se.conf", "ill_index")
    ill_index.build_idx()

    hospital_index = Xywy_Indexer("./conf/xywy_se.conf", "hospital_index")
    hospital_index.build_idx()

    yao_index = Xywy_Indexer("./conf/xywy_se.conf", "yao_index")
    yao_index.build_idx()

    zixun_index = Xywy_Indexer("./conf/xywy_se.conf", "zixun_index")
    zixun_index.build_idx()

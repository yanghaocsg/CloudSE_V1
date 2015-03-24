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

from Indexer import Indexer

logger = logging.getLogger(__file__)

class Xywy_Indexer(Indexer):
    def __init__(self, conf="./conf/xywy_se.conf", head="doctor_index"):
        Indexer.__init__(self, conf, head)

    def parse_query(self, uni_query='', ):
        list_id = []
        
        list_id.extend(self.match(re.sub('\s+','',uni_query), self.one_idx))
        list_id.extend(self.match(uni_query, self.one_idx))
        if len(list_id)<10 and self.two:
            list_id.extend(self.match(uni_query, self.two_idx))
        if len(list_id)<10 and self.three:
            list_id.extend(self.match(uni_query, self.three_idx))
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

#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle, lz4

#self module
sys.path.append('../data')
import Data
import Info, Indexer, Ranker
import Restart

logger = logging.getLogger(__file__)

def process_data():
    Data.run()
    logger.error('data process finished')
    
    #info
    Info.info.saveInfo()
    logger.error('info process finished')

    #indexer
    Indexer.indexer.build_idx()
    logger.error('indexer process finished')
    
    #restart
    Restart.restart(name='SearchServer')
    logger.error('restart process finished')
    
if __name__=='__main__':
    process_data()
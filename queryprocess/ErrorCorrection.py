#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, datetime, os, simplejson, subprocess
from collections import defaultdict
from unipath import Path
import cPickle,ConfigParser
import lz4,csv
import glob

import YhLog,  YhTool
from YhPinyin import yhpinyin
logger = logging.getLogger(__file__)



class ErrorCorrection:
    def __init__(self, conf='./conf/ec.conf', head='ec'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.config = ConfigParser.ConfigParser({'ifn_pic':'', 'ofn_chinese_ed':'','ofn_pinyin':'', 'ofn_pinyin_ed':'', 'ec_len':3, 'max_keyword':10})
        self.config.read(Path(self.cwd, conf))
        #src file
        self.ifn_pic = self.config.get(head, 'ifn_pic')
        self.ofn_entity = self.config.get(head, 'ofn_entity')
        self.ofn_chinese_ed = self.config.get(head, 'ofn_chinese_ed')
        self.ofn_pinyin=self.config.get(head, 'ofn_pinyin')
        self.ofn_pinyin_ed=self.config.get(head, 'ofn_pinyin_ed')
        self.ec_len = self.config.get(head, 'ec_len')
        self.max_keyword = self.config.get(head, 'max_keyword')
        self.dict_data = defaultdict(set)
        self.dict_ec_pinyin= {}
        self.dict_entity =  {}
        logger.error('ec_len %s' % self.ec_len)
        
    def load(self):
        self.dict_ec_pinyin = cPickle.load(open(Path(self.cwd, self.ofn_pinyin)))
        self.dict_entity = cPickle.load(open(Path(self.cwd, self.ofn_entity)))
        logger.error('len pinyin %s len entity %s ' % (len(self.dict_ec_pinyin), len(self.dict_entity)))
    
    
    
    def ec(self, query=u'xinzangbin'):
        str_res, status = '', 0
        if query in self.dict_entity:
            status = -1
        else:
            if query in self.dict_ec_pinyin:
                str_res = self.dict_ec_pinyin[query]
            else:
                set_pinyin = yhpinyin.line2py(query)
                logger.error('errorcorrect [%s] [%s]' % (query, '|'.join(set_pinyin)))
                for s in set_pinyin:
                    #logger.error('errorcorrect pinyin %s\t%s' % (query, s))
                    if s in self.dict_ec_pinyin:
                        str_res = self.dict_ec_pinyin[s]
                        break
                if str_res and not self.is_ed_equal_1(query, str_res):
                    str_res = ''
            if str_res:
                status = 1
        logger.error('ec [%s] [%s] [%s]' % (query, str_res, status))
        return str_res, status
    
    def is_ed_equal_1(self, query=u'包卜', query_ec=u'鲍勃'):
        #logger.error('%s %s' % (query, query_ec))
        if len(query) != len(query_ec):
            return False
        flag = False
        for i in xrange(len(query)):
            #logger.error('[%s][%s]' % (query[:i]+query[i+1:], query_ec[:i]+query_ec[i+1:]))
            if query[:i]+query[i+1:] == query_ec[:i]+query_ec[i+1:]:
                flag = True
                break
        return flag
        
    def ec_leftmost(self, query=u'吊丝男士第一季全集'):
        str_res, status = '', 0
        if len(query) < self.ec_len:
            return '', status
        else:
            bool_chinese = 0
            for q in query:
                if YhChineseNorm.is_chinese(q):
                    bool_chinese = 1
                    break
            if bool_chinese: #only ec left part, for chinese
                end_pos = min(len(query), self.max_keyword)
                for i in xrange(end_pos, self.min_keyword-1, -1):
                    sub_query = query[:i]
                    #logger.error('sub_query %s' % sub_query)
                    str_res, status = self.ec(sub_query)
                    if str_res:
                        #logger.error('errorcorrect_leftmost org[%s] sub_query[%s] sub_ec[%s]' % (query, sub_query, str_res))
                        str_res = str_res + query[i:]
                        status = 2
                        break
                    else:
                        status = 0
        logger.error('ec leftmost [%s] [%s] [%s]' % (query, str_res, status))
        return str_res, status    
    
    '''
    return str_res, status
    status:
        -1, noneed ec
        0, need ec
        1, pinyin ec
        2, pinyin left most ec
    '''
    def process(self, query='心藏病'):
        str_res, status = '', 0
        str_res, status = self.ec(query)
        if status == 0:
            str_res, status = self.ec_leftmost(query)
        logger.error('ec process [%s] [%s] [%s]' % (query, str_res, status))
        return str_res, status
        
    #build process
    def build(self):
        dict_ifn_pic = simplejson.loads(self.ifn_pic)
        dict_query = {}
        for tag,file in dict_ifn_pic.iteritems():
            for f in glob.glob(Path(self.cwd, file)):
                dict_data = cPickle.load(open(f))
                dict_query.update(dict_data)
        cPickle.dump(dict_query, open(Path(self.cwd, self.ofn_entity), 'w+'))
        self.build_pinyin(dict_query, self.ofn_pinyin)
        
    def build_pinyin(self, dict_query={}, ofn_pic='',test=1):
        dict_pinyin = {} #defaultdict(set)
        for k, v in dict_query.iteritems():
            list_py_k = yhpinyin.line2py_list(k)
            py = ''.join(list_py_k)
            dict_pinyin[py] = k
            for i in range(len(list_py_k)):
                py_i = list_py_k[i]
                if py_i[0] in ['l', 'n']:
                    py = ''.join(list_py_k[:i] + ['l'] + [py_i[1:]]+list_py_k[i+1:])
                    py = ''.join(list_py_k[:i] + ['n'] + [py_i[1:]]+list_py_k[i+1:])
                    dict_pinyin[py] = k
                if py_i[-2:] in ['an','in', 'on']:
                    py = ''.join(list_py_k[:i] + [py_i] +['g']+list_py_k[i+1:])
                    dict_pinyin[py]= k 
                if py_i[-3:] in ['ang', 'ing', 'ong']:
                    py = ''.join(list_py_k[:i] + [py_i[:-1]]+list_py_k[i+1:])
                    dict_pinyin[py] = k
        cPickle.dump(dict_pinyin, open(Path(self.cwd, ofn_pic), 'w+'))
        logger.error('build_pinyin file %s len %s' % (ofn_pic, len(dict_pinyin)))
        if test:
            self.validate(dict_pinyin)
        return dict_pinyin
        
    
    def validate(self, dict_query={}, len=10):
        for i, k in enumerate(dict_query): 
            if i > 10: break
            logger.error('validate %s\t%s' % (k, '|'.join(['%s' % s for s in dict_query[k]])))

ec = ErrorCorrection()
def run():
    ec.build()
    ec.load()
    ec.process()
    ec.process('xinzangbin')
    ec.process('xinzangbing')
    
if __name__=='__main__':
    run()
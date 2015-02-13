#!/usr/bin/env python
#coding:utf8

import cProfile, pstats, StringIO
import Searcher

pr = cProfile.Profile()
pr.enable()
Searcher.searcher.process(query=u'儿童尿尿有点黄，而且痛', cache=0)
pr.disable()
s = StringIO.StringIO()
sortby = 'cumulative'
ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
ps.print_stats()
print s.getvalue()
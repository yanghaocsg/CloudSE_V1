import sys, traceback, imp, os, subprocess, logging, datetime, re, signal
from unipath import Path
import multiprocessing


import YhLog
logger = logging.getLogger(__file__)
def process(name='SearchServer'):
    p = multiprocessing.Process(target=restart, kwargs={'py':name})
    p.daemon=True
    p.start()
    
def restart(py='SearchServer'):
    str_cmd = 'ps -ef | grep %s' % py
    logger.error('restart cmd %s' % str_cmd)
    p = subprocess.Popen(str_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    list_pid = []
    for buf in p[0].split('\n'):
        logger.error('restart %s' % buf)
        try:
            user, pid,_ = re.split('\s+', buf, 2)
            pid = int(pid)
            logger.error('kill %s' % pid)
            os.kill(pid, signal.SIGKILL)
        except:
            logger.error('%s\t%s' % (buf, traceback.format_exc()))

def start():
    cwd = Path(__file__).absolute().ancestor(1)
    subprocess.check_call('cd %s;  python SearchServer.py' % cwd, shell=True)
    
if __name__=='__main__':
    restart()
    #start()
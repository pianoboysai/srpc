#!/usr/bin/env python
# -*- coding: utf-8 -*
# author: SAI
import os,sys,time,traceback
from optparse import OptionParser
import multiprocessing
from srpc import *

class SRPCServerExample(object):
    def ask(self, name):
        return 'hello %s!' % name

def simulate_server(bindport):
    print 'start server, port:', bindport
    s = SRPCServer('127.0.0.1', bindport)
    s.register_instance(SRPCServerExample())
    s.server_forever()

def simulate_threadpool_server(bindport):
    print 'start threadpool server, port:', bindport
    s = SRPCThreadPoolServer('127.0.0.1', bindport)
    s.register_instance(SRPCServerExample())
    s.server_forever()

def simulate_gevent_server(bindport):
    import gevent
    import gevent.monkey
    gevent.monkey.patch_all()
    class SRPCGEventServer(SRPCServer):
        def exec_rpc_thread(self, sock, address):
            gevent.spawn(self.reply_rpc,sock,address)
    print 'start gevent server, port:', bindport
    s = SRPCGEventServer('127.0.0.1', bindport)
    s.register_instance(SRPCServerExample())
    s.server_forever()


def simulate_client(bindport, mode,number):
    c = SRPCClient('127.0.0.1', bindport, connect_mode=mode)
    #print c.listMethods()
    for i in xrange(number):
        ret = c.ask('jack')
    return
def main():
    parser = OptionParser()
    parser.add_option('-n', '--number', help='client request number', type=int, default=10000, metavar=10000)
    parser.add_option('-m', '--mode', help='connection mode, 0:long | 1:short', type=int, default=0, metavar=0)
    parser.add_option('-p', '--port', help='testing port', type=int, default=3000, metavar=3000)
    parser.add_option('-d', '--debug', help='', action='store_true')
    (opts, args) = parser.parse_args()
    print opts
    mode = opts.mode
    if mode == 0:
        mode = SRPC_LONG_CONNECT
    else:
        mode = SRPC_SHORT_CONNECT
    number=opts.number
    g_bindport=opts.port

    serverlist=[simulate_server,simulate_threadpool_server,simulate_gevent_server]
    for i,serverfunc in enumerate(serverlist):
        print 'test',serverfunc.__name__
        p = multiprocessing.Process(target=serverfunc, args=(g_bindport+i,))
        p.start()
        time.sleep(1)
        t = time.time()
        simulate_client(g_bindport+i,mode,number)
        print 'qps:',number/(time.time() - t)
        p.terminate()
        print '-'*20
    print 'over'

if __name__=='__main__':
    main()

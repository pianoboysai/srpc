#!/usr/bin/env python
# -*- coding: utf-8 -*
# author: SAI

import os,sys,time,threading
import srpc

'''
server code:
===================================
class SRPCExampleClass(object):
    def __init__(self, age):
        super(SRPCExampleClass, self).__init__()
        self.age=age
        
    def say(self, name):
        return self.age
    
    def eat(self, obj):
        print 'i eat', obj.age
        return 'apple'
        
def srpc_testfunc(name):
    return 'hello boy %s' % name

if __name__=='__main__':
    server=SRPCServer('127.0.0.1',22000)
    server.register_function(srpc_testfunc)
    server.register_instance(SRPCExampleClass(123))
    server.start_server()
'''


if __name__=='__main__':
    print time.ctime()
    client=srpc.SRPCClient('127.0.0.1',22000)
    for t in xrange(10000):
        print client.srpc_testfunc('sai')
    print client.srpc_ThreadNumber()
    print time.ctime()

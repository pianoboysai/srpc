#!/usr/bin/env python
# -*- coding: utf-8 -*
# author: SAI
import gevent
import gevent.monkey
gevent.monkey.patch_all()
import gevent.queue

import sys,os,time
from srpc import *

class SRPCGEventServer(SRPCServer):
    def exec_rpc_thread(self, sock, address):
        job=gevent.spawn(self.reply_rpc,sock,address)

if __name__=='__main__':
    s=SRPCGEventServer('127.0.0.1',22000)
    s.register_instance(SRPCServerExample())
    s.start_server() 



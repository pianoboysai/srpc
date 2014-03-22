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

g_bindport=find_idle_port()
def main():
    print 'start gevent server',g_bindport
    s=SRPCGEventServer('127.0.0.1',g_bindport)
    s.register_instance(SRPCServerExample())
    s.server_forever() 

if __name__=='__main__':
    main() 




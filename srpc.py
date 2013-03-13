#!/usr/bin/env python
# -*- coding: utf-8 -*
# author: SAI

import os, sys, time, socket
import threading
import cPickle, Queue, struct
def SRPCSerializationData(data):
    return cPickle.dumps(data,0)
def SRPCDeserializationData(data):
    return cPickle.loads(data)

class SRPCFuncnameRepetitiveError(Exception):
    def __init__(self, funcname):
        super(SRPCFuncnameRepetitiveError, self).__init__()
        self.description="funcname '%s' has been registered!dont' be repetitive" % funcname
    def __str__(self):
        return repr(self.description)

SRPC_SOCKET_BUFFER_SIZE = 4096
def SRPCSend(sock, data):
    if not data:
        data=''
    sendsize=len(data)
    sock.sendall(struct.pack('I',sendsize))
    while sendsize:
        d=data[0:SRPC_SOCKET_BUFFER_SIZE]
        sock.sendall(d)
        data=data[SRPC_SOCKET_BUFFER_SIZE:]
        if data=='':
            break

#return data
def SRPCRecv(sock):
    ret=sock.recv(4)
    if ret=='':
        return ''
    ret=struct.unpack('I', ret)
    size=int(ret[0])
    data=''
    while len(data)<size:
        ret=sock.recv(SRPC_SOCKET_BUFFER_SIZE)
        if ret=='':
            break
        data+=ret
    return  data
    

class SRPCClientCall(object):
    def __init__(self,sock,funcname):
        super(SRPCClientCall, self).__init__()
        self.__funcname=funcname
        self.__sock=sock

    def callfunc(self,*args, **kwargs):
        data=SRPCSerializationData([self.__funcname,args,kwargs])
        SRPCSend(self.__sock,data)
        data=SRPCRecv(self.__sock)
        if data:
            data=SRPCDeserializationData(data)
            if data[0]==0:
                data=data[1]
            else:
                raise Exception(data[1])
        return data

class SRPCClient(object):
    def __init__(self,ip,port,socket_timeout=None):
        super(SRPCClient, self).__init__()
        self.sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.sock.connect((ip,port))
        print self.sock.getsockname() 
        if socket_timeout:
            self.sock.settimeout(socket_timeout)
        
    def __getattr__(self, name):
        return SRPCClientCall(self.sock,name).callfunc
    
    def __del__(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except:
            print sys.exc_info()
            pass
        try:
            self.sock.close()
        except:
            print sys.exc_info()
            pass
    
class SRPCThread(threading.Thread):
    def __init__(self,queue, method_dict):
        super(SRPCThread, self).__init__()
        self.method_dict=method_dict
        self.queue=queue
    
    def run(self):
        while 1:
            sock, address=self.queue.get()
            print threading.currentThread().ident,'fetch connect from', address
            try:
                self.callfunc(sock, self.method_dict)
            except:
                print sys.exc_info()
                
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
            print threading.currentThread().ident, 'complete'
            
    def callfunc(self, sock, method_dict):
        while 1:
            data=SRPCRecv(sock)
            if not data:
                break
            funcname,args,kwargs=SRPCDeserializationData(data)
            func=method_dict.get(funcname)
            if func:
                try:
                    ret=(0, apply(func, args,kwargs))
                except:
                    ret=(-1, str(sys.exc_info()))
            else:
                ret = (-1,"funcname '%s' has not been registered!" % funcname)
            
            data=SRPCSerializationData(ret)
            SRPCSend(sock,data)
        

class SRPCServer(object):
    def __init__(self,ip,port,initthreadnumber=4):
        super(SRPCServer, self).__init__()
        self.conn=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.address=(ip,port)
        self.__method_dict={}
        self.__method_dict['srpc_ListMethods']=self.srpc_ListMethods
        self.__method_dict['srpc_ThreadNumber']=self.srpc_ThreadNumber
                
        self.__threads=[]
        self.__queue=Queue.Queue()
        self.create_rpc_thread(initthreadnumber)
    
    def __del__(self):
        self.conn.shutdown(socket.SHUT_RDWR)
        self.conn.close()
        self.conn=None
    
    def create_rpc_thread(self, threadnumber):
        for i in xrange(threadnumber):
            t=SRPCThread(self.__queue, self.__method_dict)
            t.setDaemon(True)
            t.start()
            self.__threads.append(t)
        print 'creart rpc thread complete,current number', len(self.__threads)
        
    def register_function(self,func):
        funcname=func.func_name
        if self.__method_dict.get(funcname):
            raise SRPCFuncnameRepetitiveError(funcname)
        self.__method_dict[funcname]=func 

    def register_instance(self,instance):
        l=dir(instance)
        for i in l:
            if not i.startswith('_'):
                funcname=i
                func=getattr(instance,funcname)
                if self.__method_dict.get(funcname):
                    raise SRPCFuncnameRepetitiveError
                self.__method_dict[funcname]=func
    
    def srpc_ListMethods(self):
        return self.__method_dict.keys()
    
    def srpc_ThreadNumber(self):
        return len(self.__threads)
            
    #return (0, 'success') or (-1,'fail reason')
    def start_server(self):
        self.conn.bind(self.address)
        self.conn.listen(20)
        print 'start server at', self.address
        while 1:
            
            try:
                sock,address=self.conn.accept()
                size=self.__queue.qsize()
                if size:
                    self.create_rpc_thread(size*4)
                #for test self.__threads[0].callfunc(sock, self.__method_dict)
                self.__queue.put((sock, address))
            except:
                print sys.exc_info()
                sys.exit(0)
            finally:
                pass


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


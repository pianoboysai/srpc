#!/usr/bin/env python
# -*- coding: utf-8 -*
# author: SAI
import os, sys, time, socket, traceback
import threading
import cPickle, Queue, struct
from optparse import OptionParser

SRPC_FUNCCALL_TYPE=0
SRPC_FUNCRETURN_TYPE=1
SRPC_REQUEST_COMPLETION=2
SRPC_REQUEST_ERROR=-1
SRPC_SOCKET_BUFFER_SIZE = 4096

class SRPCFuncnameRepetitiveError(Exception):
    def __init__(self, funcname):
        super(SRPCFuncnameRepetitiveError, self).__init__()
        self.description="funcname '%s' has been registered!dont' be repetitive" % funcname
    def __str__(self):
        return repr(self.description)

def SRPCSend(sock, data):
    t=time.time()
    if not data:
        data=''
    sendsize=len(data)
    sock.sendall(struct.pack('I',sendsize)+data)#这里不要分开写成两次sendall，不然第2次还得等第一次发完才继续
    return

#return data
def SRPCRecv(sock):
    ret=sock.recv(4)
    if ret=='':
        return ''
    ret=struct.unpack('I', ret)
    size=int(ret[0])
    data=''
    while len(data)<size:
        ret=sock.recv(size-len(data))
        if ret=='':
            break
        data+=ret
    return  data

class SRPCProtocol(object):
    def __init__(self):
        super(SRPCProtocol, self).__init__() 
    def serialization(self, data):
        return cPickle.dumps(data,0)
    def deserialization(self, data):
        return cPickle.loads(data) 

SRPC_SHORT_CONNECT=0
SRPC_LONG_CONNECT=1
class SRPCClientCall(object):
    def __init__(self,funcname, client):
        super(SRPCClientCall, self).__init__()
        self.client=client
        self.funcname=funcname
    def callfunc(self, *args, **kwargs):
        return self.client.callfunc(self.funcname, *args, **kwargs)

class SRPCClient(object):
    def __init__(self,ip,port,socket_timeout=None, connect_mode=SRPC_SHORT_CONNECT, protocol=SRPCProtocol()):
        super(SRPCClient, self).__init__()
        self.protocol=protocol
        self.connect_mode=connect_mode
        #print sock.getsockname() 
        self.ipport=(ip,port)
        self.socket_timeout=socket_timeout
        if self.connect_mode==SRPC_LONG_CONNECT:
            self.__collect()
    
    def __del__(self):
        if self.connect_mode==SRPC_LONG_CONNECT:
            self.__close(self.sock)

    def __getattr__(self, funcname):
        return SRPCClientCall(funcname, self).callfunc
    
    def __collect(self):
        sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect(self.ipport)
        sock.settimeout(self.socket_timeout)
        self.sock=sock
        
    def __close(self, sock):
        try:
            if sock:
                data=self.protocol.serialization([SRPC_REQUEST_COMPLETION])
                SRPCSend(sock,data)
                sock.shutdown(socket.SHUT_RDWR)
        except:
            traceback.print_exc()
            
        try:
            if sock:
                sock.close()
        except:
            traceback.print_exc()
            
    def callfunc(self, funcname, *args, **kwargs):
        ret=None
        try:
            if self.connect_mode==SRPC_SHORT_CONNECT:
                self.__collect()
            
            data=self.protocol.serialization([SRPC_FUNCCALL_TYPE, funcname, args, kwargs])
            SRPCSend(self.sock,data)
            data=SRPCRecv(self.sock)
            data=self.protocol.deserialization(data)
            if data[0]==SRPC_FUNCRETURN_TYPE:
                ret=data[1]
            elif data[0]==SRPC_REQUEST_ERROR:
                print data[1]
            
            if self.connect_mode==SRPC_SHORT_CONNECT:
                self.__close(self.sock)
                
        except Exception, err:
            traceback.print_exc()
            raise err

        return ret

class SRPCServer(object):
    def __init__(self, ip, port, timeout=None,protocol=SRPCProtocol()):
        super(SRPCServer, self).__init__()
        self.conn=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.address=(ip,port)
        self.protocol=protocol
        self.method_dict={}
        self.method_dict['listMethods']=self.listMethods
        self.timeout=timeout
        
    def __del__(self):
        self.conn.shutdown(socket.SHUT_RDWR)
        self.conn.close()
        self.conn=None

    def reply_rpc(self, sock, addr):
        try:
            while 1:
                data=SRPCRecv(sock)
                data=self.protocol.deserialization(data)
                #print data[0]
                if data[0]==SRPC_FUNCCALL_TYPE:
                    funcname,args,kwargs=data[1:]
                    func=self.method_dict.get(funcname)
                    if callable(func):
                        try:
                            ret=(SRPC_FUNCRETURN_TYPE, apply(func, args,kwargs))
                        except:
                            ret=(SRPC_REQUEST_ERROR, str(sys.exc_info()))
                    else:
                        ret=(SRPC_REQUEST_ERROR, "'%s' is not func!" % funcname)
                        
                elif data[0]==SRPC_REQUEST_COMPLETION:
                    break
                else:
                    ret = (SRPC_REQUEST_ERROR, "funcname '%s' has not been registered!" % funcname)
                
                data=self.protocol.serialization(ret)
                SRPCSend(sock,data)

            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        except Exception, err:
            traceback.print_exc()
            print err
            raise err
            #traceback.print_exc()

    def register_function(self,func):
        funcname=func.func_name
        if self.method_dict.get(funcname):
            raise SRPCFuncnameRepetitiveError(funcname)
        self.method_dict[funcname]=func 

    def register_instance(self,instance):
        l=dir(instance)
        for i in l:
            if not i.startswith('_'):
                funcname=i
                func=getattr(instance,funcname)
                if self.method_dict.get(funcname):
                    raise SRPCFuncnameRepetitiveError
                self.method_dict[funcname]=func
  
    def exec_rpc_thread(self, sock, addr):
        t=threading.Thread(target=self.reply_rpc, args=(sock, addr))
        t.setDaemon(True)
        t.start()
        
    def listMethods(self):
        return self.method_dict.keys()

    #return (0, 'success') or (-1,'fail reason')
    def server_forever(self):
        self.conn.bind(self.address)
        self.conn.listen(20)
        print 'start server at', self.address
        while 1:
            try:
                sock,address=self.conn.accept()
                sock.settimeout(self.timeout)
                self.exec_rpc_thread(sock, address)
            except:
                print sys.exc_info()
                sys.exit(0)
            finally:
                pass

class SRRPThreadPoolServer(SRPCServer):
    def __init__(self, ip, port, timeout=None,protocol=SRPCProtocol(), init_threadnumber=30):
        super(SRRPThreadPoolServer, self).__init__( ip, port, timeout,protocol)
        self.init_threadnumber=init_threadnumber
        self.queue=Queue.Queue()
        self.threadlist=[]
        for i in xrange(init_threadnumber):
            t=threading.Thread(target=self.rpcthread)
            t.setDaemon(True)
            t.start()
            self.threadlist.append(t)

    def rpcthread(self):
        print threading.currentThread().ident,'thread create' 
        while 1:
            sock, addr=self.queue.get()
            self.reply_rpc(sock, addr)  
            
    def exec_rpc_thread(self, sock, addr):
        self.queue.put((sock, addr))

class SRPCServerExample(object):
    def __init__(self):
        super(SRPCServerExample, self).__init__()
        self.name='sai'
        
    def ask(self, name):
        return 'hello %s! myname is %s' % (name, self.name)

class Tester(object):
    def __init__(self):
        super(Tester, self).__init__()
        
    def simulate_server(self):
        s=SRPCServer('127.0.0.1',22000)
        s.register_instance(SRPCServerExample())
        s.server_forever()
    
    def simulate_threadpool_server(self):
        s=SRRPThreadPoolServer('127.0.0.1',22000)
        s.register_instance(SRPCServerExample())
        s.server_forever()
    
    def simulate_client(self):
        def client_call():
            try:
                c=SRPCClient('127.0.0.1',22000, 2)
                print c.listMethods()
                start_time=time.time()
                for i in xrange(100):
                    ret=c.ask('jack')
                    print ret
                print time.time()-start_time
                c=SRPCClient('127.0.0.1',22000, 2, connect_mode=SRPC_LONG_CONNECT)
                ret=c.ask('sai')
                print ret
            except Exception, err:
                print err
        
        for i in xrange(10):
            #client_call()
            t=threading.Thread(target=client_call)
            #t.setDaemon(True)
            t.start()
        print 'over'
        
def srpc_testfunc(name):
    return 'hello boy %s' % name

if __name__=='__main__':
    parser=OptionParser(version="0.1")
    parser.add_option('-c','--client',help='simulate client', action='store_true')
    parser.add_option('-s','--server',help='simulate server', action='store_true')
    parser.add_option('-t','--threadpoolserver',help='simulate threadpool server', action='store_true')
    (opts,args)=parser.parse_args()
    if opts.server:
        a=Tester()
        a.simulate_server()
    elif opts.threadpoolserver:
        a=Tester()
        a.simulate_threadpool_server()
    elif opts.client:
        a=Tester()
        a.simulate_client()
    else:
        parser.print_help()



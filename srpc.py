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

class SRPCClientCall(object):
    def __init__(self,client,protocol, funcname):
        super(SRPCClientCall, self).__init__()
        self.__funcname=funcname
        self.__client=client
        self.__protocol=protocol

    def callfunc(self, *args, **kwargs):
        ret=None
        try:
            data=self.__protocol.serialization([SRPC_FUNCCALL_TYPE, self.__funcname, args, kwargs])
            SRPCSend(self.__client,data)
            data=SRPCRecv(self.__client)
            data=self.__protocol.deserialization(data)
            if data[0]==SRPC_FUNCRETURN_TYPE:
                ret=data[1]
            elif data[0]==SRPC_REQUEST_ERROR:
                print data[1]
            
        except Exception, err:
            traceback.print_exc()
            print err

        return ret
        
class SRPCProtocol(object):
    def __init__(self):
        super(SRPCProtocol, self).__init__() 
    def serialization(self, data):
        return cPickle.dumps(data,0)
    def deserialization(self, data):
        return cPickle.loads(data) 

class SRPCClient(object):
    def __init__(self,ip,port,socket_timeout=None, protocol=SRPCProtocol()):
        super(SRPCClient, self).__init__()
        self.__protocol=protocol
        self.__sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.__sock.connect((ip,port))
        self.__sock.settimeout(socket_timeout)
        #print self.__sock.getsockname() 
    
    def __getattr__(self, funcname):
        return SRPCClientCall(self.__sock,self.__protocol, funcname).callfunc
    
    def __del__(self):
        try:
            data=self.__protocol.serialization([SRPC_REQUEST_COMPLETION])
            SRPCSend(self.__sock,data)
            self.__sock.shutdown(socket.SHUT_RDWR)
        except:
            traceback.print_exc()
        try:
            self.__sock.close()
        except:
            traceback.print_exc()

class SRPCServer(object):
    def __init__(self, ip, port, timeout=None,protocol=SRPCProtocol()):
        super(SRPCServer, self).__init__()
        self.conn=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.address=(ip,port)
        self.__protocol=protocol
        self.__method_dict={}
        self.__method_dict['srpc_ListMethods']=self.srpc_ListMethods
        self.__timeout=timeout
        
    def __del__(self):
        self.conn.shutdown(socket.SHUT_RDWR)
        self.conn.close()
        self.conn=None

    def reply_rpc(self, sock, addr):
        try:
            while 1:
                data=SRPCRecv(sock)
                data=self.__protocol.deserialization(data)
                #print data[0]
                if data[0]==SRPC_FUNCCALL_TYPE:
                    funcname,args,kwargs=data[1:]
                    func=self.__method_dict.get(funcname)
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
                
                data=self.__protocol.serialization(ret)
                SRPCSend(sock,data)

        except:
            traceback.print_exc()

        #print 'complete rpc from',addr
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except:
            traceback.print_exc()
        try:
            sock.close()
        except:
            traceback.print_exc()

            
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

            
    def exec_rpc_thread(self, sock, addr):
        t=threading.Thread(target=self.reply_rpc, args=(sock, addr))
        t.setDaemon(True)
        t.start()
        
    def srpc_ListMethods(self):
        return self.__method_dict.keys()

    #return (0, 'success') or (-1,'fail reason')
    def start_server(self):
        self.conn.bind(self.address)
        self.conn.listen(20)
        print 'start server at', self.address
        while 1:
            try:
                sock,address=self.conn.accept()
                sock.settimeout(self.__timeout)
                self.exec_rpc_thread(sock, address)
            except:
                print sys.exc_info()
                sys.exit(0)
            finally:
                pass

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
        s.start_server()
    
    def simulate_client(self):
        return_queue=Queue.Queue()
        def client_call():
            c=SRPCClient('127.0.0.1',22000)
            t=time.time()
            for i in xrange(100):
                ret=c.ask('jack')
            return_queue.put([ret, time.time()-t])
        
        st=time.time()
        tl=[]
        for i in xrange(1000):
            #client_call()
            #continue
            t=threading.Thread(target=client_call)
            t.start()
            tl.append(t)
            
        for i in tl:
            i.join()
        
        while 1:
            if return_queue.empty():
                break
            return_queue.get()
        print time.time()-st
            
def srpc_testfunc(name):
    return 'hello boy %s' % name

if __name__=='__main__':
    parser=OptionParser(version="0.1")
    parser.add_option('-c','--client',help='simulate client', action='store_true')
    parser.add_option('-s','--server',help='simulate server', action='store_true')
    (opts,args)=parser.parse_args()
    if opts.server:
        a=Tester()
        a.simulate_server()
    elif opts.client:
        a=Tester()
        a.simulate_client()
    else:
        parser.print_help()



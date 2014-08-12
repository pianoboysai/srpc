#!/usr/bin/env python
# -*- coding: utf-8 -*
# author: SAI
import os, sys, time, socket, traceback
import threading, json, cPickle, Queue, struct
from optparse import OptionParser

SRPC_FUNCCALL_TYPE=0
SRPC_FUNCRETURN_TYPE=1
SRPC_REQUEST_COMPLETION=2
SRPC_REQUEST_ERROR=-1
SRPC_SOCKET_BUFFER_SIZE = 4096

class SRPCException(Exception):
    pass

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
    sock.sendall(struct.pack('I',sendsize)+data)
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

class SRPCDefaultProtocol(object):
    def serialization(self, data):
        return cPickle.dumps(data,0)
    def deserialization(self, data):
        return cPickle.loads(data)

class SRPCJSONProtocol(object):
    def serialization(self, data):
        return json.dumps(data)
    def deserialization(self, data):
        s=json.loads(data)
        return s

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
    def __init__(self,ip,port,socket_timeout=None, connect_mode=SRPC_SHORT_CONNECT, protocol=SRPCDefaultProtocol()):
        super(SRPCClient, self).__init__()
        self.protocol=protocol
        self.connect_mode=connect_mode
        #print sock.getsockname() 
        self.ipport=(ip,port)
        self.socket_timeout=socket_timeout
        if self.connect_mode==SRPC_LONG_CONNECT:
            self.__connect()
    
    def __del__(self):
        if self.connect_mode==SRPC_LONG_CONNECT:
            self.__close(self.sock)

    def __getattr__(self, funcname):
        return SRPCClientCall(funcname, self).callfunc
    
    def __connect(self):
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
        if self.connect_mode==SRPC_SHORT_CONNECT:
            self.__connect()
        
        try:
            data=self.protocol.serialization([SRPC_FUNCCALL_TYPE, funcname, args, kwargs])
            SRPCSend(self.sock,data)
            data=SRPCRecv(self.sock)
            data=self.protocol.deserialization(data)
            if data[0]==SRPC_FUNCRETURN_TYPE:
                ret=data[1]
            elif data[0]==SRPC_REQUEST_ERROR:
                raise SRPCException(data[1])
            else:
                raise SRPCException('unknown')
        finally:
            if self.connect_mode==SRPC_SHORT_CONNECT:
                self.__close(self.sock)

        return ret

class SRPCServer(object):
    def __init__(self, ip, port, timeout=None,protocol=SRPCDefaultProtocol()):
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
                    ret=(SRPC_REQUEST_ERROR, "funcname '%s' has not been registered!" % funcname)
                
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
        self.conn.listen(100)
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

class SRPCThreadPoolServer(SRPCServer):
    def __init__(self, ip, port, timeout=None,protocol=SRPCDefaultProtocol(), init_threadnumber=30):
        super(SRPCThreadPoolServer, self).__init__(ip, port, timeout,protocol)
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
        
def simulate_server(bindport,threadnumber,protocol):
    if threadnumber:
        print 'start threadpool server, port:',bindport
        s=SRPCThreadPoolServer('127.0.0.1',bindport, protocol=protocol,init_threadnumber=threadnumber)
    else:
        print 'start server, port:',bindport
        s=SRPCServer('127.0.0.1',bindport,protocol=protocol)
    s.register_instance(SRPCServerExample())
    s.server_forever()
    
    
def simulate_client(bindport,number,protocol,mode,debug):
    c=SRPCClient('127.0.0.1', bindport, protocol=protocol,connect_mode=mode)
    if debug:
        print c.listMethods()

    for i in xrange(number):
        ret=c.ask('jack')
        if debug:
            print ret
        
def find_idle_port():
    import random
    while 1:
        port=random.randint(20000,60000) 
        a=os.popen('netstat -an|grep ":%d"' % port).read()
        if not a:
           return port 

g_bindport=find_idle_port()
def main():
    global g_bindport
    parser=OptionParser()
    parser.add_option('-p','--protocol',default='',help='json is optional',metavar='json')
    parser.add_option('-t','--threadnumber',help='use threadpool server, 0 is default server', type=int, default=0, metavar='0')
    parser.add_option('-c','--client',help='client number', type=int, default=1, metavar=1)
    parser.add_option('-n','--number',help='every client request number', type=int, default=100, metavar=100)
    parser.add_option('-m','--mode',help='connection mode, 0:long | 1:short', type=int, default=0, metavar=0)
    parser.add_option('-d','--debug',help='', action='store_true')
    parser.add_option('-r','--remoteport',help='only launch client to connect port', type=int, default=0, metavar=0)
    (opts,args)=parser.parse_args()
    print opts
    if opts.protocol=='json':
        protocol=SRPCJSONProtocol()
    else:
        protocol=SRPCDefaultProtocol()
    threadnumber=opts.threadnumber
    clientnumber=opts.client 
    number=opts.number
    mode=opts.mode
    remoteport=opts.remoteport

    if mode==0:
        mode=SRPC_LONG_CONNECT
    else:
        mode=SRPC_SHORT_CONNECT
    debug=opts.debug
    if not remoteport:
        th=threading.Thread(target=simulate_server,args=(g_bindport,threadnumber,protocol))
        th.setDaemon(True)
        th.start()
        time.sleep(0.1)
    else:
        g_bindport=remoteport

    t=time.time()
    l=[]
    for i in xrange(clientnumber):
        th=threading.Thread(target=simulate_client,args=(g_bindport,number,protocol,mode,debug))
        th.start()
        l.append(th)
    for th in l:
        th.join()
    
    print time.time()-t
        

if __name__=='__main__':
    main()



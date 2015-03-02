#!/usr/bin/env python
# -*- coding: utf-8 -*
# author: SAI
import os, sys, time, socket, traceback
import threading, json, cPickle, Queue, struct
from optparse import OptionParser

SRPC_FUNCCALL_TYPE = 0
SRPC_FUNCRETURN_TYPE = 1

SRPC_REQUEST_ERROR = -1
SRPC_SHORT_CONNECT = 0
SRPC_LONG_CONNECT = 1

class SRPCException(Exception):
    pass

class SRPCFuncnameRepetitiveError(SRPCException):
    def __init__(self, funcname):
        super(SRPCFuncnameRepetitiveError, self).__init__()
        self.description = "funcname '%s' has been registered!dont' be repetitive" % funcname

    def __str__(self):
        return repr(self.description)


def SRPCSend(sock, data):
    if not data:
        data = ''
    sendsize = len(data)
    sock.sendall(struct.pack('I', sendsize) + data)
    return

#return data
def SRPCRecv(sock):
    ret = sock.recv(4)
    if ret == '':
        return ''
    ret = struct.unpack('I', ret)
    size = int(ret[0])
    data = ''
    while len(data) < size:
        ret = sock.recv(size - len(data))
        if ret == '':
            break
        data += ret
    return data


class SRPCDefaultProtocol(object):
    def serialization(self, data):
        return cPickle.dumps(data, 0)

    def deserialization(self, data):
        return cPickle.loads(data)


class SRPCJSONProtocol(object):
    def serialization(self, data):
        return json.dumps(data)

    def deserialization(self, data):
        s = json.loads(data)
        return s

#needn't inherit Object for performance
class SRPCClientCall:
    def __init__(self, funcname, client):
        self.client = client
        self.funcname = funcname

    def callfunc(self, *args, **kwargs):
        return self.client.callfunc(self.funcname, *args, **kwargs)


class SRPCClient(object):
    def __init__(self, ip, port, socket_timeout=None, connect_mode=SRPC_LONG_CONNECT, protocol=SRPCDefaultProtocol()):
        super(SRPCClient, self).__init__()
        self.protocol = protocol
        self.connect_mode = connect_mode
        #print sock.getsockname() 
        self.ipport = (ip, port)
        self.sock=None
        self.socket_timeout = socket_timeout
        if self.connect_mode == SRPC_LONG_CONNECT:
            self.__connect()

    def __del__(self):
        if self.connect_mode == SRPC_LONG_CONNECT:
            self.__close()

    def __getattr__(self, funcname):
        return SRPCClientCall(funcname, self).callfunc

    def __connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.ipport)
        sock.settimeout(self.socket_timeout)
        self.sock = sock

    def __isconnected(self):
        return self.sock

    def __close(self):
        try:
            if self.sock:
                self.sock.shutdown(socket.SHUT_RDWR)
        except:
            print traceback.format_exc()
        try:
            if self.sock:
                self.sock.close()
        except:
            print traceback.format_exc()
        self.sock=None

    def callfunc(self, funcname, *args, **kwargs):
        ret = None
        exp=None
        try:
            if not self.__isconnected():
                self.__connect()

            data = self.protocol.serialization([SRPC_FUNCCALL_TYPE, funcname, args, kwargs])
            SRPCSend(self.sock, data)
            data = SRPCRecv(self.sock)
            data = self.protocol.deserialization(data)
            if data[0] == SRPC_FUNCRETURN_TYPE:
                ret = data[1]
            elif data[0] == SRPC_REQUEST_ERROR:
                exp=SRPCException(data[1])
            else:
                exp=SRPCException('unknown')

        except:
            exp=SRPCException(traceback.format_exc())
            print traceback.format_exc()
            self.__close()
        finally:
            if self.connect_mode == SRPC_SHORT_CONNECT:
                self.__close()

        if exp:
            raise exp
        return ret


class SRPCServer(object):
    def __init__(self, ip, port, timeout=None, protocol=SRPCDefaultProtocol()):
        super(SRPCServer, self).__init__()
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.address = (ip, port)
        self.protocol = protocol
        self.method_dict = {}
        self.method_dict['listMethods'] = self.listMethods
        self.timeout = timeout

    def __del__(self):
        self.conn.shutdown(socket.SHUT_RDWR)
        self.conn.close()
        self.conn = None

    def reply_rpc(self, sock, addr):
        try:
            while 1:
                data = SRPCRecv(sock)
                if not data:
                    #client disconnected
                    break
                data = self.protocol.deserialization(data)
                #print data[0]
                if data[0] == SRPC_FUNCCALL_TYPE:
                    funcname, args, kwargs = data[1:]
                    func = self.method_dict.get(funcname)
                    if callable(func):
                        try:
                            ret = (SRPC_FUNCRETURN_TYPE, apply(func, args, kwargs))
                        except:
                            ret = (SRPC_REQUEST_ERROR, str(sys.exc_info()))
                    else:
                        ret = (SRPC_REQUEST_ERROR, "'%s' is not func!" % funcname)

                else:
                    ret = (SRPC_REQUEST_ERROR, "funcname '%s' has not been registered!" % funcname)

                data = self.protocol.serialization(ret)
                SRPCSend(sock, data)

            #print 'client disconnected'
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        except Exception, err:
            print 'server error',traceback.format_exc()
            raise err


    def register_function(self, func):
        funcname = func.func_name
        if self.method_dict.get(funcname):
            raise SRPCFuncnameRepetitiveError(funcname)
        self.method_dict[funcname] = func

    def register_instance(self, instance):
        l = dir(instance)
        for i in l:
            if not i.startswith('_'):
                funcname = i
                func = getattr(instance, funcname)
                if self.method_dict.get(funcname):
                    raise SRPCFuncnameRepetitiveError
                self.method_dict[funcname] = func

    def exec_rpc_thread(self, sock, addr):
        t = threading.Thread(target=self.reply_rpc, args=(sock, addr))
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
                sock, address = self.conn.accept()
                sock.settimeout(self.timeout)
                self.exec_rpc_thread(sock, address)
            except:
                print sys.exc_info()
                sys.exit(0)
            finally:
                pass


class SRPCThreadPoolServer(SRPCServer):
    def __init__(self, ip, port, timeout=None, protocol=SRPCDefaultProtocol(), init_threadnumber=30):
        super(SRPCThreadPoolServer, self).__init__(ip, port, timeout, protocol)
        self.init_threadnumber = init_threadnumber
        self.queue = Queue.Queue()
        self.threadlist = []
        for i in xrange(init_threadnumber):
            t = threading.Thread(target=self.rpcthread)
            t.setDaemon(True)
            t.start()
            self.threadlist.append(t)

    def rpcthread(self):
        #print threading.currentThread().ident, 'thread create'
        while 1:
            sock, addr = self.queue.get()
            self.reply_rpc(sock, addr)

    def exec_rpc_thread(self, sock, addr):
        self.queue.put((sock, addr))


# -*- coding: utf-8 -*-
import six, urllib, urlparse

from socket import socket, AF_INET, SOCK_STREAM
from kazoo.client import KazooClient

from utils import encoder, parser
from bitstring import ConstBitStream, BitStream
import random, threading, inspect


class RespCode:
    def __init__(self):
        pass

    RESPONSE_WITH_EXCEPTION = 0 + 144
    RESPONSE_VALUE = 1 + 144
    RESPONSE_NULL_VALUE = 2 + 144
    RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS = 3 + 144
    RESPONSE_VALUE_WITH_ATTACHMENTS = 4 + 144
    RESPONSE_NULL_VALUE_WITH_ATTACHMENTS = 5 + 144


class Dubbo(object):
    def __init__(self, interface, version, method, args, dubbo_v="2.0.2"):
        self.dubbo_v = dubbo_v
        self.interface = interface
        self.version = version
        self.method = method
        self.args = args
        self.attachments = {"path": interface, "interface": interface, "version": version}

    def _encode(self, encoder):
        elements = []
        elements.append(six.binary_type(encoder.encode(self.dubbo_v)))
        elements.append(six.binary_type(encoder.encode(self.interface)))
        elements.append(six.binary_type(encoder.encode(self.version)))
        elements.append(six.binary_type(encoder.encode(self.method)))

        arg_type = "".join([arg_type for arg_type, _ in self.args])
        elements.append(six.binary_type(encoder.encode(arg_type)))
        for _, arg in self.args:
            elements.append(six.binary_type(encoder.encode(arg)))
        elements.append(six.binary_type(encoder.encode(self.attachments)))

        return "".join(elements)

    def _invoke(self, client):
        data = self._encode(encoder.Encoder())
        reg_header = ConstBitStream('intbe:16=-9541,intbe:8=-62,intbe:8=0,uintbe:64=0,uintbe:32=%d' % len(data))
        client.send(reg_header.bytes)
        client.send(data)

        res_header = ConstBitStream(bytes=client.recv(16)).readlist('intbe:16,intbe:8,intbe:8,uintbe:64,uintbe:32')
        length = res_header[-1]

        bytes = []
        while length > 0:
            data = client.recv(1024)
            bytes.append(data)
            length -= len(data)

        body = ConstBitStream(bytes="".join(bytes))
        resp_code = body.read('uintbe:8')
        if resp_code == RespCode.RESPONSE_NULL_VALUE or resp_code == RespCode.RESPONSE_NULL_VALUE_WITH_ATTACHMENTS:
            return None
        elif resp_code == RespCode.RESPONSE_WITH_EXCEPTION or resp_code == RespCode.RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS:
            p = parser.ParserV2(body)
            res = p.read_object()
            raise Exception(res)
        elif resp_code == RespCode.RESPONSE_VALUE or resp_code == RespCode.RESPONSE_NULL_VALUE_WITH_ATTACHMENTS:
            p = parser.ParserV2(body)
            res = p.read_object()
            return res


class DubboZK(Dubbo):
    _instance_lock = threading.Lock()

    def __init__(self, interface, hosts, version="0.0.0", dubbo_v="2.0.2", lb_mode=0):
        self.lb_mode = lb_mode  # lb_mode=0 轮训;lb_mode= 1 随机
        self._rr = -1

        self.dubbo_v = dubbo_v
        self.interface = interface
        self.version = version
        self.attachments = {"path": interface, "interface": interface, "version": version}
        # zk
        zk = KazooClient(hosts=hosts)
        zk.start()
        self.zk = zk
        # providers
        providers = zk.get_children("/dubbo/%s/providers" % interface)
        uris = [urlparse.urlparse(urllib.unquote(provider)) for provider in providers]
        self.uris = uris
        # client
        clients = []
        for uri in uris:
            client = socket(AF_INET, SOCK_STREAM)
            client.connect((uri.hostname, uri.port))
            clients.append(client)
        self.clients = clients

        # add method
        if "methods" in uris[0].path:
            query = uris[0].path
        elif "methods" in uris[0].query:
            query = uris[0].query
        else:
            print(u"no method found.")
            exit(1)
        params = urlparse.parse_qs(query)
        for method in params["methods"][0].split(","):
            def _decorator(func):
                def _(*args):
                    self.method = func
                    self.args = args[0]
                    return self.invoke(*args)

                return _

            setattr(self, method, _decorator(method))

    def __new__(cls, *args, **kwargs):
        if not hasattr(DubboZK, "_instance"):
            with DubboZK._instance_lock:
                if not hasattr(DubboZK, "_instance"):
                    DubboZK._instance = object.__new__(cls)
        return DubboZK._instance

    def invoke(self, *args):
        args_len = len(args)
        if args_len > 1:
            self.method = args[0]
            self.args = args[1]

        clients_len = len(self.clients)
        if self.lb_mode == 0:
            self._rr = (self._rr + 1) % clients_len
            return self._invoke(self.clients[self._rr])

        bound = (0, clients_len - 1)
        return self._invoke(self.clients[random.randint(bound[0], bound[1])])

    def close(self):
        for client in self.clients:
            try:
                client.close()
            except Exception, e:
                print(e)

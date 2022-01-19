# -*- coding: utf-8 -*-
from urllib.parse import urlparse, unquote, parse_qs

from socket import socket, AF_INET, SOCK_STREAM
from io import StringIO
from kazoo.client import KazooClient

from src.utils import encoder, parser, provider
from bitstring import ConstBitStream
import random, threading
from request import Request, Attachments
from response import ResponseCode


class Dubbo(object):
    __slots__ = ('dubbo_v', 'interface', 'version', 'method', 'args', 'attachments')

    def __init__(self, interface, version, method, args, attachments=None, dubbo_v="2.6.8"):
        self.dubbo_v = dubbo_v
        self.interface = interface
        self.version = version
        self.method = method
        self.args = args
        self.attachments = attachments if attachments else Attachments(
            {"path": interface, "interface": interface, 'version': version})


class DubboProvider(Request, provider.ProviderRegistryMixin):
    def __init__(self):
        pass


class DubboConsumer(Dubbo):
    _instance_lock = threading.Lock()

    def __init__(self, interface, hosts, version="0.0.0", dubbo_v="2.6.8", lb_mode=0):
        self.lb_mode = lb_mode  # lb_mode=0 轮训;lb_mode= 1 随机
        self._rr = -1

        self.dubbo_v = dubbo_v
        self.interface = interface
        self.version = version
        self.attachments = Attachments({"path": interface, "interface": interface})
        if self.version:
            self.attachments.version = self.version
        # zk
        zk = KazooClient(hosts=hosts)
        zk.start()
        self.zk = zk
        # providers
        providers = zk.get_children("/dubbo/%s/providers" % interface)
        uris = [urlparse(unquote(provider)) for provider in providers]
        self.uris = uris

        if len(uris) == 0:
            print("no service found")
            return

        # client
        clients = []
        for uri in uris:
            client = socket(AF_INET, SOCK_STREAM)
            client.connect((uri.hostname, uri.port))
            clients.append(client)
        self.clients = clients

        # add method
        query = None
        if "methods" in uris[0].path:
            query = uris[0].path
        elif "methods" in uris[0].query:
            query = uris[0].query
        else:
            print(u"no method found.")

        if query is not None:
            params = parse_qs(query)
            for method in params["methods"][0].split(","):
                def _decorator(func):
                    def _(*args):
                        self.method = func
                        self.args = args[0]
                        return self.invoke(*args)

                    return _

                setattr(self, method, _decorator(method))

    def __new__(cls, *args, **kwargs):
        if not hasattr(DubboConsumer, "_instance"):
            with DubboConsumer._instance_lock:
                if not hasattr(DubboConsumer, "_instance"):
                    DubboConsumer._instance = object.__new__(cls)
        return DubboConsumer._instance

    def invoke(self, *args):
        if len(args) > 1:
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
            except Exception as e:
                print(e)

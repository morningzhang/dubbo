# -*- coding: utf-8 -*-
import six, urllib, urlparse

from socket import socket, AF_INET, SOCK_STREAM
from kazoo.client import KazooClient

from utils import encoder, parser
from bitstring import ConstBitStream


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
        body = ConstBitStream(bytes=client.recv(res_header[-1]))
        with_attachments = body.read('uintbe:8')
        if with_attachments == 149:
            return None
        elif with_attachments == 148 or with_attachments == 145:
            p = parser.ParserV2(body)
            res = p.read_object()
            return res


class DubboZK(Dubbo):
    def __init__(self, interface, hosts, version="0.0.0", dubbo_v="2.0.2"):
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
        uri = urlparse.urlparse(urllib.unquote(providers[0]))
        # client
        client = socket(AF_INET, SOCK_STREAM)
        client.connect((uri.hostname, uri.port))
        self.client = client
        # add method
        params = urlparse.parse_qs(uri.query)
        for method in params["methods"][0].split(","):
            setattr(self, method, self.invoke)

    def invoke(self, method, args):
        self.args = args
        self.method = method

        return self._invoke(self.client)
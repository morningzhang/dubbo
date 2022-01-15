# -*- coding:utf8 -*-

import time
from socket import socket, AF_INET, SOCK_DGRAM
from urllib.parse import urlunparse, urlencode, ParseResult, quote_plus

from kazoo.client import KazooClient


class ProviderRegistryMixin(object):
    service_path = {}
    registry_zk = {}

    @property
    def timestamp(self):
        return int(time.time() * 1000)

    def local_host_ip(self):
        s = socket(AF_INET, SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        return ip

    def generate_provider_url(self, schema="dubbo", host="127.0.0.1", port="20880", sapplication=None, service=None,
                              methods=None, validate=True, defaultFilter=None, dubbo_v="2.6.8"):
        if methods is None:
            methods = []
        query_map = {
            "applicaton": sapplication,
            "bean.name": service,
            "default.service.filter": defaultFilter,
            "default.validation": validate,
            "dubbo": dubbo_v,
            "generic": False,
            "interface": service,
            "methods": ",".join(methods),
            "side": "provider",
            "timestamp": self.timestamp
        }
        query = urlencode(query_map)
        f = ParseResult(scheme=schema, netloc='%s:%s' % (host, port), path=service, params='', query=query, fragment='')
        return urlunparse(f)

    def start(self, port=20880):
        pass

    def registry(self, port, service, methods, zks):
        if zks is None:
            raise Exception("zk 地址为空")
        providers_path = "/dubbo/%s/providers" % (service)
        ip = self.local_host_ip()
        provider_url = self.generate_provider_url(host=ip, service=service, port=port, methods=methods)
        self.service_path[service] = "%s/%s" % (providers_path, quote_plus(provider_url))
        value = str(ip).encode("utf8")

        zk = KazooClient(hosts=zks)
        self.registry_zk[service] = zk
        zk.start()
        # zk.ensure_path(providers_path+"/")
        try:
            zk.delete(self.service_path[service])
        except Exception as e:
            pass
        finally:
            zk.create(self.service_path[service], value, ephemeral=True, sequence=False, makepath=True)

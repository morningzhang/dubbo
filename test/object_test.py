# -*- coding: utf-8 -*-
from dubbo import Dubbo
from socket import socket, AF_INET, SOCK_STREAM

if __name__ == '__main__':

    client = socket(AF_INET, SOCK_STREAM)
    client.connect(("127.0.0.1", 20880))
    try:
        d = Dubbo("org.apache.dubbo.demo.DemoService", "0.0.0", "swich",
                  (("Lorg/apache/dubbo/demo/Person;", {"name": u"某某", "address": [u"余杭"], "age": 15}),))
        res = d._invoke(client)
        print(res)
    finally:
        client.close()

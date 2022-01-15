# -*- coding: utf-8 -*-
from socket import socket, AF_INET, SOCK_STREAM

from pydubbo import Dubbo

if __name__ == '__main__':

    client = socket(AF_INET, SOCK_STREAM)
    client.connect(("127.0.0.1", 20880))
    try:
        interface = "org.apache.dubbo.demo.DemoService"
        d = Dubbo(interface, "0.0.0", "echo", (("Ljava/util/List;", [100, 200]), ("Ljava/lang/String;", u"某某")))
        res = d._invoke(client)
        print(res)
    finally:
        client.close()

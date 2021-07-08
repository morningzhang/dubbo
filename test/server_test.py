# -*- coding: utf-8 -*-

import socket
from bitstring import ConstBitStream

from pydubbo.pydubbo import DubboZKProvider

HEADER_SIZE = 16
BACKLOG_SIZE = 128


def start_server(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0",port)) 
    server.listen(128)
    
    provider = DubboZKProvider()
    while True:
        con,address = server.accept()
        while True:
            try:
                # 接受套接字的大小，怎么发就怎么收
                headers_bytes = con.recv(HEADER_SIZE)
                headers = ConstBitStream(bytes=headers_bytes).readlist('intbe:16,intbe:8,intbe:8,uintbe:64,uintbe:32')
                length = headers[-1]
                
                data = con.recv(length)
                
                parsed_data = provider.parse(data,length)
                
                print('服务器收到消息',parsed_data)
            except Exception as e:
                break

if __name__ == "__main__":
    start_server(20880)
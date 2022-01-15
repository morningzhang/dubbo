# -*- coding: utf-8 -*-

import socket

from bitstring import ConstBitStream

from pydubbo import DubboProvider, RespCode
from  utils import encoder

HEADER_SIZE = 16
BACKLOG_SIZE = 128


def execute(*args):
    print(len(args))
    print(args)
    return "hello"


def start_server(port,func):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", port))
    server.listen(128)

    provider = DubboProvider()
    while True:
        con, address = server.accept()
        try:
            # 接受套接字的大小，怎么发就怎么收
            headers_bytes = con.recv(HEADER_SIZE)
            headers = ConstBitStream(bytes=headers_bytes).readlist('intbe:16,intbe:8,intbe:8,uintbe:64,uintbe:32')
            length = headers[-1]

            data = con.recv(length)

            parsed_data = provider.parse(data, length)

            print('服务器收到消息', parsed_data)

            data = bytearray()

            data.extend(ConstBitStream(f'uintbe:8={RespCode.RESPONSE_VALUE_WITH_ATTACHMENTS}').bytes)
            data.extend(encoder.encode_object(func(*parsed_data['arguments'])))
            reg_header = ConstBitStream('intbe:16=-9541,intbe:8=-62,intbe:8=0,uintbe:64=0,uintbe:32=%d' % len(data))
            con.send(reg_header.bytes)
            con.send(data)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    start_server(20880,execute)

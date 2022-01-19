import asyncio

from bitstring import BitStream, ReadError
from request import Request
from router import Router
from pydubbo import DubboProvider


class DServerProtocol(asyncio.Protocol):
    def __init__(self):
        self._buffer = None
        self._transport = None

    def connection_made(self, transport):
        self._buffer = BitStream()
        self._transport = transport

    def data_received(self, data):
        self._buffer.append(data)
        req = Request.parse(self._buffer)
        res = Router.exec(req)
        self._transport.send(res)

    def eof_received(self):
        self._buffer.clear()

    def connection_lost(self, exception):
        self._buffer = None

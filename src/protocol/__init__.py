import asyncio

from bitstring import BitStream, ReadError

from pydubbo import DubboProvider


class DServerProtocol(asyncio.Protocol):
    def __init__(self):
        self._buffer = None

    def connection_made(self, transport):
        self._buffer = BitStream()

    def data_received(self, data):
        self._buffer.append(data)
        pos_before = self._buffer.bytepos
        try:
            headers = self._buffer.readlist('intbe:16,intbe:8,intbe:8,uintbe:64,uintbe:32')
            length = headers[-1]
            content = self._buffer.read(f'bytes:{length}')
            print(content.bytes.decode())
        except ReadError as _re:
            self._buffer.bytepos = pos_before
        except ValueError as _ve:
            self._buffer.clear()
            self._buffer.bytepos = 0

    def eof_received(self):
        self._buffer.clear()

    def connection_lost(self, exception):
        self._buffer = None

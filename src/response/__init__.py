import asyncio

from bitstring import ConstBitStream, BitStream, ReadError

from utils.encoder import Encoder
from utils.parser import ParserV2


class ResponseCode:
    RESPONSE_WITH_EXCEPTION = 144
    RESPONSE_VALUE = 145
    RESPONSE_NULL_VALUE = 146
    RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS = 147
    RESPONSE_VALUE_WITH_ATTACHMENTS = 148
    RESPONSE_NULL_VALUE_WITH_ATTACHMENTS = 149


class Response:
    __slots__ = 'data'

    def __init__(self, data):
        self.data = data

    def parse(self, buffer):
        pos_before = self._buffer.bytepos
        try:
            length = buffer.readlist('intbe:16,intbe:8,intbe:8,uintbe:64,uintbe:32')[-1]
            body = buffer.read(length * 8)
            code = body.read('uintbe:8')
            if code in (ResponseCode.RESPONSE_NULL_VALUE, ResponseCode.RESPONSE_NULL_VALUE_WITH_ATTACHMENTS):
                self.data = None
            elif code in (ResponseCode.RESPONSE_WITH_EXCEPTION, ResponseCode.RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS):
                p = ParserV2(body)
                res = p.read_object()
                raise Exception(res)
            elif code in (ResponseCode.RESPONSE_VALUE, ResponseCode.RESPONSE_VALUE_WITH_ATTACHMENTS):
                p = ParserV2(body)
                res = p.read_object()
                self.data = res
        except ReadError as _re:
            self._buffer.bytepos = pos_before
        except ValueError as _ve:
            self._buffer.clear()
            self._buffer.bytepos = 0
        except Exception as e:
            raise Exception(e)
        return self

    def encode(self):
        data = BitStream(f'intbe:16=-9541,intbe:8=-62,intbe:8=0,uintbe:64=0,uintbe:32={len(self.data)}')
        data.extend(Encoder().encode(self.data))
        return data.bytes

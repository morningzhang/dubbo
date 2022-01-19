import asyncio
from io import StringIO

from bitstring import ConstBitStream, BitStream, ReadError

from pydubbo import Dubbo
from response import Response
from utils.encoder import Encoder
from utils.parser import ParserV2

JAVA_PRIMATIVE_TYPE_BYTECODE_MAP = {
    "J": "long",
    "I": "int",
    "F": "float",
    "B": "byte",
    "S": "short",
    "D": "double",
    "Z": "boolean",
    "C": "char"
}

JAVA_PRIMATIVE_TYPE_BYTECODE_LIST = [
    "J",
    "I",
    "F",
    "B",
    "S",
    "D",
    "Z",
    "C"
]


class Attachments(dict):
    def __setattr__(self, k, v):
        self[k] = v

    def __getattr__(self, k):
        return self[k]


class Request(Dubbo):

    def __init__(self, interface, version, method, args):
        super().__init__(interface, version, method, args)

    def _encode(self, encoder):
        elements = bytearray()
        elements.extend(encoder.encode(self.dubbo_v))
        elements.extend(encoder.encode(self.interface))
        if self.version:
            elements.extend(encoder.encode(self.version))
        elements.extend(encoder.encode(self.method))

        arg_type = "".join([arg_type for arg_type, _ in self.args])
        elements.extend(encoder.encode(arg_type))
        for _, arg in self.args:
            elements.extend(encoder.encode(arg))
        elements.extend(encoder.encode(self.attachments))

        return elements

    @classmethod
    def parse_arguments_types(arg_types):
        if not arg_types:
            return ()

        stream = StringIO(arg_types)
        stream_len = len(arg_types)

        arguments = []

        start_idx = 0
        while start_idx < stream_len - 1:
            char = stream.read(1)
            start_idx += 1
            if char in JAVA_PRIMATIVE_TYPE_BYTECODE_LIST:
                arguments.append(JAVA_PRIMATIVE_TYPE_BYTECODE_MAP.get(char))
                continue

            if char == "L":
                arg_type = char
                while char != ";" and start_idx < stream_len:
                    char = stream.read(1)
                    start_idx += 1
                    arg_type += char

                arguments.append(arg_type)

        return arguments

    def parse(self, buffer):
        pos_before = self._buffer.bytepos
        try:
            length = buffer.readlist('intbe:16,intbe:8,intbe:8,uintbe:64,uintbe:32')[-1]
            body = buffer.read(length * 8)
            p = ParserV2(body)
            self.dubbo_v = p.read_object()
            self.interface = p.read_object()
            self.version = p.read_object()
            self.method = p.read_object()
            self.args = [p.read_object() for _x in Request.parse_arguments_types(p.read_object())]
            self.attachments = p.read_object()
            return self
        except ReadError as _re:
            self._buffer.bytepos = pos_before
        except ValueError as _ve:
            self._buffer.clear()
            self._buffer.bytepos = 0

    async def encode(self):
        body = self._encode(Encoder())
        data = BitStream(f'intbe:16=-9541,intbe:8=-62,intbe:8=0,uintbe:64=0,uintbe:32={len(body)}')
        data.extend(body)
        return data.bytes

    def response(self, data):
        return Response(self, data)

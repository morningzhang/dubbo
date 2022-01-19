import asyncio

from response import Response
from utils import encoder


class Router(dict):

    def add_router(self, interface, method, version, handler):
        k = f"{interface}|{method}|{version}"
        self.__setattr__(k, handler)

    async def exec(self, request):
        k = f"{request.interface}|{request.method}|{request.version}"
        handler = self.__getattr__(k)

        if handler and callable(handler):
            if asyncio.iscoroutinefunction(handler):
                res = await handler(request)
            else:
                res = handler(request)

            res = res if isinstance(res, Response) else Response(res)
            return res.encode()

        return None

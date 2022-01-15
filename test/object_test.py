# -*- coding: utf-8 -*-
from socket import socket, AF_INET, SOCK_STREAM

from pydubbo import Dubbo

if __name__ == '__main__':

    client = socket(AF_INET, SOCK_STREAM)
    client.connect(("10.252.22.245", 28080))
    try:
        d = Dubbo("com.weimob.o2o.wecom.data.api.interfaces.query.QueryCusApi", "0.0.0", "queryCusStatis",
                  (("Lcom/weimob/o2o/wecom/data/api/interfaces/query/request/CusStatisRequest;",
                    {"corpid": 1, "dateRangeType": 3}),))
        res = d._invoke(client)
        print(f'Received: {res!r}')
    finally:
        client.close()

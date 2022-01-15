# -*- coding: utf-8 -*-
from pydubbo import DubboConsumer

if __name__ == '__main__':
    d = DubboConsumer(interface="com.dianwoba.redcliff.account.provider.TransferProvider", hosts='192.168.11.29:2281',
                      version="0.0.0")
    try:
        res = d.receiveMessage([("Ljava/lang/Integer;", 180702001)])
        print(res)
        d.close()
    except Exception as e:
        print(e.message.detailMessage)
        exit(1)

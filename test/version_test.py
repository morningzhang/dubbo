# -*- coding: utf-8 -*-
from pydubbo.pydubbo import DubboZK

if __name__ == '__main__':
    d = DubboZK(interface="com.dianwoba.redcliff.account.provider.TransferProvider", hosts='192.168.11.29:2281',
                version="0.0.0")
    try:
        res = d.receiveMessage([("Ljava/lang/Integer;", 180702001)])
        print(res)
        d.close()
    except Exception as e:
        print(e.message.detailMessage)
        exit(1)

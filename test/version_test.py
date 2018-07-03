# -*- coding: utf-8 -*-
from pydubbo.pydubbo import DubboZK

if __name__ == '__main__':
    d = DubboZK(interface="com.dianwoba.redcliff.account.provider.TransferProvider", hosts='192.168.11.29:2281',version="1.0.0")
    res = d.receiveMessage("receiveMessage", [("Ljava/lang/Integer;", 180702001)])
    print(res)
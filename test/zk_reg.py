# -*- coding: utf-8 -*-
from pydubbo.pydubbo import DubboZK

if __name__ == '__main__':
    d = DubboZK(interface="com.dianwoba.genius.provider.StaffProvider", hosts='192.168.11.29:2185')
    res = d.findByCode("findByCode", [("Ljava/lang/String;", u"00064")])
    print(res)
    res = d.findByCode("findByCode", [("Ljava/lang/String;", u"00064")])
    print(res.data.name)

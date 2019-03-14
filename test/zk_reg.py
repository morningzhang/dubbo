# -*- coding: utf-8 -*-
from pydubbo.pydubbo import DubboZK

if __name__ == '__main__':
    d = DubboZK(interface="com.dianwoba.genius.provider.StaffProvider", hosts='192.168.11.29:2185')
    res = d.findByCode([("Ljava/lang/String;", u"00064")]) #根据返回的方法，动态添加.输入的参数只有一个java string类型
    print(res, res.data)#(<com.dianwoba.dubbo.base.result.ResponseDTO object at 0x1035d2a50>, <com.dianwoba.genius.domain.dto.StaffDTO object at 0x1035d2ad0>)
    res = d.findByMobile([("Ljava/lang/String;", u"18657112153")]) #根据返回的方法，动态添加.输入的参数只有一个java string类型
    print(res.data.name)
    for s in d.uris:
        print(s)
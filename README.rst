python调用dubbo,目前支持dubbo协议，不需要服务端修改成jsonrpc.实现客户端的负载均衡、配合Zookeeper自动发现服务功能等
依赖pyhessian，bitstring

示例代码
::

    from pydubbo.pydubbo import DubboZK

    if __name__ == '__main__':
        d = DubboZK(interface="com.dianwoba.genius.provider.StaffProvider", hosts='192.168.11.29:2185')
        res = d.findByCode([("Ljava/lang/String;", u"00064")]) #根据返回的方法，动态添加.输入的参数只有一个java string类型
        print(res, res.data)#(<com.dianwoba.dubbo.base.result.ResponseDTO object at 0x1035d2a50>, <com.dianwoba.genius.domain.dto.StaffDTO object at 0x1035d2ad0>)
        res = d.findByMobile([("Ljava/lang/String;", u"18657112153")]) #根据返回的方法，动态添加.输入的参数只有一个java string类型
        print(res.data.name)




具体参照测test目录，目前支持常用的dubbo协议。



1.dubbo协议

1.1 请求头

    intbe:16=-9541,intbe:8=-62,intbe:8=0,uintbe:64=0,uintbe:32=数据包长度

    总长度16字节。

    依次是2字节的magic信息 ，一个字节的附加信息 ，一个字节的空信息，8个字节的空信息，4个字节的数据包长度




1.2 信息体

    通过hessian协议序列化的二进制信息



1.3 响应头

    intbe:16,intbe:8,intbe:8,uintbe:64,uintbe:32

    总长度16个字节

    依次是2字节的magic信息 ，一个字节的附加信息 ，一个字节的空信息，8个字节的空信息，4个字节的数据包长度
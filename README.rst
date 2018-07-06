用python调用dubbo可以用于测试等
依赖pyhessian，bitstring
具体参照测试代码，目前支持常用的dubbo协议。



1.dubbo协议

1.1 请求头

    intbe:16=-9541,intbe:8=-62,intbe:8=0,uintbe:64=0,uintbe:32=数据包长度

    总长度16字节。

    依次是2字节的magic信息 ，一个字节的附加信息 ，一个字节的空信息，8个字节的空信息，4个字节的数据包长度




1.2 信息体

    通过hessian协议封装的数据信息



1.3 响应头

    intbe:16,intbe:8,intbe:8,uintbe:64,uintbe:32

    总长度16个字节

    依次是2字节的magic信息 ，一个字节的附加信息 ，一个字节的空信息，8个字节的空信息，4个字节的数据包长度
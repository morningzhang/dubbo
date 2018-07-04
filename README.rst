用python调用dubbo可以用于测试等
依赖pyhessian，bitstring
具体参照测试代码

from pydubbo.pydubbo import DubboZK
d = DubboZK(interface="com.dianwoba.dw.riderservice.service.RiderStatService", hosts='xxx:2181')
res = d.fetchRiderCompletedOrders("fetchRiderCompletedOrders", [("Ljava/lang/Long;", 571307),("Ljava/lang/Integer;",0),("Ljava/lang/Integer;",1530584726)])
res
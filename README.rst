# dubbo
用python调用dubbo可以用于测试等
依赖pyhessian，bitstring

1. java的服务端代码
1.1 pojo对象

```java
package org.apache.dubbo.demo;

import java.io.Serializable;
import java.util.List;

public class Person implements Serializable {
    String name;
    List<String> address;
    int age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getAddress() {
        return address;
    }

    public void setAddress(List<String> address) {
        this.address = address;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
```

1.2 接口
```java
package org.apache.dubbo.demo;

import java.util.List;

public interface DemoService {

    String sayHello(String name);

    Long echo(List<Integer> val, String aaa);

    Person swich(Person p);


}

```

2. python端调用
```python
client = socket(AF_INET, SOCK_STREAM)
client.connect(("127.0.0.1", 20880))
try:
    d = Dubbo("org.apache.dubbo.demo.DemoService", "0.0.0", "swich",
              (("Lorg/apache/dubbo/demo/Person;", {"name": u"某某", "address": [u"余杭"], "age": 15}),))
    res = d.invoke(client)
    print(res)
finally:
    client.close()

```
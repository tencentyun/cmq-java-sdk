# cmq-java-sdk
Tencent CMQ Java Http SDK


## SDK使用方式：
API的具体使用方式，可以参考源码中example目录下的demo

项目引入SDK可以使用Maven依赖或者直接下载源码

### 方式一：Maven依赖
直接pom文件中加入maven依赖。1.0.7版本开始，SDK的artifactId改为cmq-http
```xml
<dependency>
    <groupId>com.qcloud</groupId>
    <artifactId>cmq-http</artifactId>
    <version>1.0.7</version>
</dependency>

```

### 方式二：源码依赖
直接下载源码，放入项目中依赖，方便自定义SDK代码
#### 1）获取master代码：
```
git clone https://github.com/tencentyun/cmq-java-sdk.git
```

#### 2）获取指定tag代码，如v1.0.7：

```
git clone --branch v1.0.7 https://github.com/tencentyun/cmq-java-sdk.git
```

#### 3）编译，在工程目录下执行：
```
mvn clean install -Dmaven.test.skip=true 
```


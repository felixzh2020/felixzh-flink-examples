# 项目背景

本项目基于Apache Flink官方使用案例，结合本人实际实时计算项目开发经历，集成本人使用到的案例。
一方面：用于个人积累；另一方面：拥抱开源。

# 前置条件

Java、Scala、Maven、Git。

# 编译、打包、运行
可以直接使用命令运行编译、打包的jar，或者在idea直接运行项目。由于项目结构直接沿用Flink源码中flink-examples工程结构，为避免可能的依赖问题，务必使用如下命令进行编译、打包：
mvn clean package -DskipTests -Dfast

# 新增功能列表

1. 基于DataSet API使用广播变量
https://github.com/felixzh2015/felixzh-flink-examples/tree/master/flink-examples-batch/src/main/java/org/apache/flink/examples/java/broadcast
2. KafkaFlinkKafka案例
https://github.com/felixzh2015/felixzh-flink-examples/tree/master/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/kafka


# 微信公众号

欢迎关注微信公众号(大数据从业者)，期待共同交流沟通、共同成长！

# 博客地址

https://www.cnblogs.com/felixzh/

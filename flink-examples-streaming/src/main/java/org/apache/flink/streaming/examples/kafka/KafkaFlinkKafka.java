package org.apache.flink.streaming.examples.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

/**
 * @ClassName KafkaFlinkKafka
 * @Description 消费kafka数据，数据格式：字符串（忽略schema），经过处理，再生产到kafka。
 * @Author felixzh
 * @Date 2018/12/24 14:44
 **/
public class KafkaFlinkKafka {
    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 4) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-topic <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> --group.id <some id>");
            return;
        }

        //1. StreamExecutionEnvironment配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

        //2. kafka consumer配置
        Properties kafkaConsumerPros = new Properties();
        kafkaConsumerPros.setProperty("group.id", parameterTool.getRequired("group.id"));
        kafkaConsumerPros.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"));
        //kafka consumer partition动态发现
        kafkaConsumerPros.setProperty("flink.partition-discovery.interval-millis", "1000");

        //3. kafka producer配置
        Properties kafkaProducerPros = new Properties();
        kafkaProducerPros.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"));

        //4. 输入DataStream：消费的kafka topic并行度要与kafka partition数目相等
        DataStream<String> input = env
                .addSource(
                        new FlinkKafkaConsumer010<>(
                                parameterTool.getRequired("input-topic"),
                                new SimpleStringSchema(),
                                kafkaConsumerPros)).setParallelism(3).keyBy(value -> value.split(",")[0]);

        //5. 处理，如：增加时间戳。这里使用lambda表达式替换如下注释中的写法
        /*DataStream<String> output = input.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return System.currentTimeMillis() + ":" + value;
            }
        });*/
        DataStream<String> output = input.map(value -> (System.currentTimeMillis() + ":" + value)).setParallelism(3);

        //6. 输出DataStream：生产的kafka topic并行度要与kafka partition数目相等
        FlinkKafkaProducer010 kafkaSink = new FlinkKafkaProducer010<>(
                parameterTool.getRequired("output-topic"),
                new SimpleStringSchema(),
                kafkaProducerPros
        );
        kafkaSink.setFlushOnCheckpoint(true);
        kafkaSink.setLogFailuresOnly(false);
        kafkaSink.setWriteTimestampToKafka(true);
        output.addSink(kafkaSink).setParallelism(3);

        //7. 调用execute
        env.execute("Kafka Flink Kafka UseCase");

    }
}

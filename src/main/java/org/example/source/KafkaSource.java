package org.example.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Author: morris
 * @Date: 2020/9/16 17:03
 * @reviewer
 */
public class KafkaSource {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        // 指定Kafka的连接位置
        properties.setProperty("bootstrap.servers", "hadoop001:9092");
        // 指定监听的主题，并定义Kafka字节消息到Flink对象之间的转换规则
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("flink-stream-in-topic", new SimpleStringSchema(), properties));
        stream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] split = s.split(",");
                        for (String v :
                                split) {
                            collector.collect(new Tuple2<>(v, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();
        env.execute("Flink Streaming");
    }
}

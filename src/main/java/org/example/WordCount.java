package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;


/**
 * @Author: morris
 * @Date: 2020/9/16 15:19
 * @reviewer
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> stringDataSource = env.readTextFile("C:\\Users\\Administrator\\Desktop\\tmp\\data\\新建文本文档.txt");
        stringDataSource
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] s1 = s.split(" ");
                    for (String v : s1) {
                        collector.collect(new Tuple2<>(v, 1));
                    }
                })
                .groupBy(0)
                .sum(1)
                .print();
    }
}

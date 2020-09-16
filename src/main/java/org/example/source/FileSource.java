package org.example.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * 流处理 读文件
 * @Author: morris
 * @Date: 2020/9/16 15:46
 * @reviewer
 */
public class FileSource {
    public static void main(String[] args) throws Exception {
        final String filePath = "C:\\Users\\Administrator\\Desktop\\tmp\\data\\新建文本文档.txt";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readFile(new TextInputFormat(new Path(filePath)),
                filePath,
                FileProcessingMode.PROCESS_ONCE,
                10
        );

        env.execute("file stream");

        source.print();
    }
}

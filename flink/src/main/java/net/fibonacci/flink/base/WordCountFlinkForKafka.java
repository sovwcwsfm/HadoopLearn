package net.fibonacci.flink;

import net.fibonacci.flink.map.WordSplitTask;
import net.fibonacci.flink.model.WordCountModel;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/7/19 20:01
 * @Description: wordCount flink版本
 */
public class WordCountFlinkForKafka {
    public static void main(String[] args) throws Exception {
        // 1. 程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 获取输入数据 这里通过Socket流来获取输入数据
//        DataStreamSource<String> socketDataStream = env.socketTextStream("", 111);

        // Keys have to start with '-' or '--' For example, --key1 value1 -key2 value2.
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");
        DataStreamSource<String> socketDataStream = env.socketTextStream(hostname, port);

        // 3. 数据处理 切分数据获取单词-> 按单词分组 -> 数量+1
        SingleOutputStreamOperator<WordCountModel> result = socketDataStream
                .flatMap(new WordSplitTask())
                .keyBy("word")     // 通过 字段名 分组
//                .keyBy(0);                  // 通过 key 的索引分组
                .sum("count");

        // 4. 输出数据
        result.print();
        // 启动应用
        env.execute("workCount for flink");

    }
}

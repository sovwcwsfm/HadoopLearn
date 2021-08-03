package net.fibonacci.flink.base;

import net.fibonacci.flink.base.map.WordSplitTask;
import net.fibonacci.flink.base.model.WordCountModel;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSourceBase;

import java.util.Properties;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/7/19 20:01
 * @Description: wordCount flink 读取kafka数据版本
 */
public class WordCountFlinkForKafka {
    public static void main(String[] args) throws Exception {
        // 1. 程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 获取输入数据 这里通过Kafka获取输入数据
        String topic="test112";
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers","192.168.85.111:9092");
        consumerProperties.setProperty("group.id","test112_consumer");

        FlinkKafkaConsumer010<String> myConsumer =
                new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), consumerProperties);
        //task
        DataStreamSource<String> data = env.addSource(myConsumer).setParallelism(3);

        // 3. 数据处理 切分数据获取单词-> 按单词分组 -> 数量+1
        SingleOutputStreamOperator<WordCountModel> result = data
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

package net.fibonacci.flink.window;

import net.fibonacci.flink.base.map.WordSplitTask;
import net.fibonacci.flink.base.model.WordCountModel;
import net.fibonacci.flink.window.souce.TestSource;
import net.fibonacci.flink.window.fun.SumProcessWindowFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/18 14:37
 * @Description: 通过window 每隔5s 计算最近10s 单词出现的次数
 */
public class TimeWindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("hostname");
//        int port = parameterTool.getInt("port");
//
//        DataStreamSource<String> source = env.socketTextStream(host, port);

        // 自定义数据源
        DataStreamSource<String> source = env.addSource(new TestSource());

        source.flatMap(new WordSplitTask())
                .keyBy(WordCountModel::getWord)
                // 设定滑动滑动窗口大小 10s 滑动间隔 5s
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum("count")
//                .process(new SumProcessWindowFunction())
                .print()
                .setParallelism(1);

        env.execute("window word count");

    }

}

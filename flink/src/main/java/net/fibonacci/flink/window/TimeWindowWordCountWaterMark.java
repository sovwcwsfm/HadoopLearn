package net.fibonacci.flink.window;

import net.fibonacci.flink.base.map.WordSplitTask;
import net.fibonacci.flink.base.model.WordCountModel;
import net.fibonacci.flink.window.fun.SumProcessWindowFunction;
import net.fibonacci.flink.window.souce.TestSource2;
import net.fibonacci.flink.window.time.EventTimeExtractor;
import net.fibonacci.flink.window.time.PeriodicWatermarkGeneratorDelay;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/19 16:20
 * @Description: 乱序处理问题
 */
public class TimeWindowWordCountWaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 自定义数据源
        DataStreamSource<String> source = env.addSource(new TestSource2());

        source.flatMap(new WordSplitTask())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .forGenerator(ctx -> new PeriodicWatermarkGeneratorDelay())
                                .withTimestampAssigner(ctx -> new EventTimeExtractor()))    // 指定EventTime 字段
                .keyBy(WordCountModel::getWord)
                // 设定滑动滑动窗口大小 10s 滑动间隔 5s 指定时间类型为 事件时间
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .sum("count")
                .process(new SumProcessWindowFunction())
                .print()
                .setParallelism(1);

        env.execute("window word count");
    }
}

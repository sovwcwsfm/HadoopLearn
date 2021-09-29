package net.fibonacci.flink.window.project.hot;

import net.fibonacci.flink.window.project.hot.aggregate.PageCountAgg;
import net.fibonacci.flink.window.project.hot.aggregate.PageWindowResult;
import net.fibonacci.flink.window.project.hot.map.HotPageLogMap;
import net.fibonacci.flink.window.project.hot.model.LogBean;
import net.fibonacci.flink.window.project.hot.model.ResultBean;
import net.fibonacci.flink.window.project.hot.process.TopNHotPage;
import net.fibonacci.flink.window.project.hot.process.TopNHotPage2;
import net.fibonacci.flink.window.project.hot.strategy.PeriodicWatermarkGenerator;
import net.fibonacci.flink.window.project.hot.strategy.TimeStampExtractor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/20 15:10
 * @Description: 热点数据实时统计
 * 83.149.9.123 - - 17/05/2020:10:05:03 +0000 GET /presentations/logstash-kafkamonitor-2020/images/kibana-search.png
 * 每5s 统计10分钟内 数据TopN
 * 知识点 滑动窗口、水位、聚合
 */
public class HotPage {

    private static final String PATH = "/Users/sovwcwsfm/MyDocument/BigData/Doc/NaiXue/HadoopLearn/flink/src/main/java/net/fibonacci/flink/window/data/data2.log";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.enableCheckpointing(3600);
        env.getCheckpointConfig().setCheckpointTimeout(60);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.setStateBackend(new FsStateBackend(""));

        DataStreamSource<String> source = env.readTextFile(PATH);

        source.map(new HotPageLogMap())
                .assignTimestampsAndWatermarks(  // 设置水位
                        WatermarkStrategy
                                .forGenerator(ctx -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner(context -> new TimeStampExtractor()))
                // 按URL 分组
                .keyBy(LogBean::getPageUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                // 计算窗口数据 每个URL在window里面出现的次数   将内容转换为输出格式增加EventTime时间
                .aggregate(new PageCountAgg(), new PageWindowResult())
                // 按照窗口进行分组 每个数据都有在一个单独的窗口里面 所以需要按照窗口进行统计
                .keyBy(ResultBean::getWindowTime)
                // 求TopN
                .process(new TopNHotPage(3))

                // Test1 这样做好像没什么软用 并不能每隔5s 输出一次 并且没法实现TopN
//                .sum("count")
//                .filter((FilterFunction<LogBean>) logBean -> logBean.getCount() > 1)

                // Test2
//                .sum("count")
//                .keyBy(LogBean::getPageUrl)
//                .process(new TopNHotPage2(3))
                .print();


        env.execute("hot page");

    }

}

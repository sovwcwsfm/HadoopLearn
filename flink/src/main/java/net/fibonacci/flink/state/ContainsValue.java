package net.fibonacci.flink.state;

import net.fibonacci.flink.state.map.ContainsValueWithAggregatingState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/3 16:35
 * @Description: 合并数据
 */
public class ContainsValue {

    public static void main(String[] args) throws Exception {
        // 程序应用入口
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源
        DataStreamSource<Tuple2<String, Long>> dataStreamSource = executionEnvironment.fromElements(
                Tuple2.of("hadoop", 2L),
                Tuple2.of("hadoop", 3L),
                Tuple2.of("hadoop", 4L),
                Tuple2.of("flink", 4L),
                Tuple2.of("hadoop", 4L),
                Tuple2.of("spark", 1L),
                Tuple2.of("flink", 4L),
                Tuple2.of("spark", 5L),
                Tuple2.of("spark", 6L)
        );

        dataStreamSource
                .keyBy(value -> value.f0)
                .flatMap(new ContainsValueWithAggregatingState())
                .print();

        executionEnvironment.execute("containForState");
    }
}

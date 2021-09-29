package net.fibonacci.flink.state;

import net.fibonacci.flink.state.map.CountWindowAverageWithListState;
import net.fibonacci.flink.state.map.CountWindowAverageWithMapState;
import net.fibonacci.flink.state.map.CountWindowAverageWithValueState;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/7/30 10:45
 * @Description: 同个key 每3次对值取平均输出
 */
public class CountAverage {
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

        // 数据处理
        dataStreamSource
                .keyBy(value -> value.f0)
//                .flatMap(new CountWindowAverageWithValueState())
//                .flatMap(new CountWindowAverageWithListState())
                .flatMap(new CountWindowAverageWithMapState())
                .print();


        // 执行任务
        executionEnvironment.execute("countAverage");
    }
}

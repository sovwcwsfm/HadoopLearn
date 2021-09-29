package net.fibonacci.flink.state.map;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/3 16:46
 * @Description: 通过 ReducingState 计算 value 总和
 * ReducingState<T> ：这个状态为每一个 key 保存一个聚合之后的值
 * get() 获取状态值
 * add() 更新状态值，将数据放到状态中
 * clear() 清除状态
 */
public class SumWithReducingState extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

    // 用于保存每一个 key 对应的 value 的总值
    private ReducingState<Long> sumState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ReducingStateDescriptor<Long> descriptor = new ReducingStateDescriptor<>("sum", new ReduceFunction<Long>() {
            @Override
            public Long reduce(Long aLong, Long t1) throws Exception {
                // 每个key的数据处理 along 上次处理完的数据
                return aLong + t1;
            }
        }, Long.class);

        sumState = getRuntimeContext().getReducingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Long> input, Collector<Tuple2<String, Long>> collector) throws Exception {
        // sum 逻辑都在 reduce 中  flatMap 没有其他逻辑了
        // 1. 将value 放到state中 2. 取出state的结果输出

        // 将数据放到状态中
        sumState.add(input.f1);

        collector.collect(Tuple2.of(input.f0, sumState.get()));
    }
}

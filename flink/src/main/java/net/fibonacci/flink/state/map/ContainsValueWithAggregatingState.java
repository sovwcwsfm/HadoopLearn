package net.fibonacci.flink.state.map;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/8/4 15:51
 * @Description: 将所有输入的数据汇总成一条输出
 * hadoop 1
 * hadoop 1
 * hadoop 2
 * hadoop 3
 *
 * output hadoop,Fibonacci: 1 and 1 and 2 and 3
 */
public class ContainsValueWithAggregatingState extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, String>> {

    private AggregatingState<Long, String> totalStr;

    private final String ACC = "Fibonacci: ";

    @Override
    public void open(Configuration parameters) throws Exception {
        AggregatingStateDescriptor<Long, String, String> descriptor = new AggregatingStateDescriptor<>("totalStr", new AggregateFunction<Long, String, String>() {
            @Override
            public String createAccumulator() {
                // 没有数据输入的时候的输出
                return ACC;
            }

            @Override
            public String add(Long value, String accumulator) {
                if (ACC.equals(accumulator)) {
                    // 第一个值
                    return accumulator + value;
                }else {
                    // 有值 值 and 值
                    return accumulator + " and " + value;
                }
            }

            @Override
            public String getResult(String accumulator) {
                // 获取的结果
                return accumulator;
            }

            @Override
            public String merge(String value1, String value2) {
                // 合并两个 accumulator
                return value1 + " and " + value2;
            }
        }, String.class);

        totalStr = getRuntimeContext().getAggregatingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Long> input, Collector<Tuple2<String, String>> output) throws Exception {
        totalStr.add(input.f1);

        output.collect(Tuple2.of(input.f0, totalStr.get()));
    }
}

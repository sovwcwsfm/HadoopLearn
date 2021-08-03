package net.fibonacci.flink.state.map;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.LinkedList;

/**
 * @Auther: sovwcwsfm
 * @Date: 2021/7/30 10:16
 * @Description:  按key分组 同个key 每3次 对值取平均输出
 *                ListState<T> 保存每个key的每条数据
 *                get() 获取状态值
 *                add() 添加数据状态值
 *                clear() 清楚状态值
 */
public class CountWindowAverageWithListState extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Double>> {

    // 保存每条数据
    // 因为进行过 keyBy 所以能进到flatmap的 key都是相同的
    private ListState<Tuple2<String, Long>> elementsByKey;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化 注册状态
        ListStateDescriptor<Tuple2<String, Long>> descriptor = new ListStateDescriptor<>(
                "average",          // 状态的名称
                Types.TUPLE(Types.STRING, Types.LONG));       // 状态存储的数据类型 这里为啥要定义类型？不能通过泛型拿到嘛？

        // 通过当前运行上下文获取状态
        elementsByKey = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Long> input, Collector<Tuple2<String, Double>> out) throws Exception {
        // 获取状态值
        Iterable<Tuple2<String, Long>> currentState = elementsByKey.get();

        if (null == currentState) {
            elementsByKey.addAll(Collections.EMPTY_LIST);
        }

        elementsByKey.add(input);

        // 获取数据
        LinkedList<Tuple2<String, Long>> allElements = Lists.newLinkedList(elementsByKey.get());

        if (allElements.size() == 3) {
            // 计算平均
            long count = 0;
            long sum = 0;
            // 统计数据
            for (Tuple2<String, Long> element : allElements) {
                count ++;
                sum += element.f1;
            }
            // 清除数据
            elementsByKey.clear();
            // 计算平均 输出下游
            out.collect(Tuple2.of(input.f0, sum / (double)count));
        }
    }
}
